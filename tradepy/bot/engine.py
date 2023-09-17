import contextlib
import random
import pickle
import pandas as pd
from functools import cached_property
from datetime import date
from pathlib import Path

import tradepy
from tradepy.blacklist import Blacklist
from tradepy.constants import CacheKeys
from tradepy.core.account import Account
from tradepy.core.adjust_factors import AdjustFactors
from tradepy.core.conf import TradingConf
from tradepy.core.models import Order, Position
from tradepy.strategy.base import LiveStrategy
from tradepy.decorators import require_mode, timeout, timeit
from tradepy.mixins import TradeMixin
from tradepy.types import MarketPhase
from tradepy.bot.broker import BrokerAPI
from tradepy.depot.misc import AdjustFactorDepot
from tradepy.utils import get_latest_trade_date
from tradepy.vendors.types import AskBid


LOG = tradepy.LOG
timeouts_conf = tradepy.config.trading.timeouts


class TradingCacheManager:
    def __init__(self, workspace_dir: Path) -> None:
        self.workspace = workspace_dir

    @cached_property
    def redis_client(self):
        return tradepy.config.common.get_redis_client()

    def __get_lock_key(self, cache_key):
        return f"{cache_key}:lock"

    @contextlib.contextmanager
    def use_local_cache(self, key: str):
        cache_file = self.workspace / f"{key}.pkl"
        if cache_file.exists():
            with open(cache_file, "rb") as fh:
                yield pickle.load(fh), None
        else:
            set_cache = lambda obj: cache_file.write_bytes(pickle.dumps(obj))
            yield None, set_cache

    @contextlib.contextmanager
    def use_redis_cache(self, key: str, lock_timeout: int = 10):
        def set_cache(obj):
            with (self.workspace / f"{key}.pkl").open("wb") as fh:
                pickle.dump(obj, fh, protocol=pickle.HIGHEST_PROTOCOL)
            self.redis_client.set(key, pickle.dumps(obj))

        lock_key = self.__get_lock_key(key)
        with self.redis_client.lock(lock_key, timeout=lock_timeout, sleep=1):
            if self.redis_client.exists(key):
                try:
                    content = self.redis_client.get(key)
                except UnicodeDecodeError as e:
                    content = pickle.loads(e.object)
                    yield content, None
            elif (local_file := self.workspace / f"{key}.pkl").exists():
                with open(local_file, "rb") as fh:
                    content = pickle.load(fh)
                yield content, None
            else:
                yield None, set_cache

    def is_cache_locked(self, cache_key: str) -> bool:
        lock_key = self.__get_lock_key(cache_key)
        return bool(self.redis_client.get(lock_key))


class TradingEngine(TradeMixin):
    def __init__(self) -> None:
        self.workspace_dir = Path.home() / ".tradepy" / "workspace" / str(date.today())
        self.workspace_dir.mkdir(exist_ok=True, parents=True)
        self.cache = TradingCacheManager(self.workspace_dir)

        self.conf: TradingConf = tradepy.config.trading  # type: ignore
        assert self.conf
        self.strategy_conf = self.conf.strategy

        self.adjust_factors: AdjustFactors = AdjustFactorDepot.load()
        self.strategy: LiveStrategy = self.strategy_conf.load_strategy()  # type: ignore

    @cached_property
    def account(self) -> Account:
        return BrokerAPI.get_account()

    def _load_indicators_from_cache(self, key: str) -> pd.DataFrame | None:
        with self.cache.use_redis_cache(key) as (
            content,
            _,
        ):
            return content

    def _load_hist_daily_from_cache(self) -> pd.DataFrame:
        with self.cache.use_local_cache(CacheKeys.hist_k) as (df, _):
            assert (
                isinstance(df, pd.DataFrame) and not df.empty
            ), "Unable to find the cached day k data!"
            return df

    def _jit_sell_price(self, price: float, slip_pct: float) -> float:
        slip = slip_pct * 1e-2
        if tradepy.config.common.mode == "paper-trading":
            jitter = random.uniform(0, slip)
            return price * (1 - jitter)
        return price * (1 - slip)

    def _merge_hist_daily_and_current_quote(
        self, hist_df: pd.DataFrame, quote_df: pd.DataFrame
    ) -> pd.DataFrame:
        # Interpolate missing data
        quote_df["mkt_cap_rank"] = hist_df.groupby(level="code")["mkt_cap_rank"].tail(1)
        quote_df.dropna(subset=["mkt_cap_rank"], inplace=True)
        df = pd.concat([hist_df, quote_df])

        # Remove stocks that don't have enough data for computing the indicators
        n_bars = df.groupby(level="code").size()
        drop = n_bars[n_bars < self.conf.indicators_window_size].index
        if drop.empty:
            return df
        return df.query("code not in @drop").copy()

    def _get_buy_options(
        self, ind_df: pd.DataFrame, orders: list[Order], positions: list[Position]
    ) -> pd.DataFrame:
        already_traded = set(x.code for x in orders + positions)
        codes_and_prices = [
            (
                code,
                self.adjust_factors.to_real_price(code, price_and_weight[0]),
                price_and_weight[1],
            )
            for code, *indicators in ind_df[self.strategy.buy_indicators].itertuples(
                name=None
            )
            if (code not in already_traded)
            and (not Blacklist.contains(code))
            and (price_and_weight := self.strategy.should_buy(*indicators))
        ]

        if not codes_and_prices:
            return pd.DataFrame()

        (
            codes,
            prices,
            weights,
        ) = zip(*codes_and_prices)
        timestamp = ind_df.iloc[0]["timestamp"]
        return pd.DataFrame(
            {
                "order_price": prices,
                "weight": weights,
                "timestamp": [timestamp] * len(prices),
            },
            index=pd.Index(codes, name="code"),
        )

    def _get_close_codes(self, ind_df: pd.DataFrame) -> list[str]:
        if not self.strategy.close_indicators:
            return []

        return [
            code
            for code, *indicators in ind_df[self.strategy.close_indicators].itertuples(
                name=None
            )
            if self.strategy.should_sell(*indicators)
        ]

    def _compute_open_indicators(self, quote_df: pd.DataFrame) -> pd.DataFrame | None:
        cache_key = CacheKeys.indicators_df
        if self.cache.is_cache_locked(cache_key):
            LOG.info("已经有其他进程在计算开盘指标了，不再重复计算。")
            return

        with self.cache.use_redis_cache(
            cache_key, timeouts_conf.compute_open_indicators
        ) as (df, set_cache):
            if isinstance(df, pd.DataFrame):
                LOG.info("已从缓存读取了预计算指标，不再重复计算。交易行为应该与上次相同。")
                return df

            # Compute indicators
            hist_df = self._load_hist_daily_from_cache()
            raw_df = self._merge_hist_daily_and_current_quote(hist_df, quote_df)
            with timeit() as timer:
                ind_df = self.strategy.compute_open_indicators(raw_df)
                today = quote_df.iloc[0]["timestamp"]
                ind_df = ind_df.query("timestamp == @today").copy()
            LOG.info(f"计算开盘指标: {timer['seconds']}s")

            # Cache the result
            assert callable(set_cache), set_cache
            set_cache(ind_df)
            return df

    def _compute_close_indicators(self, quote_df: pd.DataFrame) -> pd.DataFrame | None:
        cache_key = CacheKeys.close_indicators_df
        if self.cache.is_cache_locked(cache_key):
            LOG.info("已经有其他进程在计算收盘指标了，不再重复计算。")
            return

        positions = BrokerAPI.get_positions(available_only=True)  # type: ignore
        closable_positions_codes = [
            pos.code for pos in positions if pos.code in quote_df.index
        ]

        if not closable_positions_codes:
            LOG.info("当前没有可平仓位。")
            return

        with self.cache.use_redis_cache(
            cache_key, timeouts_conf.compute_close_indicators
        ) as (df, set_cache):
            if isinstance(df, pd.DataFrame):
                LOG.info("已从缓存读取了收盘指标，不再重复计算。交易行为应该与上次相同。")
                return df

            # Compute indicators
            hist_df = self._load_hist_daily_from_cache()
            hist_df = hist_df.loc[closable_positions_codes].copy()
            quote_df = quote_df.loc[closable_positions_codes].copy()
            raw_df = self._merge_hist_daily_and_current_quote(hist_df, quote_df)

            with timeit() as timer:
                ind_df = self.strategy.compute_close_indicators(raw_df)
                today = quote_df.iloc[0]["timestamp"]
                ind_df = ind_df.query("timestamp == @today").copy()
            LOG.info(f"计算收盘指标: {timer['seconds']}s")

            # Cache the result
            assert set_cache
            set_cache(ind_df)
            return ind_df

    def _intraday_trade(self, ind_df: pd.DataFrame, quote_df: pd.DataFrame):
        positions: list[Position] = BrokerAPI.get_positions(available_only=True)  # type: ignore
        trade_date = ind_df.iloc[0]["timestamp"]

        # [1] Sell existing positions
        # NOTE:
        # Use the current quote frame to decide whether to take profit or stop loss.
        # This is to avoid the situation where position stock does not appear in the
        # indicator frame. This happens if the indicator frame is incomplete for some reason.
        sell_orders: list[Order] = []
        for pos in positions:
            if pos.code not in quote_df.index:
                # The stock is probably suspended today
                continue

            bar = quote_df.loc[pos.code].to_dict()  # type: ignore

            # Take profit
            if take_profit_price := self.should_take_profit(self.strategy, bar, pos):
                pos.update_price(
                    self._jit_sell_price(
                        take_profit_price, self.strategy_conf.take_profit_slip
                    )
                )
                sell_orders.append(pos.to_sell_order(trade_date, action="止盈"))

            # Stop loss
            elif stop_loss_price := self.should_stop_loss(self.strategy, bar, pos):
                pos.update_price(
                    self._jit_sell_price(
                        stop_loss_price, self.strategy_conf.stop_loss_slip
                    )
                )
                sell_orders.append(pos.to_sell_order(trade_date, action="止损"))

        if sell_orders:
            LOG.info("发送卖出指令")
            LOG.log_orders(sell_orders)
            BrokerAPI.place_orders(sell_orders)

        # [2] Buy stocks
        orders = BrokerAPI.get_orders()
        port_df = self._get_buy_options(
            ind_df, orders, positions
        )  # list[DF_Index, BuyPrice]
        if not port_df.empty:
            n_bought = sum(1 for o in orders if o.direction == "buy")
            n_signals = len(port_df)

            avail_opens_count = max(0, self.strategy_conf.max_position_opens - n_bought)
            port_df, budget = self.strategy.adjust_portfolio_and_budget(
                port_df,
                budget=self.account.free_cash_amount,
                total_asset_value=self.account.total_asset_value,
                n_stocks=len(ind_df),
                max_position_opens=avail_opens_count,
            )

            buy_orders = self.strategy.generate_buy_orders(port_df, budget)
            LOG.info(
                f"当日已买入{n_bought}, 最大可开仓位{self.strategy_conf.max_position_opens}, "
                f"当前可用资金{self.account.free_cash_amount}. "
                f"今日剩余开仓限额{avail_opens_count}, 实际开仓{len(buy_orders)}. "
                f"触发买入{n_signals}"
            )
            if buy_orders:
                LOG.info("发送买入指令")
                BrokerAPI.place_orders(buy_orders)
                LOG.log_orders(buy_orders)

    def _pre_close_trade(self, ind_df: pd.DataFrame):
        close_codes = self._get_close_codes(ind_df)
        if not close_codes:
            LOG.info("没有需要平仓的股票")
            return

        positions_to_close: list[Position] = [
            pos
            for pos in BrokerAPI.get_positions(available_only=True)  # type: ignore
            if pos.code in close_codes
        ]
        sell_orders, trade_date = [], ind_df.iloc[0]["timestamp"]

        for pos in positions_to_close:
            bar = ind_df.loc[pos.code].to_dict()  # type: ignore
            real_price = self.adjust_factors.to_real_price(pos.code, bar["close"])
            pos.update_price(real_price)
            sell_orders.append(pos.to_sell_order(trade_date, action="平仓"))

        if sell_orders:
            LOG.info("发送卖出指令")
            LOG.log_orders(sell_orders)
            BrokerAPI.place_orders(sell_orders)

    @timeout(timeouts_conf.compute_open_indicators + 30)
    def on_pre_market_open_call_p2(self, quote_df: pd.DataFrame):
        ind_df = self._compute_open_indicators(quote_df)
        if isinstance(ind_df, pd.DataFrame) and not ind_df.empty:
            self._intraday_trade(ind_df, quote_df)

    @timeout(timeouts_conf.compute_close_indicators + 30)
    def on_cont_trade_pre_close(self, quote_df: pd.DataFrame):
        if not BrokerAPI.get_positions(available_only=True):  # type: ignore
            LOG.info("当前没有可用的持仓仓位，不执行收盘平仓交易逻辑")
            return

        ind_df = self._compute_close_indicators(quote_df)
        if isinstance(ind_df, pd.DataFrame):
            self._pre_close_trade(ind_df)

    @timeout(timeouts_conf.handle_cont_trade)
    def on_cont_trade(self, quote_df: pd.DataFrame):
        cache_key = CacheKeys.indicators_df
        if not self.cache.is_cache_locked(cache_key):
            LOG.warn("已进入盘中交易, 但指标仍在计算中!")
            return

        ind_df = self._load_indicators_from_cache(cache_key)
        assert ind_df
        quote_df = self.strategy.adjust_stocks_latest_prices(quote_df)
        ind_df.update(quote_df)
        self._intraday_trade(ind_df, quote_df)

    @require_mode("live-trading", "paper-trading")
    def handle_tick(self, market_phase: MarketPhase, quote_df: pd.DataFrame):
        quote_df["timestamp"] = str(get_latest_trade_date())
        quote_df["code"] = quote_df.index

        match market_phase:
            case MarketPhase.PRE_OPEN_CALL_P2:
                self.on_pre_market_open_call_p2(quote_df)

            case MarketPhase.CONT_TRADE:
                self.on_cont_trade(quote_df)

            case MarketPhase.CONT_TRADE_PRE_CLOSE:
                self.on_cont_trade_pre_close(quote_df)

    @require_mode("live-trading", "paper-trading")
    def handle_cancel_expired_orders(
        self, pending_orders: list[Order], stock_ask_bids: dict[str, AskBid]
    ):
        if (expiry_seconds := self.conf.pending_order_expiry) <= 0:
            LOG.warn("未设置订单过期时间, 不执行撤单逻辑")
            return

        orders_to_cancel = []
        for o in pending_orders:
            if o.duration < expiry_seconds:
                continue

            comment = f"订单[{o.short_description}]未能在{expiry_seconds}秒内成交, 已超时{o.duration - expiry_seconds}秒"
            pos_1_price = stock_ask_bids[o.code][o.direction][0]
            if o.price == pos_1_price:
                comment += f", 但仍在买1/卖1队列中, 暂不撤单"
                LOG.info(comment)
                continue

            LOG.info(comment)
            orders_to_cancel.append(o)

        if orders_to_cancel:
            LOG.info(f"撤单以下委托: {orders_to_cancel}")
            BrokerAPI.cancel_orders(orders_to_cancel)
