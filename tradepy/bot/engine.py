import random
import pickle
import pandas as pd
from functools import cached_property
from datetime import date
from pathlib import Path

import tradepy
from tradepy.blacklist import Blacklist
from tradepy.constants import CacheKeys
from tradepy.core.adjust_factors import AdjustFactors
from tradepy.core.conf import TradingConf
from tradepy.core.models import Order, Position
from tradepy.strategy.base import LiveStrategy
from tradepy.decorators import require_mode, timeout
from tradepy.mixins import TradeMixin
from tradepy.types import MarketPhase
from tradepy.bot.broker import BrokerAPI
from tradepy.depot.misc import AdjustFactorDepot
from tradepy.utils import get_latest_trade_date


LOG = tradepy.LOG
timeouts_conf = tradepy.config.trading.timeouts


class TradingEngine(TradeMixin):
    def __init__(self) -> None:
        self.workspace = Path.home() / ".tradepy" / "workspace" / str(date.today())
        self.workspace.mkdir(exist_ok=True, parents=True)

        self.conf: TradingConf = tradepy.config.trading  # type: ignore
        assert self.conf
        self.strategy_conf = self.conf.strategy

        self.adjust_factors: AdjustFactors = AdjustFactorDepot.load()
        self.account = BrokerAPI.get_account()
        self.strategy: LiveStrategy = self.strategy_conf.load_strategy()  # type: ignore

    @cached_property
    def redis_client(self):
        return self.conf.get_redis_client()

    def _jit_sell_price(self, price: float, slip_pct: float) -> float:
        slip = slip_pct * 1e-2
        if tradepy.config.common.mode == "paper-trading":
            jitter = random.uniform(0, slip)
            return price * (1 - jitter)
        return price * (1 - slip)

    def _read_dataframe_from_cache(self, cache_key: str) -> pd.DataFrame | None:
        if not self.redis_client.exists(cache_key):
            # Try local disk
            cache_file = self.workspace / f"{cache_key}.pkl"
            if cache_file.exists():
                return pd.read_pickle(cache_file)

            # Ooops still miss
            return None

        try:
            self.redis_client.get(cache_key)
            raise Exception("不应该到这里!缓存中的dataframe应该是bytes类型的。")
        except UnicodeDecodeError as e:
            return pickle.loads(e.object)

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
            if self.strategy.should_close(*indicators)
        ]

    def _compute_open_indicators(self, quote_df: pd.DataFrame) -> pd.DataFrame | None:
        lock_key = CacheKeys.compute_open_indicators
        if self.redis_client.get(lock_key):
            LOG.info("已经有其他进程在计算开盘指标了，不再重复计算。")
            return

        with self.redis_client.lock(
            lock_key, timeout=timeouts_conf.compute_open_indicators, sleep=1
        ):
            cache_key = CacheKeys.indicators_df
            if not self.redis_client.exists(cache_key):
                ind_df = self.strategy.compute_open_indicators(quote_df)

                self.redis_client.set(cache_key, pickle.dumps(ind_df))
                ind_df.to_pickle(self.workspace / f"{cache_key}.pkl")
                return ind_df
            else:
                LOG.info("已从缓存读取了预计算指标，不再重复计算。交易行为应该与上次相同。")
                val = self._read_dataframe_from_cache(cache_key)
                assert (
                    isinstance(val, pd.DataFrame) and not val.empty
                ), "Indicators cache was set but the value is either not found or empty??"
                return val

    def _compute_close_indicators(
        self, quote_df: pd.DataFrame, ind_df: pd.DataFrame
    ) -> pd.DataFrame | None:
        lock_key = CacheKeys.compute_close_indicators
        if self.redis_client.get(lock_key):
            LOG.info("已经有其他进程在计算收盘指标了，不再重复计算。")
            return

        with self.redis_client.lock(
            lock_key, timeout=timeouts_conf.compute_close_indicators, sleep=1
        ):
            positions = BrokerAPI.get_positions(available_only=True)  # type: ignore
            closable_positions_codes = [
                pos.code for pos in positions if pos.code in ind_df.index
            ]

            if not closable_positions_codes:
                LOG.info("当前没有可平仓位。")
                return

            cache_key = CacheKeys.close_indicators_df
            if not self.redis_client.exists(cache_key):
                ind_df = ind_df.loc[closable_positions_codes].copy()
                df = self.strategy.compute_close_indicators(quote_df.copy(), ind_df)

                self.redis_client.set(cache_key, pickle.dumps(df))
                df.to_pickle(self.workspace / f"{cache_key}.pkl")
                return df
            else:
                LOG.info("已从缓存读取了收盘指标，不再重复计算。交易行为应该与上次相同。")
                val = self._read_dataframe_from_cache(cache_key)
                assert (
                    isinstance(val, pd.DataFrame) and not val.empty
                ), "Indicators cache was set but the value is either not found or empty??"
                return val

    def _inday_trade(self, ind_df: pd.DataFrame, quote_df: pd.DataFrame):
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
                pos.close(
                    self._jit_sell_price(
                        take_profit_price, self.strategy_conf.take_profit_slip
                    )
                )
                sell_orders.append(pos.to_sell_order(trade_date, action="止盈"))

            # Stop loss
            elif stop_loss_price := self.should_stop_loss(self.strategy, bar, pos):
                pos.close(
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

        # [3] Cancel expired orders
        if (expiry := self.conf.strategy.pending_order_expiry) <= 0:
            return

        orders_to_cancel = [
            o for o in orders if o.pending_vol > 0 and o.duration >= expiry
        ]

        if orders_to_cancel:
            LOG.info(f'发送撤单指令: {orders_to_cancel}')
            BrokerAPI.cancel_orders(orders_to_cancel)

    def _pre_close_trade(self, ind_df: pd.DataFrame):
        close_codes = self._get_close_codes(ind_df)
        if not close_codes:
            LOG.info("没有需要平仓的股票")
            return

        positions: list[Position] = [
            pos
            for pos in BrokerAPI.get_positions(available_only=True)  # type: ignore
            if pos.code in close_codes
        ]
        sell_orders, trade_date = [], ind_df.iloc[0]["timestamp"]

        for pos in positions:
            bar = ind_df.loc[pos.code].to_dict()  # type: ignore
            real_price = self.adjust_factors.to_real_price(pos.code, bar["close"])
            pos.close(real_price)
            sell_orders.append(pos.to_sell_order(trade_date, action="平仓"))

        if sell_orders:
            LOG.info("发送卖出指令")
            LOG.log_orders(sell_orders)
            BrokerAPI.place_orders(sell_orders)

    @timeout(timeouts_conf.handle_pre_market_open_call)
    def on_pre_market_open_call_p2(self, quote_df: pd.DataFrame):
        ind_df = self._compute_open_indicators(quote_df)
        if isinstance(ind_df, pd.DataFrame) and not ind_df.empty:
            self._inday_trade(ind_df, quote_df)

    @timeout(timeouts_conf.handle_cont_trade)
    def on_cont_trade(self, quote_df: pd.DataFrame):
        if self.redis_client.get(CacheKeys.compute_open_indicators):
            LOG.warn("已进入盘中交易, 但指标计算仍在进行中!")
            return

        ind_df = self._read_dataframe_from_cache(CacheKeys.indicators_df)
        ind_df = self.strategy.compute_intraday_indicators(quote_df.copy(), ind_df)
        self._inday_trade(ind_df, quote_df)

    @timeout(timeouts_conf.handle_cont_trade_pre_close)
    def on_cont_trade_pre_close(self, quote_df: pd.DataFrame):
        if not BrokerAPI.get_positions(available_only=True):  # type: ignore
            LOG.info("当前没有可用的持仓仓位，不执行收盘平仓交易逻辑")
            return

        ind_df = self._read_dataframe_from_cache(CacheKeys.indicators_df)
        ind_df = self._compute_close_indicators(quote_df, ind_df)
        if isinstance(ind_df, pd.DataFrame):
            self._pre_close_trade(ind_df)

    @require_mode("live-trading", "paper-trading")
    def handle_tick(self, market_phase: MarketPhase, quote_df: pd.DataFrame):
        trade_date = str(get_latest_trade_date())
        quote_df["timestamp"] = trade_date

        match market_phase:
            case MarketPhase.PRE_OPEN_CALL_P2:
                self.on_pre_market_open_call_p2(quote_df)

            case MarketPhase.CONT_TRADE:
                self.on_cont_trade(quote_df)

            case MarketPhase.CONT_TRADE_PRE_CLOSE:
                self.on_cont_trade_pre_close(quote_df)
