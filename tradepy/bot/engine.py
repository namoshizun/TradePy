import os
import io
import pickle
from typing import Any
import pandas as pd
from functools import cached_property
from datetime import date
from pathlib import Path
from celery import shared_task

import tradepy
from tradepy.constants import CacheKeys, Timeouts
from tradepy.core.context import Context
from tradepy.core.order import Order
from tradepy.core.position import Position
from tradepy.core.strategy import LiveStrategy
from tradepy.decorators import timeout
from tradepy.types import MarketPhase
from tradepy.bot import broker
from tradepy.warehouse import AdjustFactorDepot


LOG = tradepy.LOG


def _load_ctx_vars_from_env():
    def deserialize_value(v):
        try:
            return float(v)
        except ValueError:
            return v

    def read_var_name(k):
        return k.replace("CTX_", "").lower()

    return {
        read_var_name(key): deserialize_value(val)
        for key, val in os.environ.items()
        if key.startswith("CTX_")
    }


class TradingEngine:

    def __init__(self) -> None:
        self.workspace = Path.home() / ".tradepy" / "workspace" / str(date.today())
        self.workspace.mkdir(exist_ok=True, parents=True)

        ctx_args = dict(
            cash_amount=broker.get_account_free_cash_amount(),  # type: ignore
            trading_unit=int(os.environ["TRADE_UNIT"]),
            stop_loss=float(os.environ["TRADE_STOP_LOSS"]),
            take_profit=float(os.environ["TRADE_TAKE_PROFIT"]),
            max_position_opens=int(os.environ["TRADE_MAX_POSITION_OPENS"]),
            max_position_size=float(os.environ["TRADE_MAX_POSITION_SIZE"]),
            hfq_adjust_factors=AdjustFactorDepot.load(),
        )
        ctx_args.update(_load_ctx_vars_from_env())

        self.ctx = Context.build(**ctx_args)
        self.strategy: LiveStrategy = tradepy.config.get_strategy_class()(self.ctx)
        self.latest_adjust_factors_df: pd.DataFrame = self._get_latest_adjust_factors()

    @cached_property
    def redis_client(self):
        return tradepy.config.get_redis_client()

    def _get_latest_adjust_factors(self) -> pd.DataFrame:
        adj_df = self._read_dataframe_from_cache(CacheKeys.latest_adjust_factors)
        if adj_df is None:
            assert isinstance(self.ctx.hfq_adjust_factors, pd.DataFrame)
            adj_df = (
                self.ctx.hfq_adjust_factors
                .groupby("code")
                .apply(lambda x: x.sort_values("timestamp").iloc[-2])
            )
            serialized = pickle.dumps(adj_df)
            self.redis_client.set(CacheKeys.latest_adjust_factors, serialized)
        return adj_df

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

    def _pre_compute_indicators(self, quote_df: pd.DataFrame) -> pd.DataFrame | None:
        lock_key = CacheKeys.compute_indicators
        if self.redis_client.get(lock_key):
            LOG.info("已经有其他进程在计算指标了，不再重复计算。")
            return

        with self.redis_client.lock(lock_key, timeout=Timeouts.pre_compute_indicators, sleep=1):
            cache_key = CacheKeys.indicators_df
            if not self.redis_client.exists(cache_key):
                ind_df = self.strategy.pre_compute_indicators(quote_df)

                self.redis_client.set(cache_key, pickle.dumps(ind_df))
                ind_df.to_pickle(self.workspace / f"{cache_key}.pkl")
                return ind_df
            else:
                LOG.info("已从缓存读取了预计算指标，不再重复计算。交易行为应该与上次相同。")
                val = self._read_dataframe_from_cache(cache_key)
                assert isinstance(val, pd.DataFrame) and not val.empty, "Indicators cache was set but the value is either not found or empty??"
                return val

    def _backward_adjust_prices(self, quote_df: pd.DataFrame):
        adj_df = self.latest_adjust_factors_df
        quote_df["close"] *= adj_df["hfq_factor"]
        quote_df["low"] *= adj_df["hfq_factor"]
        quote_df["high"] *= adj_df["hfq_factor"]
        quote_df["open"] *= adj_df["hfq_factor"]
        return quote_df.round(2)

    def _to_real_price(self, price: float, code: str) -> float:
        factor = self.latest_adjust_factors_df.loc[code, "hfq_factor"]
        return round(price / factor, 2)  # type: ignore

    def get_buy_options(self,
                        ind_df: pd.DataFrame,
                        orders: list[Order]) -> list[tuple[Any, float]]:
        already_ordered = set(o.code for o in orders)
        return [
            (code, self._to_real_price(price, code))
            for code, *indicators in ind_df[self.strategy.buy_indicators].itertuples(name=None)  # twice faster than the default .itertuples options
            if (code not in already_ordered) and (price := self.strategy.should_buy(*indicators))
        ]

    def _trade(self, ind_df: pd.DataFrame):
        positions: list[Position] = broker.get_positions(available_only=True)  # type: ignore
        trade_date = ind_df.iloc[0]["timestamp"]

        # [1] Sell existing positions
        sell_orders = []
        for pos in positions:
            if pos.code not in ind_df.index:
                # The stock is probably suspended today
                continue

            bar = ind_df.loc[pos.code].to_dict()  # type: ignore
            bar["close"] = self._to_real_price(bar["close"], pos.code)

            # Take profit
            if take_profit_price := self.strategy.should_take_profit(bar, pos):
                pos.close(take_profit_price)
                sell_orders.append(pos.to_sell_order(trade_date))

            # Stop loss
            elif stop_loss_price := self.strategy.should_stop_loss(bar, pos):
                pos.close(stop_loss_price * 0.99)  # to secure the order fulfillment
                sell_orders.append(pos.to_sell_order(trade_date))

        if sell_orders:
            LOG.info('发送卖出指令')
            LOG.log_orders(sell_orders)
            broker.place_orders(sell_orders)

        # [2] Buy stocks
        orders = broker.get_orders()  # type: ignore
        buy_options = self.get_buy_options(ind_df, orders)  # list[DF_Index, BuyPrice]
        if buy_options:
            free_cash_amount = broker.get_account_free_cash_amount()  # type: ignore
            port_df, budget = self.strategy.get_portfolio_and_budget(ind_df, buy_options, free_cash_amount)
            buy_orders = self.strategy.generate_buy_orders(port_df, budget)

            if buy_orders:
                broker.place_orders(buy_orders)
                LOG.info('发送买入指令')
                LOG.log_orders(buy_orders)

    @timeout(Timeouts.handle_pre_market_open_call)
    def on_pre_market_open_call_p2(self, quote_df: pd.DataFrame):
        ind_df = self._pre_compute_indicators(quote_df)  # NOTE: price adjustment will be done there as well
        if isinstance(ind_df, pd.DataFrame) and not ind_df.empty:
            self._trade(ind_df)

    @timeout(Timeouts.handle_cont_trade)
    def on_cont_trade(self, quote_df: pd.DataFrame):
        if self.redis_client.get(CacheKeys.compute_indicators):
            LOG.warn("已进入盘中交易, 但指标计算仍在进行中!")
            return

        quote_df = self._backward_adjust_prices(quote_df)
        ind_df = self._read_dataframe_from_cache(CacheKeys.indicators_df)
        assert isinstance(ind_df, pd.DataFrame)

        ind_df = self.strategy.update_indicators(ind_df, quote_df)
        self._trade(ind_df)

    @timeout(Timeouts.handle_cont_trade_pre_close)
    def on_cont_trade_pre_close(self, quote_df: pd.DataFrame):
        quote_df = self._backward_adjust_prices(quote_df)
        raise NotImplementedError("TODO")

    def handle_tick(self, market_phase: MarketPhase, quote_df: pd.DataFrame):
        trade_date = str(self.ctx.get_trade_date())
        quote_df["timestamp"] = trade_date

        match market_phase:
            case MarketPhase.PRE_OPEN_CALL_P2:
                self.on_pre_market_open_call_p2(quote_df)

            case MarketPhase.CONT_TRADE:
                self.on_cont_trade(quote_df)

            case MarketPhase.CONT_TRADE_PRE_CLOSE:
                self.on_cont_trade_pre_close(quote_df)


@shared_task(name="tradepy.handle_tick")
def handle_tick(payload):
    quote_df_reader = io.StringIO(payload["market_quote"])

    TradingEngine().handle_tick(
        market_phase=payload["market_phase"],
        quote_df=pd.read_csv(quote_df_reader, index_col="code", dtype={"code": str}),
    )
