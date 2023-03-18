import os
import io
import pickle
import pandas as pd
from functools import cached_property
from datetime import date
from pathlib import Path
from celery import shared_task

import tradepy
from tradepy.constants import CacheKeys, Timeouts
from tradepy.core.context import Context
from tradepy.core.strategy import LiveStrategy
from tradepy.decorators import timeout
from tradepy.types import MarketPhase
from tradepy.bot import broker
from tradepy.warehouse import AdjustFactorDepot


LOG = tradepy.LOG


class TradingEngine:

    def __init__(self) -> None:
        self.workspace = Path.home() / ".tradepy" / "workspace" / str(date.today())
        self.workspace.mkdir(exist_ok=True, parents=True)

        self.ctx = Context(
            cash_amount=broker.get_account_free_cash_amount(),
            trading_unit=int(os.environ["TRADE_UNIT"]),
            stop_loss=float(os.environ["TRADE_STOP_LOSS"]),
            take_profit=float(os.environ["TRADE_TAKE_PROFIT"]),
            hfq_adjust_factors=AdjustFactorDepot.load(),
        )
        self.strategy: LiveStrategy = tradepy.config.get_strategy_class()(self.ctx)

    @cached_property
    def redis_client(self):
        return tradepy.config.get_redis_client()

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
            raise Exception(f'Should not reach here!!! Fetching cache key "{cache_key}" whose value is expected to be bytes.')
        except UnicodeDecodeError as e:
            return pickle.loads(e.object)

    def _pre_compute_indicators(self, quote_df: pd.DataFrame) -> pd.DataFrame | None:
        lock_key = CacheKeys.compute_indicators
        if self.redis_client.get(lock_key):
            LOG.info("Another process is computing indicators, skip this time.")
            return

        with self.redis_client.lock(lock_key, timeout=Timeouts.pre_compute_indicators, sleep=1):
            cache_key = CacheKeys.indicators_df
            if not self.redis_client.exists(cache_key):
                quote_df = self.strategy.pre_compute_indicators(quote_df)

                self.redis_client.set(cache_key, pickle.dumps(quote_df))
                quote_df.to_pickle(self.workspace / f"{cache_key}.pkl")
                return quote_df
            else:
                LOG.info("Indicators has been computed. Won't compute again and the trade actions will probably the same.")
                val = self._read_dataframe_from_cache(cache_key)
                assert isinstance(val, pd.DataFrame) and not val.empty, "Indicators cache was set but the value is either not found or empty??"
                return val

    def _adjust_prices(self, quote_df: pd.DataFrame):
        assert isinstance(self.ctx.hfq_adjust_factors, pd.DataFrame)

        if not (adj_df := self._read_dataframe_from_cache(CacheKeys.latest_adjust_factors)):
            LOG.info('Compute and cache the latest adjustment factor for each stock.')
            adj_df = (
                self.ctx.hfq_adjust_factors
                .groupby("code")
                .apply(lambda x: x.sort_values("timestamp").iloc[-2])
            )
            rd = tradepy.config.get_redis_client()
            rd.set(CacheKeys.latest_adjust_factors, pickle.dumps(adj_df))

        quote_df["close"] *= adj_df["hfq_factor"]
        quote_df["low"] *= adj_df["hfq_factor"]
        quote_df["high"] *= adj_df["hfq_factor"]
        quote_df["open"] *= adj_df["hfq_factor"]
        return quote_df.round(2)

    def _trade(self, ind_df: pd.DataFrame):
        ...

    @timeout(Timeouts.handle_pre_market_open_call)
    def on_pre_market_open_call_p2(self, quote_df: pd.DataFrame):
        ind_df = self._pre_compute_indicators(quote_df)
        if isinstance(ind_df, pd.DataFrame) and not ind_df.empty:
            self._trade(ind_df)

    @timeout(Timeouts.handle_cont_trade)
    def on_cont_trade(self, quote_df: pd.DataFrame):
        quote_df = self._adjust_prices(quote_df)
        ind_df = self._read_dataframe_from_cache(CacheKeys.indicators_df)
        assert isinstance(ind_df, pd.DataFrame)

        ind_df.update(quote_df)
        self._trade(ind_df)

    @timeout(Timeouts.handle_cont_trade_pre_close)
    def on_cont_trade_pre_close(self, quote_df: pd.DataFrame):
        quote_df = self._adjust_prices(quote_df)

    def handle_tick(self, market_phase: MarketPhase, quote_df: pd.DataFrame):
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
        quote_df=pd.read_csv(quote_df_reader, index_col="code"),
    )
