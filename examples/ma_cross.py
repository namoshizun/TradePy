from typing import Annotated

import polars as pl

from tradepy.core.config import StrategyConf
from tradepy.strategy import (
    SMA,
    BacktestStrategyBase,
    Lag,
    OriginalPrice,
)
from tradepy.strategy.indicators import Volatility


class MACrossConf(StrategyConf):
    min_volatility: float = 2
    min_stock_price: float = 3
    stop_loss: float = 5
    take_profit: float = 8
    max_position_size: float = 0.2
    max_position_opens: int = 10
    min_trade_amount: int = 5000


MA10 = Annotated[float, SMA(10)]
MA10_REF1 = Annotated[MA10, Lag(1)]
MA30 = Annotated[float, SMA(30)]
MA30_REF1 = Annotated[MA30, Lag(1)]
MA120 = Annotated[float, SMA(120)]


class MACrossStrategy(BacktestStrategyBase[MACrossConf]):
    def buy(
        self,
        name: str,
        close: float,
        orig_open: Annotated[float, OriginalPrice(column="open")],
        volatility: Annotated[float, Volatility("atr")],
        ma10: MA10,
        ma10_ref1: MA10_REF1,
        ma30: MA30,
        ma30_ref1: MA30_REF1,
        ma120: MA120,
    ) -> float | None:
        if "ST" in name:
            return

        if orig_open < self.config.min_stock_price:
            return

        if volatility < self.config.min_volatility:
            return

        if (ma10 > ma120) and (ma10_ref1 < ma30_ref1) and (ma10 > ma30):
            return close

    def sell(
        self,
        close: float,
        ma10: MA10,
        ma30: MA30,
        ma10_ref1: MA10_REF1,
        ma30_ref1: MA30_REF1,
    ) -> float | None:
        if (ma10_ref1 > ma30_ref1) and (ma10 < ma30):
            return close

    def pre_process(self, df: pl.DataFrame) -> pl.DataFrame:
        name_cats = df["name"].cat.get_categories()
        st_names = name_cats.filter(name_cats.str.contains("ST"))

        # 过滤掉北交所 / ST
        return df.filter(
            (pl.col("exchange") != "BJ") & ~(pl.col("name").is_in(st_names))
        )
