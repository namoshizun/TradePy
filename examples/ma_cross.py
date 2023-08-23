import pandas as pd
from tradepy.strategy.factors import FactorsMixin
from tradepy.strategy.base import BacktestStrategy, BuyOption
from tradepy.decorators import tag


class MovingAverageCrossoverStrategy(BacktestStrategy, FactorsMixin):
    @tag(outputs=["ema10_ref1", "sma30_ref1"], notna=True)
    def moving_averages_ref1(self, ema10, sma30) -> pd.Series:
        return ema10.shift(1), sma30.shift(1)

    def should_buy(
        self,
        orig_open,
        sma120,
        ema10,
        sma30,
        typical_price,
        atr,
        ema10_ref1,
        sma30_ref1,
        close,
        company,
    ) -> BuyOption | None:
        if "ST" in company:
            return

        if orig_open < self.min_stock_price:
            return

        volatility = 100 * atr / typical_price
        if volatility < self.min_volatility:
            return

        if (ema10 > sma120) and (ema10_ref1 < sma30_ref1) and (ema10 > sma30):
            return close, 1

    def should_close(self, ema10, sma30, ema10_ref1, sma30_ref1):
        return (ema10_ref1 > sma30_ref1) and (ema10 < sma30)

    def pre_process(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.query('market != "科创板"').copy()
