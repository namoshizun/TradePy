from typing import Any

import polars as pl
from financetoolkit.performance import performance_model
from financetoolkit.risk import risk_model

from tradepy.trade_book import TradeBook


class BasicEvaluator:
    def __init__(self, trade_book: TradeBook):
        self.trade_book = trade_book

    @property
    def capitals(self) -> pl.Series:
        return self.trade_book.cap_logs_df["capital"]

    @property
    def returns(self) -> pl.Series:
        return self.trade_book.cap_logs_df["pct_chg"]

    @property
    def trades_df(self) -> pl.DataFrame:
        return self.trade_book.trade_logs_df

    def get_profit_factor(self) -> float:
        r = self.returns
        gross_profit = float(r.filter(r > 0).sum() or 0)
        gross_loss = float(r.filter(r < 0).abs().sum() or 0)
        return (
            round(gross_profit / gross_loss, 2) if gross_loss else float("inf")
        )

    def get_max_drawdown(self) -> float:
        result = risk_model.get_max_drawdown(self.returns.to_pandas())
        return round(100 * float(result), 2)  # type: ignore[arg-type]

    def get_sharpe_ratio(self) -> float:
        # get_sharpe_ratio(series) returns r / std(r); its mean equals mean(r)/std(r)
        result = performance_model.get_sharpe_ratio(self.returns.to_pandas())
        return round(float(result.mean()), 2)

    def get_total_returns(self) -> float:
        cap = self.capitals
        return round(100 * float(cap[-1]) / float(cap[0]), 2)  # type: ignore[arg-type]

    def get_win_rate(self) -> float:
        closing = self.trades_df.filter(pl.col("action") != "开仓")
        wins = int((closing["pct_chg"] > 0).sum())
        total = len(closing)
        return round(100 * wins / total, 2) if total else 0.0

    def get_number_of_trades(self) -> int:
        return int((self.trades_df["action"] == "开仓").sum())

    def get_number_of_stop_loss(self) -> int:
        return int((self.trades_df["action"] == "止损").sum())

    def get_number_of_take_profit(self) -> int:
        return int((self.trades_df["action"] == "止盈").sum())

    def get_number_of_close(self) -> int:
        return int((self.trades_df["action"] == "平仓").sum())

    def get_avg_return(self) -> float:
        pct_chgs = self.trades_df["pct_chg"].drop_nulls()
        mean = pct_chgs.mean()
        return round(mean, 2) if isinstance(mean, float) else 0.0

    def get_stddev_return(self) -> float:
        pct_chgs = self.trades_df["pct_chg"].drop_nulls()
        std = pct_chgs.std()
        return round(std, 2) if isinstance(std, float) else 0.0

    def evaluate_trades(self) -> dict[str, Any]:
        return {
            "total_returns": self.get_total_returns(),
            "max_drawdown": self.get_max_drawdown(),
            "sharpe_ratio": self.get_sharpe_ratio(),
            "profit_factor": self.get_profit_factor(),
            "win_rate": self.get_win_rate(),
            "number_of_trades": self.get_number_of_trades(),
            "number_of_stop_loss": self.get_number_of_stop_loss(),
            "number_of_take_profit": self.get_number_of_take_profit(),
            "number_of_close": self.get_number_of_close(),
            "avg_return": self.get_avg_return(),
            "stddev_return": self.get_stddev_return(),
        }

    def basic_report(self):
        metrics = self.evaluate_trades()
        print(
            f"""
===========
开仓 = {metrics["number_of_trades"]}
止损 = {metrics["number_of_stop_loss"]}
止盈 = {metrics["number_of_take_profit"]}
提前平仓 = {metrics["number_of_close"]}
胜率 {metrics["win_rate"]}%
盈亏比 = {metrics["profit_factor"]}
最大回撤 = {metrics["max_drawdown"]}%
期末收益 = {metrics["total_returns"]}%
平均开仓收益: {metrics["avg_return"]}% (标准差: {metrics["stddev_return"]}%)
夏普比率: {metrics["sharpe_ratio"]}
==========="""
        )
