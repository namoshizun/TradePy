import abc
from typing import Any
import numpy as np
import pandas as pd
import quantstats as qs
from dataclasses import dataclass
from tradepy.trade_book import TradeBook
from tradepy.trade_cal import trade_cal


def coerce_type(type_):
    def inner(func):
        def dec(*args, **kwargs):
            return type_(func(*args, **kwargs))

        return dec

    return inner


class ResultEvaluator:
    @abc.abstractclassmethod
    def evaluate_trades(cls):
        raise NotImplementedError


@dataclass
class BasicEvaluator(ResultEvaluator):
    trade_book: TradeBook

    @property
    def capitals(self) -> pd.Series:
        return self.trade_book.cap_logs_df["capital"]

    @property
    def returns(self) -> pd.Series:
        return self.trade_book.cap_logs_df["pct_chg"]

    @property
    def trades_df(self):
        return self.trade_book.trade_logs_df

    @coerce_type(float)
    def get_profit_factor(self) -> float:
        return qs.stats.profit_factor(self.returns)

    @coerce_type(float)
    def get_max_drawdown(self) -> float:
        res = qs.stats.max_drawdown(self.capitals)
        return round(100 * res, 2)  # type: ignore

    @coerce_type(float)
    def get_sharpe_ratio(self) -> float:
        r = qs.stats.sharpe(self.returns)
        return round(r, 2)  # type: ignore

    @coerce_type(float)
    def get_total_returns(self) -> float:
        return round(100 * self.capitals.iloc[-1] / self.capitals.iloc[0], 2)

    @coerce_type(float)
    def get_win_rate(self) -> float:
        wins = (self.trades_df.query('action != "开仓"')["pct_chg"] > 0).sum()
        loss = (self.trades_df.query('action != "开仓"')["pct_chg"] <= 0).sum()
        return round(100 * wins / (wins + loss), 2)

    @coerce_type(int)
    def get_number_of_trades(self) -> int:
        return (self.trades_df["action"] == "开仓").sum()

    @coerce_type(int)
    def get_number_of_stop_loss(self) -> int:
        return (self.trades_df["action"] == "止损").sum()

    @coerce_type(int)
    def get_number_of_take_profit(self) -> int:
        return (self.trades_df["action"] == "止盈").sum()

    @coerce_type(int)
    def get_number_of_close(self) -> int:
        return (self.trades_df["action"] == "平仓").sum()

    @coerce_type(float)
    def get_avg_return(self) -> float:
        pct_chgs = self.trades_df["pct_chg"].dropna()
        return round(pct_chgs.mean(), 2)

    @coerce_type(float)
    def get_stddev_return(self) -> float:
        pct_chgs = self.trades_df["pct_chg"].dropna()
        return round(pct_chgs.std(), 2)

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

    def html_report(self, **kwargs):
        qs.reports.html(self.trade_book.cap_logs_df["pct_chg"], **kwargs)

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
最大回撤 = {metrics["max_drawdown"]}%
期末资金 = {metrics["total_returns"]}%
平均开仓收益: {metrics["avg_return"]}% (标准差: {metrics["stddev_return"]}%)
夏普比率: {metrics["sharpe_ratio"]}
==========="""
        )
