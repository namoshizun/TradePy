from dataclasses import dataclass
import pandas as pd
import quantstats as qs
from tradepy.trade_book import TradeBook


def coerce_type(type_):
    def inner(func):
        def dec(*args, **kwargs):
            return type_(func(*args, **kwargs))
        return dec
    return inner


@dataclass
class ResultEvaluator:

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
    def get_success_rate(self) -> float:
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
        return (self.trades_df["action"] == "平常").sum()

    def html_report(self, **kwargs):
        qs.reports.html(self.trade_book.cap_logs_df["pct_chg"], **kwargs)

    def basic_report(self):
        # Trade logs
        df = self.trades_df.copy().reset_index()

        # Count the number of trade actions, the counts of win / lose closes
        close_results = df.query('action == "平仓"').copy()
        close_results["win_close"] = close_results["pct_chg"] > 0
        action_counts = df.groupby("action").size()

        wins = action_counts["止盈"] + (close_wins := close_results["win_close"].sum())
        loses = action_counts["止损"] + (close_lose := (~close_results["win_close"]).sum())
        succ_rate = round(100 * wins / (wins + loses), 2)

        # Calculate return stats
        pct_chgs = df["pct_chg"].dropna()
        avg_pct_chg = round(pct_chgs.mean(), 2)
        std_pct_chg = round(pct_chgs.std(), 2)

        # Capital logs
        print(f'''
===========
开仓 = {action_counts["开仓"]}
止损 = {action_counts["止损"]}
止盈 = {action_counts["止盈"]}
平仓亏损 = {close_lose}
平仓盈利 = {close_wins}
最大回撤 = {self.get_max_drawdown()}%
总收益 = {self.get_total_returns()}%

胜率 {succ_rate}%
平均收益: {avg_pct_chg}% (标准差: {std_pct_chg}%)
夏普比率: {self.get_sharpe_ratio()}

平仓收益统计:
{close_results.groupby("win_close")["pct_chg"].describe().round(2)}
===========
        ''')
