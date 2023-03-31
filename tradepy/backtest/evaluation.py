import pandas as pd
import quantstats as qs
from tradepy.trade_book import TradeBook


class ResultEvaluator:

    @staticmethod
    def html_report(trade_book: TradeBook, **kwargs):
        qs.reports.html(trade_book.cap_logs_df["pct_chg"], **kwargs)

    @staticmethod
    def basic_report(trade_book: TradeBook):
        # Trade logs
        df = trade_book.trade_logs_df.copy().reset_index()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)

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
        cap_returns = trade_book.cap_logs_df["pct_chg"]
        sharp_ratio = round(qs.stats.sharpe(cap_returns), 2)

        print(f'''
===========
开仓 = {action_counts["开仓"]}
止损 = {action_counts["止损"]}
止盈 = {action_counts["止盈"]}
平仓亏损 = {close_lose}
平仓盈利 = {close_wins}

胜率 {succ_rate}%
平均收益: {avg_pct_chg}% (标准差: {std_pct_chg}%)
夏普比率: {sharp_ratio}

平仓收益统计:
{close_results.groupby("win_close")["pct_chg"].describe().round(2)}
===========
        ''')
