import talib
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import tradepy.trade_cal
from tradepy.core.trade_book import TradeBook
from tradepy.warehouse import BroadBasedIndexBarsDepot


def plot_capital_curve(trade_book: TradeBook, since_date="1900-01-01", until_date="3000-01-01"):
    index_df = (
        BroadBasedIndexBarsDepot.load()
        .loc["SSE"]
        .set_index("timestamp")
        .rename(columns={
            "close": "SSE-close",
            "low": "SSE-low",
            "high": "SSE-high",
            "open": "SSE-open",
        })
    )

    # -----
    cap_df = trade_book.cap_logs_df.copy()
    cap_df.index = cap_df.index.astype(str)
    cap_df = cap_df.join(index_df)

    for col in ["capital", "market_value", "free_cash_amount"]:
        cap_df[col] = cap_df[col].reset_index()[col].interpolate(method="pad").values

    cap_df.dropna(inplace=True)
    cap_df = cap_df.query('@since_date <= timestamp <= @until_date')

    # -----
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Candlestick(
            x=cap_df.index,
            open=cap_df['SSE-open'],
            high=cap_df['SSE-high'],
            low=cap_df['SSE-low'],
            close=cap_df['SSE-close'],
            increasing_line_color='red',
            decreasing_line_color='green'
        ),
        secondary_y=True
    )
    fig.add_trace(
        go.Scatter(
            x=cap_df.index,
            y=cap_df["capital"],
            mode='lines',
            line=dict(color='blue', width=1),
        ),
        secondary_y=False
    )
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),  # hide weekends
        ]
    )
    fig.show()


def plot_bars(bars_df, code: str, buy_date: str, window_size: tuple[int, int] = (200, 60)):
    trade_cal = sorted(tradepy.trade_cal.trade_cal)

    df = bars_df.query("code == @code").reset_index().copy()
    df["ma60"] = talib.SMA(df["close"], 60).round(2)
    df["ma20"] = talib.SMA(df["close"], 20).round(2)
    df["ma5"] = talib.SMA(df["close"], 5).round(2)

    mid_day_cal_idx = trade_cal.index(buy_date)
    sicne_date = trade_cal[max(mid_day_cal_idx - window_size[0], 0)]
    until_date = trade_cal[min(mid_day_cal_idx + window_size[1], len(trade_cal) - 1)]

    df = df.query('@sicne_date <= timestamp <= @until_date')
    df.dropna(inplace=True)

    fig = go.Figure(data=[
        go.Candlestick(
            x=df["timestamp"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            text=df["pct_chg"].astype(str) + "%",
            increasing_line_color='red',
            decreasing_line_color='green'
        ),
        go.Scatter(
            x=df["timestamp"],
            y=df["ma5"],
            mode='lines',
            line=dict(color='blue', width=1),
            hoverinfo="skip",
        ),
        go.Scatter(
            x=df["timestamp"],
            y=df["ma20"],
            mode='lines',
            line=dict(color='purple', width=1),
            hoverinfo="skip",
        ),
        go.Scatter(
            x=df["timestamp"],
            y=df["ma60"],
            mode='lines',
            line=dict(color='grey', width=1),
            hoverinfo="skip",
        ),
    ])
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),  # hide weekends
        ]
    )
    fig.update_layout(
        title=f'{code}: {sicne_date} => {until_date}',
        shapes=[
            dict(x0=buy_date, x1=buy_date, y0=0, y1=1, xref='x', yref='paper', line_width=1),
        ],
    )
    fig.show()


def plot_succ_rate_vs_positions_count(trade_book: TradeBook):
    trade_logs = pd.DataFrame(trade_book.trade_logs)
    trade_logs["timestamp"] = pd.to_datetime(trade_logs["timestamp"])
    trade_logs.set_index("timestamp", inplace=True)

    monthly_df = pd.DataFrame([
        {
            "timestamp": ts,
            "stop_loss": len(g.query('tag == "止损"')),
            "take_profit": len(g.query('tag == "止盈"')),
        }
        for ts, g in trade_logs.groupby(pd.Grouper(freq="M"))
    ]).set_index("timestamp")

    monthly_df = monthly_df.replace(0, np.nan).dropna()
    monthly_df["total_open"] = monthly_df["take_profit"] + monthly_df["stop_loss"]
    monthly_df["succ_rate"] = (100 * monthly_df["take_profit"] / monthly_df["total_open"]).round(2)
    monthly_df["month"] = monthly_df.index.month
    monthly_df["year"] = monthly_df.index.year

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        name="开仓数量",
        x=monthly_df.index,
        y=monthly_df["total_open"],
        #     mode='lines',
        #     line=dict(color='blue', width=1),
    ), secondary_y=True)

    fig.add_trace(go.Scatter(
        name="胜率",
        x=monthly_df.index,
        y=monthly_df["succ_rate"],
        mode="markers"
    ), secondary_y=False)

    fig.update_layout(hovermode='x unified')

    fig.show()
