import talib
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import quantstats as qs
from plotly.subplots import make_subplots

import tradepy.trade_cal
from tradepy.trade_book import TradeBook
from tradepy.types import BroadIndexType
from tradepy.depot.index import BroadBasedIndexBarsDepot


def plot_equity_curve(
    trade_book: TradeBook,
    baseline_index: BroadIndexType = "SSE",
    since_date="1900-01-01",
    until_date="3000-01-01",
):
    index_df = (
        BroadBasedIndexBarsDepot.load()
        .loc[baseline_index]
        .set_index("timestamp")
        .rename(
            columns={
                "close": f"{baseline_index}-close",
                "low": f"{baseline_index}-low",
                "high": f"{baseline_index}-high",
                "open": f"{baseline_index}-open",
            }
        )
    )

    # -----
    cap_df = trade_book.cap_logs_df.copy()
    cap_df.index = cap_df.index.astype(str)
    cap_df = cap_df.join(index_df)

    for col in ["capital", "market_value", "free_cash_amount"]:
        cap_df[col] = cap_df[col].reset_index()[col].interpolate(method="pad").values

    cap_df.dropna(inplace=True)
    cap_df.query("@since_date <= timestamp <= @until_date", inplace=True)
    cap_df["drawdown"] = (100 * qs.stats.to_drawdown_series(cap_df["capital"])).round(2)

    # -----
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[3, 1],
        vertical_spacing=0.01,
        specs=[[{"secondary_y": True}], [dict()]],
    )
    fig.add_trace(
        go.Candlestick(
            name=baseline_index,
            x=cap_df.index,
            open=cap_df[f"{baseline_index}-open"],
            high=cap_df[f"{baseline_index}-high"],
            low=cap_df[f"{baseline_index}-low"],
            close=cap_df[f"{baseline_index}-close"],
            increasing_line_color="red",
            decreasing_line_color="green",
        ),
        secondary_y=True,
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            name="总资产",
            x=cap_df.index,
            y=cap_df["capital"],
            mode="lines",
            line=dict(color="blue", width=1),
        ),
        secondary_y=False,
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            name="最大回撤",
            x=cap_df.index,
            y=cap_df["drawdown"],
            fill="tozeroy",
        ),
        row=2,
        col=1,
    )
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),  # hide weekends
        ],
    )
    fig.update(
        layout_xaxis_rangeslider_visible=False,
    )
    fig.update_layout(
        hovermode="x unified",
        yaxis=dict(autorange=True, fixedrange=False),
        margin=dict(l=20, r=20, t=30, b=30),
    )
    fig.show()


def plot_return_box(
    trade_books: list[TradeBook],
    since_date="1900-01-01",
    until_date="2100-01-01",
):
    capital_curves_df = pd.DataFrame(
        {str(idx): book.cap_logs_df["capital"] for idx, book in enumerate(trade_books)}
    )

    capital_curves_df = capital_curves_df.reset_index().melt(
        id_vars=("timestamp",), var_name="run_id", value_name="capital"
    )

    dataset = capital_curves_df.query("@since_date <= timestamp <= @until_date")
    fig = px.box(dataset, x="timestamp", y="capital")
    fig.update_traces(quartilemethod="linear")
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),  # hide weekends
        ],
    )
    fig.show()


def plot_bars(
    bars_df, code: str, buy_date: str, window_size: tuple[int, int] = (200, 60)
):
    trade_cal = sorted(tradepy.trade_cal.trade_cal)

    df = bars_df.query("code == @code").reset_index().copy()
    df["ma60"] = talib.SMA(df["close"], 60).round(2)
    df["ma20"] = talib.SMA(df["close"], 20).round(2)
    df["ma5"] = talib.SMA(df["close"], 5).round(2)

    mid_day_cal_idx = trade_cal.index(buy_date)
    sicne_date = trade_cal[max(mid_day_cal_idx - window_size[0], 0)]
    until_date = trade_cal[min(mid_day_cal_idx + window_size[1], len(trade_cal) - 1)]

    df = df.query("@sicne_date <= timestamp <= @until_date")
    df.dropna(inplace=True)

    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["timestamp"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                text=df["pct_chg"].astype(str) + "%",
                increasing_line_color="red",
                decreasing_line_color="green",
            ),
            go.Scatter(
                x=df["timestamp"],
                y=df["ma5"],
                mode="lines",
                line=dict(color="blue", width=1),
                hoverinfo="skip",
            ),
            go.Scatter(
                x=df["timestamp"],
                y=df["ma20"],
                mode="lines",
                line=dict(color="purple", width=1),
                hoverinfo="skip",
            ),
            go.Scatter(
                x=df["timestamp"],
                y=df["ma60"],
                mode="lines",
                line=dict(color="grey", width=1),
                hoverinfo="skip",
            ),
        ]
    )
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),  # hide weekends
        ]
    )
    fig.update_layout(
        title=f"{code}: {sicne_date} => {until_date}",
        shapes=[
            dict(
                x0=buy_date,
                x1=buy_date,
                y0=0,
                y1=1,
                xref="x",
                yref="paper",
                line_width=1,
            ),
        ],
    )
    fig.show()
