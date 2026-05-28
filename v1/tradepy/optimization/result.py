import itertools
import random
import pickle
import quantstats as qs
import pandas as pd
import plotly.subplots as sp
import plotly.graph_objects as go
import plotly.express as px

from functools import cached_property, cache
from pathlib import Path
from sklearn import manifold

from tradepy.optimization.parameter import Parameter, ParameterGroup


class BacktestRunsResult:
    def __init__(self, workspace_dir: Path | str) -> None:
        if isinstance(workspace_dir, str):
            workspace_dir = Path(workspace_dir)
        self.workspace_dir = workspace_dir

    def load_capital_curves(self) -> pd.DataFrame:
        cap_df_list = []

        for trade_book_path in (self.workspace_dir / "workers").rglob(
            "**/trade_book.pkl"
        ):
            run_id = trade_book_path.parent.name
            with trade_book_path.open("rb") as fh:
                trade_book = pickle.load(fh)
                caps_df = trade_book.cap_logs_df
                caps_df["run_id"] = run_id
                cap_df_list.append(caps_df[["run_id", "capital"]])

        return pd.concat(cap_df_list)

    def plot_equity_curves(self, sample_runs: int | None = None):
        """
        绘制多轮回测的收益曲线

        :param sample_runs: 随机抽样的回测轮数
        """

        cap_curves_df = self.load_capital_curves()
        if sample_runs:
            run_ids = cap_curves_df["run_id"].unique().tolist()
            random.shuffle(run_ids)
            sampled_runs = run_ids[:sample_runs]
            cap_curves_df.query("run_id in @sampled_runs", inplace=True)

        fig = px.line(
            cap_curves_df.reset_index(),
            x="timestamp",
            y="capital",
            color="run_id",
            title="多轮回测的收益曲线",
        )
        fig.update_traces(showlegend=False)
        fig.show()

    def plot_equity_curve_bands(self):
        cap_curves_df = self.load_capital_curves()
        stats_df = cap_curves_df.groupby("timestamp")["capital"].agg(["mean", "std"])
        stats_df["lower"] = stats_df["mean"] - stats_df["std"]
        stats_df["upper"] = stats_df["mean"] + stats_df["std"]
        stats_df["drawdown"] = (
            100 * qs.stats.to_drawdown_series(stats_df["mean"])
        ).round(2)

        fig = sp.make_subplots(
            rows=2, cols=1, shared_xaxes=True, row_heights=[3, 1], vertical_spacing=0.01
        )

        # Add the main plot with return curves and bands to the first subplot
        fig.add_trace(
            go.Scatter(
                name="平均值",
                x=stats_df.index,
                y=stats_df["mean"],
                mode="lines",
                line=dict(color="rgb(31, 119, 180)"),
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                name="+1标准差",
                x=stats_df.index,
                y=stats_df["upper"],
                mode="lines",
                marker=dict(color="#444"),
                line=dict(width=0),
                showlegend=False,
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                name="-1标准差",
                x=stats_df.index,
                y=stats_df["lower"],
                marker=dict(color="#444"),
                line=dict(width=0),
                mode="lines",
                fillcolor="rgba(68, 68, 68, 0.3)",
                fill="tonexty",
                showlegend=False,
            ),
            row=1,
            col=1,
        )

        # Add the drawdown plot to the second subplot
        fig.add_trace(
            go.Scatter(
                name="最大回撤",
                x=stats_df.index,
                y=stats_df["drawdown"],
                fill="tozeroy",
                # marker=dict(color="rgb(255, 0, 0)"),
            ),
            row=2,
            col=1,
        )

        # Update y-axis labels
        fig.update_layout(
            margin=dict(l=20, r=20, t=30, b=30),
        )
        fig.update_xaxes(
            rangebreaks=[
                dict(bounds=["sat", "mon"]),  # hide weekends
            ],
        )

        # Show the figure
        fig.show()

    def describe(self, metrics: list[str] | None = None):
        """
        统计所用指标的统计值，缺省时使用这些指标:
        ('total_returns', 'max_drawdown', 'sharpe_ratio', 'win_rate', 'number_of_trades', 'number_of_take_profit', 'number_of_stop_loss')

        :param metrics: 展示这些指标的统计值
        """

        if not metrics:
            metrics = [
                "total_returns",
                "max_drawdown",
                "sharpe_ratio",
                "win_rate",
                "number_of_trades",
                "number_of_take_profit",
                "number_of_stop_loss",
            ]

        return self.tasks_df[metrics].describe().round(2)

    @cached_property
    def tasks_df(self) -> pd.DataFrame:
        df = pd.read_csv(self.workspace_dir / "tasks.csv")

        # Expand the metrics and configuration column
        df["backtest_conf"] = df["backtest_conf"].apply(eval)
        df["metrics"] = df["metrics"].apply(eval)
        metrics_df = df["metrics"].apply(pd.Series)
        metrics_df.rename(
            columns={"success_rate": "win_rate"}, inplace=True
        )  # For backwards compatibility
        params_df = (
            df["backtest_conf"]
            .map(lambda v: v["strategy"])
            .map(
                lambda v: dict(
                    stop_loss=v["stop_loss"],
                    take_profit=v["take_profit"],
                    max_position_size=v["max_position_size"],
                    max_position_opens=v["max_position_opens"],
                    min_trade_amount=v["min_trade_amount"],
                    **v["custom_params"],
                )
            )
            .apply(pd.Series)
        )
        df.drop(columns=["metrics", "backtest_conf", "dataset_path"], inplace=True)
        df = pd.concat([df, metrics_df, params_df], axis=1)
        return df


class OptimizationResult(BacktestRunsResult):
    def __init__(
        self, parameters: list[Parameter | ParameterGroup], workspace_dir: Path | str
    ) -> None:
        self.parameters = parameters
        super().__init__(workspace_dir)

    def __get_param_names(self, params: list[Parameter | ParameterGroup]):
        return list(
            itertools.chain.from_iterable(
                [param.name] if isinstance(param.name, str) else param.name
                for param in params
            )
        )

    @cache
    def get_total_metrics(self) -> pd.DataFrame:
        param_names = self.__get_param_names(self.parameters)
        core_metrics = {
            "total_returns": "收益率",
            "win_rate": "胜率",
            "max_drawdown": "最大回撤",
            "number_of_trades": "开仓数",
            "sharpe_ratio": "夏普比率",
        }

        return (
            self.tasks_df.groupby(param_names)[list(core_metrics.keys())]
            .agg(["mean", "std"])
            .round(2)
            .sort_values(("total_returns", "mean"), ascending=False)
            .rename(columns=core_metrics)
        )

    def plot_performance_metric(self, metric_name: str):
        plotter = ParameterPerformancePlotter(self.get_total_metrics(), metric_name)
        plotter.plot()


class ParameterPerformancePlotter:
    def __init__(self, metrics_df: pd.DataFrame, score_name: str):
        self.metrics_df = metrics_df.reset_index()
        self.score_name = score_name
        self.param_cols = list(metrics_df.index.names)

    def _plot_2d(self, X, y, colors):
        labels = [
            f'[{self.score_name}={row[(self.score_name, "mean")]}]: '
            + " \n\n; ".join(f"{name}={row[name].iloc[0]}" for name in self.param_cols)
            for _, row in self.metrics_df.iterrows()
        ]
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=X,
                y=y,
                mode="markers",
                marker=dict(
                    color=colors,
                    showscale=True,
                    colorscale="OrRd",
                ),
                text=labels,
            )
        )

        fig.update_layout(
            margin=dict(l=20, r=20, t=30, b=30),
        )
        fig.show()

    def _plot_heatmap(self, X, y):
        fig = go.Figure(
            data=go.Heatmap(x=list(map(str, X[:, 0])), y=list(map(str, X[:, 1])), z=y)
        )
        fig.update_layout(
            xaxis_title=self.param_cols[0],
            yaxis_title=self.param_cols[1],
        )
        fig.show()

    def _plot_tsne_fit(self, X, y):
        t_sne = manifold.TSNE(
            n_components=2,
            perplexity=5,
            init="random",
            n_iter=750,
            random_state=0,
        )
        S_t_sne = t_sne.fit_transform(X)
        _x, _y = S_t_sne.T
        self._plot_2d(_x, _y, y)

    def plot(self):
        n_dim = len(self.param_cols)

        if n_dim <= 1:
            raise ValueError("参数数量必须 >= 2")

        X = self.metrics_df[self.param_cols].values
        y = self.metrics_df[("收益率", "mean")].values

        if n_dim == 2:
            return self._plot_heatmap(X, y)
        else:
            return self._plot_tsne_fit(X, y)
