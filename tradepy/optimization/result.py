import pandas as pd
import plotly.graph_objects as go
from functools import cached_property, cache
from pathlib import Path
from sklearn import manifold

from tradepy.optimization.parameter import Parameter, ParameterGroup


class OptimizationResult:
    def __init__(
        self, parameters: list[Parameter | ParameterGroup], workspace_dir: Path
    ) -> None:
        self.parameters = parameters
        self.workspace_dir = workspace_dir

    @cached_property
    def tasks_df(self) -> pd.DataFrame:
        df = pd.read_csv(self.workspace_dir / "tasks.csv")

        # Expand the metrics and configuration column
        df["backtest_conf"] = df["backtest_conf"].apply(eval)
        df["metrics"] = df["metrics"].apply(eval)
        metrics_df = df["metrics"].apply(pd.Series)
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

    @cache
    def get_total_metrics(self) -> pd.DataFrame:
        as_iterable = lambda x: x if isinstance(x, (list, tuple)) else [x]
        param_names = [name for p in self.parameters for name in as_iterable(p.name)]
        core_metrics = {
            "total_returns": "收益率",
            "success_rate": "胜率",
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
