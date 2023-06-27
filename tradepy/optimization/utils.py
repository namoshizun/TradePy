import pandas as pd
from pathlib import Path


def read_task_results(file_path: str | Path):
    if isinstance(file_path, str):
        file_path = Path(file_path)

    df = pd.read_csv(file_path, index_col="id", dtype=str)
    df["backtest_conf"] = df["backtest_conf"].apply(eval)
    df["metrics"] = df["metrics"].apply(eval)

    metrics_df = df["metrics"].apply(pd.Series)
    params_df = (
        df["backtest_conf"]
        .map(lambda v: v["strategy"])
        .map(
            lambda v: dict(
                stop_loss=v["stop_loss"],
                stop_loss_slip=v["stop_loss_slip"],
                take_profit=v["take_profit"],
                take_profit_slip=v["take_profit_slip"],
                max_position_size=v["max_position_size"],
                max_position_opens=v["max_position_opens"],
                min_trade_amount=v["min_trade_amount"],
                **v["custom_params"]
            )
        )
        .apply(pd.Series)
    )
    df.drop(columns=["metrics", "backtest_conf"], inplace=True)
    df = pd.concat([df, metrics_df, params_df], axis=1)
    return df
