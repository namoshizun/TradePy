import pandas as pd
from pathlib import Path
from typing import Literal
from loguru import logger


HERE = Path(__file__).parent


def load_dataset(
    name: Literal["adjust-factors", "daily-k", "listing"], database_dir: Path
):
    df = pd.read_pickle(HERE / f"{name}.pkl")
    if name == "daily-k":
        out_dir = database_dir / "daily-stocks"
        out_dir.mkdir(parents=True, exist_ok=True)
        for code, sub_df in df.groupby(level="code"):
            if not (file_loc := out_dir / f"{code}.csv").exists():
                logger.info(f"Saving day-k {code} to {file_loc}")
                sub_df.to_csv(file_loc, index=None)
        return

    _name = name.replace("-", "_")
    if not (file_loc := database_dir / f"{_name}.csv").exists():
        logger.info(f"Saving {_name} to {file_loc}")
        df.to_csv(file_loc)
