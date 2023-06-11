import argparse
from pathlib import Path
from tradepy.utils import get_latest_trade_date
from tradepy.depot.stocks import StocksDailyBarsDepot
from tradepy.trade_cal import trade_cal
from tqdm import tqdm


def get_stocks_dir():
    return Path(f"./database/{StocksDailyBarsDepot.folder_name}")


def main(window: int):
    p = get_stocks_dir()
    new_p = Path(str(p) + "-bak")
    if new_p.exists():
        n_bak = list(new_p.parent.glob(new_p.name))
        if len(n_bak) >= 1:
            new_p = Path(str(new_p) + f"-{len(n_bak) + 1}")

    until_date: str = str(get_latest_trade_date())
    end_idx = trade_cal.index(until_date) + 1
    start_idx = end_idx + window
    since_date = trade_cal[start_idx]
    df = StocksDailyBarsDepot.load(since_date=since_date)

    p.rename(new_p)
    p = get_stocks_dir()
    p.mkdir()
    for code, sub_df in tqdm(df.groupby("code")):
        sub_df.to_csv(p / f"{code}.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-window", type=int, default=100)
    args = parser.parse_args()
    main(args.trade_window)
