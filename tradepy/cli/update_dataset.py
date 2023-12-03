import argparse
import pandas as pd
from pathlib import Path
from tqdm import tqdm

import tradepy


def fetch_company_name_changes() -> pd.DataFrame:
    listing_df = tradepy.ak_api.get_a_stocks_list()[:3]
    names_df = pd.concat(
        tradepy.tushare.get_name_change_history(code) for code in tqdm(listing_df.index)
    )
    names_df = names_df.reset_index().set_index("code")

    # Drop duplicate records
    dfs = []
    for _, group in names_df.groupby(names_df.index):
        n_names = group["company"].nunique()
        if n_names == 1:
            group = group[:1]
        dfs.append(group)

    return pd.concat(dfs)


def main(dataset_dir: Path):
    dataset_dir.mkdir(exist_ok=True)
    name_changes_df = fetch_company_name_changes()
    name_changes_df.to_csv(dataset_dir / "company_name_changes.csv")


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("dataset_dir", type=Path)
    args = argparser.parse_args()
    main(args.dataset_dir)
