import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, date
from functools import cached_property
from typing import Literal, get_args
from tqdm import tqdm
from loguru import logger

from xtquant.xtdata import download_history_data2, get_market_data
from tradepy.utils import chunks
from tradepy.depot.stocks import StockListingDepot
from tradepy.collectors.stock_listing import StocksListingCollector
from tradepy.conversion import convert_code_to_exchange


MinutePeriod = Literal["1m", "5m"]


class QMTDataFetcher:
    def __init__(self, qmt_install_dir: Path, out_dir: Path):
        self.data_dir = qmt_install_dir / "userdata_mini" / "datadir"
        assert self.data_dir.exists()

        self.individual_stock_out_dir = out_dir / "per_stock"
        self.monthly_stock_out_dir = out_dir / "per_month"
        self.individual_stock_out_dir.mkdir(exist_ok=True)
        self.monthly_stock_out_dir.mkdir(exist_ok=True)

    @cached_property
    def listing_df(self):
        return StockListingDepot.load()

    def walk_fetched_codes(self):
        for exchange in ["SZ", "SH"]:
            for path in (self.data_dir / exchange).rglob("**/*.DAT"):
                yield path.name.split(".")[0]

    def walk_month_data(self, month_dir: Path, load=True):
        for file in month_dir.glob("*.pkl"):
            code = file.name.split(".")[0]
            if not load:
                yield code
            else:
                df = pd.read_pickle(file)
                df["code"] = code
                yield df

    def fetch(self, start_date: date, until_date: date, period: MinutePeriod):
        fetched_codes = set(self.walk_fetched_codes())
        pending_codes = list(set(self.listing_df.index.values) - set(fetched_codes))

        if not pending_codes:
            logger.info(f"已下载所有个股的{period}K线")
            return

        logger.info(
            f"准备下载{len(pending_codes)}支个股的 {period} K线, 起始日期 = {start_date}, 结束日期 = {until_date}"
        )
        for chunk in tqdm(chunks(pending_codes, 250)):
            stock_list = [
                f"{code}.{exchange}"
                for code in chunk
                if (exchange := convert_code_to_exchange(code)) in ("SZ", "SH")
            ]

            download_history_data2(
                stock_list,
                period,
                start_time=start_date.strftime("%Y%m%d"),
                end_time=until_date.strftime("%Y%m%d"),
            )

    def export_individual_stock_data(
        self, start_date: date, until_date: date, period: MinutePeriod
    ):
        exported_codes = set(
            code
            for folder in self.individual_stock_out_dir.glob("*")
            for code in self.walk_month_data(folder, load=False)
        )
        pending_codes = [
            code for code in self.walk_fetched_codes() if code not in exported_codes
        ]
        logger.info(f"已导出{len(exported_codes)}支个股的 {period} K线数据")
        logger.info(f"开始导出剩余{len(pending_codes)}个股的 {period} K线数据")
        for code in tqdm(pending_codes):
            _code = f"{code}.{convert_code_to_exchange(code)}"
            res = get_market_data(
                field_list=["time", "open", "high", "low", "close", "volume"],
                stock_list=[_code],
                period=period,
                start_time=start_date.strftime("%Y%m%d"),
                end_time=until_date.strftime("%Y%m%d"),
            )

            df = pd.DataFrame(
                {
                    "open": res["open"].T[_code],
                    "high": res["high"].T[_code],
                    "low": res["low"].T[_code],
                    "close": res["close"].T[_code],
                    "vol": res["volume"].T[_code],
                }
            )

            if df.empty:
                print(f"找不到{code}的本地数据!")
                continue

            df["date"] = df.index.map(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:8]}")
            df["time"] = df.index.map(lambda x: x[8:12])
            df["open"] = df["open"].astype(np.float32)
            df["close"] = df["close"].astype(np.float32)
            df["high"] = df["high"].astype(np.float32)
            df["low"] = df["low"].astype(np.float32)
            df["vol"] = df["vol"].astype(np.float32)
            df["month"] = df["date"].map(lambda x: x[:7])
            df.reset_index(drop=True, inplace=True)

            for month, sub_df in df.groupby("month"):
                out_dir = self.individual_stock_out_dir / str(month)
                out_dir.mkdir(exist_ok=True)
                sub_df.drop(columns=["month"], inplace=True)
                sub_df.to_pickle(out_dir / f"{code}.pkl")

    def export_monthly_stock_data(self):
        logger.info("开始导出每月的个股数据")

        for month_dir in tqdm(list(self.individual_stock_out_dir.glob("*"))):
            df = pd.concat(self.walk_month_data(month_dir, load=True))
            df["code"] = pd.Categorical(df["code"])
            df["time"] = pd.Categorical(df["time"])
            df["date"] = pd.Categorical(df["date"])
            df.reset_index(drop=True, inplace=True)
            df.set_index(["date", "code"], inplace=True)
            df.sort_index(inplace=True)
            df.to_pickle(self.monthly_stock_out_dir / f"{month_dir.name}.pkl")


def ensure_stock_listing_exists():
    try:
        StockListingDepot.load()
    except FileNotFoundError:
        logger.warning(f"找不到股票列表文件, 是否开始下载最新的股票列表?")
        yes = input("(y/n): ").lower() == "y"
        if yes:
            StocksListingCollector().run(batch_size=25)


def main(
    qmt_path: Path,
    out_dir: Path,
    start_date: date,
    until_date: date,
    period: MinutePeriod,
):
    ensure_stock_listing_exists()
    fetcher = QMTDataFetcher(qmt_path, out_dir)
    fetcher.fetch(start_date, until_date, period)
    fetcher.export_individual_stock_data(start_date, until_date, period)
    fetcher.export_monthly_stock_data()


if __name__ == "__main__":

    def iso_date(string):
        try:
            date_obj = datetime.strptime(string, "%Y-%m-%d")
            return date_obj.date()
        except ValueError:
            raise argparse.ArgumentTypeError("错误的日期格式!")

    parser = argparse.ArgumentParser(
        description="步骤如下: \n\n 1.下载QMT的分钟级K线数据;\n 2. 导出每支个股的数据为pickle文件;\n 3. 将同月份的个股数据合并, 导出为pickle文件.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--qmt",
        type=Path,
        required=True,
        help="QMT客户端安装目录, e.g., 'E:\国金证券QMT交易端'",
    )
    parser.add_argument(
        "--out_dir",
        type=Path,
        required=True,
        help="Pickle文件的输出文件夹路径, e.g., 'E:\out'",
    )
    parser.add_argument(
        "--start_date",
        type=iso_date,
        help="开始日期, e.g., '2020-01-01'",
    )
    parser.add_argument(
        "--until_date",
        default=datetime.now().date(),
        type=iso_date,
        help="结束日期, e.g., '2020-01-01'",
    )
    parser.add_argument(
        "--period",
        default="1m",
        type=str,
        choices=get_args(MinutePeriod),
        help="分钟级K线周期",
    )
    args = parser.parse_args()
    if not args.qmt.exists():
        raise ValueError(f"{args.qmt}路径不存在")
    args.out_dir.mkdir(exist_ok=True)

    main(args.qmt, args.out_dir, args.start_date, args.until_date, args.period)
