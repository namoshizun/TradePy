import os
import abc
import time
import tempfile
import talib
import pandas as pd
from typing import Any, Callable, Generator, Type
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from tqdm import tqdm

import tradepy
from tradepy import LOG
from tradepy.depot.base import GenericBarsDepot, GenericListingDepot
from tradepy.utils import get_latest_trade_date
from tradepy.utils import chunks


class DataCollector:
    def precompute_indicators(self, df):
        # Moving averages
        df["ma5"] = talib.SMA(df["close"], 5)
        df["ma20"] = talib.SMA(df["close"], 20)

        df["ema5"] = talib.EMA(df["close"], 5)
        df["ema20"] = talib.EMA(df["close"], 20)

        # RSI
        df["rsi6"] = talib.RSI(df["close"], 6)

        # MACD
        _, _, macdhist = talib.MACD(df["close"])
        df["macd"] = macdhist * 2

        df.dropna(inplace=True)
        return df.round(2)

    def run_batch_jobs(
        self,
        jobs: list[Any],
        batch_size: int,
        fun: Callable,
        iteration_pause: float = 0,
    ) -> Generator[Any, None, None]:
        batches = list(chunks(jobs, batch_size))
        tempd = tempfile.TemporaryDirectory()

        kls = self.__class__.__name__
        tradepy.LOG.info(
            f"""
[{kls}]:
    {len(jobs)} 下载任务
    批大小 = {batch_size}
    批数量 = {len(batches)}
    每批间隔(s) = {iteration_pause}
    临时下载目录: {tempd.name}
        """
        )

        with ThreadPoolExecutor() as executor:
            for batch in tqdm(batches):
                results_iter = map_routines(
                    executor, fun, [((), args) for args in batch]
                )

                for idx, result in enumerate(results_iter):
                    input_args = batch[idx]

                    if isinstance(result, pd.DataFrame):
                        temp_path = os.path.join(tempd.name, f"{idx}.csv")
                        result.to_csv(temp_path, index=False)

                    yield input_args, result

                time.sleep(iteration_pause)

        tempd.cleanup()

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError()


class DayBarsCollector(DataCollector):
    bars_depot_class: Type[GenericBarsDepot]
    listing_depot_class: Type[GenericListingDepot]

    def __init__(self, since_date: str | date | None = None) -> None:
        if not since_date:
            since_date = "2010-01-01"

        if isinstance(since_date, str):
            self.since_date: date = date.fromisoformat(since_date)
        else:
            self.since_date: date = since_date

        self.repo = self.bars_depot_class()

    def jobs_generator(self):
        LOG.info(f"检查本地数据是否需要更新")
        repo_iter = self.repo.find(always_load=True)
        curr_codes = list()
        last_trade_date = get_latest_trade_date()

        for code, df in repo_iter:
            assert isinstance(df, pd.DataFrame)
            curr_codes.append(code)

            try:
                if not df.empty:
                    latest_date = df["timestamp"].max()
                else:
                    latest_date = "2000-01-01"

                latest_date = date.fromisoformat(latest_date)
                if latest_date < last_trade_date:
                    start_date = latest_date + timedelta(days=1)
                    yield {"code": code, "start_date": start_date}
            except Exception as exc:
                LOG.info(
                    f"!!!!!!!!! failed to genereate update job for {code} !!!!!!!!!"
                )
                raise exc

        listing_df = self.listing_depot_class.load()
        new_listings = set(listing_df.index) - set(curr_codes)
        if new_listings:
            LOG.info(f"添加新标的, 起始日期 {self.since_date}")
            for code in new_listings:
                yield {
                    "code": code,
                    "start_date": self.since_date.isoformat(),
                }


def map_routines(executor, routine, arguments):
    """
    arguments: list of args and kwargs, e.g.,

    arguments = [
        (('arg1', 'arg2'), {'kw':1, 'kw':2}),
        (('arg1', 'arg2'))
    ]
    """

    futures = []

    for arg in arguments:
        kwargs = dict()
        if len(arg) == 2:
            args, kwargs = arg
        else:
            args = arg[0]

        futures.append(executor.submit(routine, *args, **kwargs))

    def result_iterator():
        try:
            # reverse to keep finishing order
            futures.reverse()
            while futures:
                yield futures.pop().result()
        finally:
            for future in futures:
                future.cancel()

    return result_iterator()
