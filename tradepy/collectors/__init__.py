import os
import abc
import time
import tempfile
import talib
import pandas as pd
from typing import Any, Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

from tradepy.utils import chunks


class DataCollector:

    def precompute_indicators(self, df, nmacd_period=120):
        # Moving averages
        df["ma5"] = talib.SMA(df["close"], 5)
        df["ma20"] = talib.SMA(df["close"], 20)

        df["ema5"] = talib.EMA(df["close"], 5)
        df["ema20"] = talib.EMA(df["close"], 20)

        # RSI
        df["rsi6"] = talib.RSI(df["close"], 6)

        # MACD
        _, _, macdhist = talib.MACD(df["close"])
        df["macd"] = (macdhist * 2)

        # PPO
        df["ppo"] = talib.PPO(df["close"])

        df.dropna(inplace=True)
        return df.round(2)

    def run_batch_jobs(self,
                       jobs: list[Any],
                       batch_size: int,
                       fun: Callable,
                       iteration_pause: float = 0) -> Generator[Any, None, None]:

        batches = list(chunks(jobs, batch_size))
        tempd = tempfile.TemporaryDirectory()

        kls = self.__class__.__name__
        print(f'''
[{kls}]:
    {len(jobs)} 下载任务
    批大小 = {batch_size}
    批数量 = {len(batches)}
    每批间隔(s) = {iteration_pause}
    临时下载目录: {tempd.name}
        ''')

        with ThreadPoolExecutor() as executor:
            for batch in tqdm(batches):
                results_iter = map_routines(executor, fun, [
                    ((), args)
                    for args in batch
                ])

                for idx, result in enumerate(results_iter):
                    input_args = batch[idx]

                    if isinstance(result, pd.DataFrame):
                        temp_path = os.path.join(tempd.name, f'{idx}.csv')
                        result.to_csv(temp_path, index=False)

                    yield input_args, result

                time.sleep(iteration_pause)

        tempd.cleanup()

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError()


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
