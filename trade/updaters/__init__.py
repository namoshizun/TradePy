import os
import abc
import time
import tempfile
import pandas as pd
from typing import Any, Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

from trade.utils import chunks


class DataUpdater:

    def run_batch_jobs(self,
                       jobs: list[Any],
                       batch_size: int,
                       fun: Callable,
                       iteration_pause: float=0) -> Generator[Any, None, None]:

        batches = list(chunks(jobs, batch_size))
        tempd = tempfile.TemporaryDirectory()

        kls = self.__class__.__name__
        print(f'''
[{kls}]:
    {len(jobs)} jobs
    batch size = {batch_size}
    number of batches = {len(batches)}
    pause between each iteration = {iteration_pause}
    temporary result folder: {tempd.name}
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