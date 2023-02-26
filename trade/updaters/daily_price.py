import pandas as pd
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor

import trade
from tqdm import tqdm
from trade.utils import get_latest_trade_date, chunks
from trade.warehouse import TicksDepot


class StockPricesUpdater:

    def __init__(self, since_date: str | date | None = None, batch_size: int = 50) -> None:
        if not since_date:
            since_date = get_latest_trade_date()
        elif isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        self.since_date: date = since_date
        self.repo = TicksDepot("daily.stocks")
        self.batch_size = batch_size

    def _jobs_iterator(self):
        # Update existing listing
        repo_iter = self.repo.traverse(always_load=True)
        curr_codes = list()

        for code, df in repo_iter:
            # Legacy
            if len(parts := code.split('.')) == 2:
                code = parts[0]

            assert isinstance(df, pd.DataFrame)
            curr_codes.append(code)

            try:
                latest_date = '2000-01-01'
                if not df.empty:
                    latest_date = df['timestamp'].max()

                latest_date = date.fromisoformat(latest_date)

                if latest_date < self.since_date:
                    start_date = latest_date + timedelta(days=1)
                    yield {
                        "code": code,
                        "start_date": start_date
                    }
            except Exception as exc:
                print(f'!!!!!!!!! failed to genereate update job for {code} !!!!!!!!!')
                raise exc

        # Add new listing
        new_listings = set(trade.listing.codes) - set(curr_codes)
        for code in new_listings:
            yield {
                "code": code,
                "start_date": date.fromisoformat('2000-01-01')
            }

    def run(self):
        assert trade.pro_api
        jobs = list(self._jobs_iterator())
        batches = list(chunks(jobs, self.batch_size))

        print(f'[DailyPricesUpdater]: {len(jobs)} jobs, batch size = {self.batch_size}, number of batches = {len(batches)}')
        with ThreadPoolExecutor() as executor:
            for batch in tqdm(batches):
                results_iter = map_routines(executor, trade.ak_api.get_daily, [
                    ((), args)
                    for args in batch
                ])

                for i, df in enumerate(results_iter):
                    code = batch[i]["code"]
                    self.repo.append(df, f'{code}.csv')


class MarketIndexUpdater:
    ...


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
