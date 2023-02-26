import akshare as ak
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
        self.repo = TicksDepot("daily.stock")
        self.batch_size = batch_size

    def _jobs_iterator(self):
        # Update existing stocks
        repo_iter = self.repo.traverse(always_load=True)
        ts_codes = list()

        for ts_code, df in repo_iter:
            assert isinstance(df, pd.DataFrame)
            ts_codes.append(ts_code)

            try:
                latest_date = '2000-01-01'
                if not df.empty:
                    latest_date = df['timestamp'].max()

                latest_date = date.fromisoformat(latest_date)

                if latest_date < self.since_date:
                    start_date = latest_date + timedelta(days=1)
                    yield {
                        "ts_code": ts_code,
                        "start_date": start_date.strftime('%Y%m%d')
                    }
            except Exception as exc:
                print(f'!!!!!!!!! failed to exaim {ts_code} !!!!!!!!!')
                raise exc

        # Add new listing
        new_listings = set(trade.listing.ts_codes) - set(ts_codes)
        for ts_code in new_listings:
            yield {
                "ts_code": ts_code,
                "start_date": date.fromisoformat('2000-01-01').strftime('%Y%m%d')
            }

    def run(self):
        assert trade.pro_api
        jobs = list(self._jobs_iterator())
        batches = list(chunks(jobs, self.batch_size))

        print(f'[DailyPricesUpdater]: {len(jobs)} jobs, batch size = {self.batch_size}, number of batches = {len(batches)}')
        with ThreadPoolExecutor() as executor:
            for batch in tqdm(batches):
                results_iter = map_routines(executor, trade.pro_api.get_daily, [
                    ((), args)
                    for args in batch
                ])

                for i, df in enumerate(results_iter):
                    code = batch[i]["ts_code"]
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
