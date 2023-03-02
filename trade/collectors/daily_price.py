import pandas as pd
from datetime import date, timedelta

import trade
from trade.utils import get_latest_trade_date
from trade.warehouse import TicksDepot
from trade.collectors import DataCollector


class StockPricesCollector(DataCollector):

    def __init__(self, since_date: str | date | None = None, batch_size: int = 50) -> None:
        if not since_date:
            since_date = get_latest_trade_date()
        elif isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        self.since_date: date = since_date
        self.repo = TicksDepot("daily.stocks")
        self.batch_size = batch_size

    def _jobs_generator(self):
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
        jobs = list(self._jobs_generator())
        results_gen = self.run_batch_jobs(jobs, self.batch_size, fun=trade.ak_api.get_daily)

        for args, ticks_df in results_gen:
            code = args["code"]
            self.repo.append(ticks_df, f'{code}.csv')
