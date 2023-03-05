import pandas as pd

import tradepy
from tradepy.collectors import DataCollector
from tradepy.warehouse import ListingDepot


class TushareStocksListingCollector(DataCollector):

    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size

    def _jobs_generator(self, listing_df):
        for code in listing_df["code"]:
            yield {
                "code": code
            }

    def run(self):
        assert tradepy.pro_api
        print("Retrieve the tushare listing data")
        listing_df = tradepy.pro_api.get_company_fundamentals()

        print("Retrieve individual stock's listing data from EastMoney")
        # NOTE: We adopt EM's industry tags so that it is easier to look up stock's related industry index data
        results_gen = self.run_batch_jobs(
            list(self._jobs_generator(listing_df)),
            self.batch_size,
            fun=tradepy.ak_api.get_stock_info,
            iteration_pause=3,
        )

        em_listing_df = pd.DataFrame([row for _, row in results_gen]).set_index("code")

        listing_df.drop("industry", axis=1, inplace=True)
        listing_df = listing_df.join(
            em_listing_df[["industry", "listdate"]],
            on="code",
        )

        listing_df.to_csv(ListingDepot.path, index=False)
        print(f'Exported to {ListingDepot.path}')
