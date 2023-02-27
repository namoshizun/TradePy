from trade import pro_api
from trade.updaters import DataUpdater
from trade.warehouse import ListingDepot


class StocksListingUpdater(DataUpdater):

    def run(self):
        assert pro_api
        listing_df = pro_api.get_company_fundamentals()
        listing_df.to_csv(ListingDepot.path, index=False)
