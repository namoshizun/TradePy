from trade import pro_api


class StocksPoolUpdater:

    def run(self):
        assert pro_api
        listing_df = pro_api.get_company_fundamentals()
        listing_df.to_csv('./datasets/listing.csv', index=False)
