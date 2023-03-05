import trade
from trade.collectors import DataCollector
from trade.warehouse import TicksDepot


# TODO: avoid duplication


class EastMoneyIndustryIndexCollector(DataCollector):

    def __init__(self, batch_size: int = 20):
        self.batch_size = batch_size

    def _jobs_generator(self, listing_df):
        for name in listing_df["name"]:
            yield {
                "name": name,
            }

    def run(self):
        print("Retrieve the EM industry listing data")
        listing_df = trade.ak_api.get_industry_listing()

        print("Retrieve individual industry's ticks data")
        results_gen = self.run_batch_jobs(
            list(self._jobs_generator(listing_df)),
            self.batch_size,
            fun=trade.ak_api.get_industry_index_ticks,
            iteration_pause=3,
        )

        print("Exporting")
        repo = TicksDepot("daily.industry")
        for args, ticks_df in results_gen:
            name = args["name"]  # noqa
            code = listing_df.query('name == @name').iloc[0]["code"]
            ticks_df["code"] = code

            repo.append(
                self.precompute_indicators(ticks_df.copy()),
                f'{code}.csv'
            )


class StockIndexCollector(DataCollector):

    code_to_index_name = {
        "sh000001": "SSE",
        "sz399001": "SZSE",
        "sz399006": "ChiNext",
        "sh000688": "STAR",
        "sh000300": "CSI-300",
        "sh000905": "CSI-500",
        "sh000852": "CSI-1000",
        "sh000016": "SSE-50",
    }

    def run(self):
        repo = TicksDepot("daily.index")
        for code, name in self.code_to_index_name.items():
            df = trade.ak_api.get_stock_index_ticks(code)
            repo.append(
                self.precompute_indicators(df.copy()),
                f'{name}.csv'
            )
