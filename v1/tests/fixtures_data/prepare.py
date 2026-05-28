from pathlib import Path
from tradepy.depot.stocks import StocksDailyBarsDepot, StockListingDepot
from tradepy.depot.misc import AdjustFactorDepot

HERE = Path(__file__).parent

if __name__ == "__main__":
    # We select the test stocks that are deemed never to be out of the market
    selected_stocks = [
        "601398",  # ICBC
        "601939",  # CCB
        "601288",  # ABC
        "601988",  # BOC
    ]

    # Prepare day bars
    df = StocksDailyBarsDepot.load()
    df.loc[selected_stocks].to_pickle(HERE / "daily-k.pkl")

    # Prepare stock listing
    df = StockListingDepot.load()
    df.loc[selected_stocks].to_pickle(HERE / "listing.pkl")

    # Prepare adjust factor
    adjust_factors = AdjustFactorDepot().load()
    adjust_factors.factors_df.loc[selected_stocks].to_pickle(
        HERE / "adjust-factors.pkl"
    )
