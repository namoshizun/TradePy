from datetime import date

from tradepy.pipelines.assemble_dataset import (
    AssembleDatasetPipeline,
    StockDayBasicsData,
    StocksIndustryClassData,
)

if __name__ == "__main__":
    since, until = date(2013, 6, 1), date(2026, 6, 1)
    pipe = AssembleDatasetPipeline(
        since,
        until,
        ingredients=[
            StocksIndustryClassData(),
            StockDayBasicsData(
                since,
                until,
                columns=["total_mv", "circ_mv", "pe_ttm", "ps_ttm"],
            ),
        ],
    )
    df = pipe.execute().collect()
    print(df)
