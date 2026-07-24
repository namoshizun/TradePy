import sys
from datetime import date

from loguru import logger

from tradepy.pipelines.assemble_dataset import (
    AssembleDatasetPipeline,
    StockDayBasicsData,
    StockFinancialIndicatorsData,
    StocksIndustryClassData,
)
from tradepy.pipelines.update_database import UpdateDatabasePipeline


def assemble_data(since: date, until: date):
    pipe = AssembleDatasetPipeline(
        since,
        until,
        ingredients=[
            StocksIndustryClassData(),
            StockDayBasicsData(
                since,
                until,
                columns=["total_mv", "circ_mv", "pe_ttm", "ps_ttm", "pb"],
            ),
            StockFinancialIndicatorsData(columns=["roic", "roa", "fcff_ps"]),
        ],
    )
    return pipe.execute().collect()


def update_data(since: date, until: date):
    pipe = UpdateDatabasePipeline(since, until)
    return pipe.execute()


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stdout, level="INFO")
    since, until = date(2013, 1, 1), date(2026, 7, 15)
    update_data(since, until)
