import io

import polars as pl
import requests as rq

from tradepy.core.types import SWStockIndustryDataFrame, SWStockIndustryModel


def fetch_stock_industry_classification_history() -> SWStockIndustryDataFrame:
    url = "https://www.swsresearch.com/swindex/pdf/SwClass2021/StockClassifyUse_stock.xls"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = rq.get(url, headers=headers, verify=False)
    response.raise_for_status()

    temp_file = io.BytesIO(response.content)
    return (  # pyright: ignore[reportReturnType]
        pl.read_excel(temp_file)
        .rename(
            {
                "股票代码": "code",
                "计入日期": "since",
                "行业代码": "industry_code",
            }
        )
        .drop("更新日期")
        .cast(SWStockIndustryModel.schema())
    )
