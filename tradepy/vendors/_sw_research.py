import io

import polars as pl
import requests as rq
from tenacity import retry, stop_after_attempt, wait_exponential

from tradepy.core.types import SWStockIndustryDataFrame, SWStockIndustryModel
from tradepy.utils import convert_code_to_exchange


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=3, max=10),
)
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
                "行业代码": "industry_l3",
            }
        )
        .drop("更新日期")
        .with_columns(
            pl.col("code")
            .str.zfill(6)
            .map_elements(
                lambda c: f"{c}.{convert_code_to_exchange(c)}",
                return_dtype=pl.Utf8,
            )
        )
        .cast(SWStockIndustryModel.schema())
    )
