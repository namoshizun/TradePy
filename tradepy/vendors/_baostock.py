import inspect
from datetime import date

import baostock as bs
import polars as pl

from tradepy.core.types import (
    StockPriceAdjustFactorsDataFrame,
    StockPriceAdjustFactorsModel,
)


def get_default_id_and_password():
    sig = inspect.signature(bs.login)
    params = sig.parameters
    return params["user_id"].default, params["password"].default


DefaultUserId, DefaultPassword = get_default_id_and_password()


class BaostockClient:
    def __init__(
        self, user_id: str = DefaultUserId, password: str = DefaultPassword
    ):
        res = bs.login(user_id, password)
        if res.error_code != "0":
            raise Exception(res.error_code, res.error_msg)

    def get_adjust_factors(
        self, code: str, since: date
    ) -> StockPriceAdjustFactorsDataFrame:
        if all(str.isalpha(char) for char in code[-2:]):
            code = code[-2:].lower() + "." + code[:-3]

        result = bs.query_adjust_factor(
            code=code, start_date=since.strftime("%Y-%m-%d")
        )
        return (  # pyright: ignore[reportReturnType]
            pl.DataFrame(result.data, schema=result.fields, orient="row")
            .rename(
                {
                    "dividOperateDate": "date",
                    "foreAdjustFactor": "forward",
                    "backAdjustFactor": "backward",
                }
            )
            .drop("adjustFactor")
            .cast(StockPriceAdjustFactorsModel.schema())
        )

    def __del__(self):
        bs.logout()
