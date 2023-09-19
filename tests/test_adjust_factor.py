import pytest
import pandas as pd
import numpy as np
from tradepy.core.adjust_factors import AdjustFactors, _assign_factor_value_to_day

# Sample data for testing
sample_factors_data = {
    "code": ["AAPL", "AAPL", "GOOGL", "GOOGL", "MSFT", "MSFT"],
    "timestamp": [1, 2, 1, 2, 1, 2],
    "hfq_factor": [2.0, 2.2, 1.5, 1.7, 3.0, 3.3],
}

sample_bars_data = {
    "code": ["AAPL", "AAPL", "AAPL", "GOOGL", "GOOGL", "GOOGL"],
    "timestamp": [1, 2, 3, 1, 2, 3],
    "open": [100.0, 110.0, 105.0, 2000.0, 2050.0, 2030.0],
    "close": [105.0, 115.0, 110.0, 2050.0, 2100.0, 2080.0],
    "high": [108.0, 118.0, 113.0, 2070.0, 2120.0, 2100.0],
    "low": [98.0, 108.0, 103.0, 1980.0, 2030.0, 2010.0],
}


@pytest.fixture
def adjust_factors_instance():
    factors_df = pd.DataFrame(sample_factors_data)
    return AdjustFactors(factors_df)


# def test_adjust_factors_latest_factors(adjust_factors_instance):
#     latest_factors = adjust_factors_instance.latest_factors
#     assert len(latest_factors) == 4  # There are 4 unique codes in the sample data


# def test_adjust_factors_to_real_price(adjust_factors_instance):
#     adjusted_price = adjust_factors_instance.to_real_price("AAPL", 110.0)
#     assert adjusted_price == 50.0  # Adjusted price for AAPL should be 110 / 2.2 = 50.0


# def test_assign_factor_value_to_day():
#     timestamps = [1, 2, 3]
#     fac_ts = [1, 2, 3, 4, 5]
#     fac_vals = [2.0, 2.2, 2.5, 1.5, 1.7]

#     factors = _assign_factor_value_to_day(fac_ts, fac_vals, timestamps)
#     assert factors == [2.0, 2.2, 2.5]  # Expected adjusted factors for timestamps


# def test_adjust_factors_backward_adjust_history_prices(adjust_factors_instance):
#     code = "AAPL"
#     bars_df = pd.DataFrame(sample_bars_data)
#     adjusted_bars = adjust_factors_instance.backward_adjust_history_prices(
#         code, bars_df
#     )

#     assert len(adjusted_bars) == 3  # 3 rows for code "AAPL"
#     assert all(adjusted_bars["close"] == [50.0, 55.0, 52.5])  # Adjusted closing prices


# def test_adjust_factors_backward_adjust_stocks_latest_prices(adjust_factors_instance):
#     bars_df = pd.DataFrame(sample_bars_data)
#     adjusted_bars = adjust_factors_instance.backward_adjust_stocks_latest_prices(
#         bars_df
#     )

#     assert len(adjusted_bars) == 6  # 6 rows in the sample data
#     assert all(
#         adjusted_bars["close"] == [210.0, 231.0, 214.5, 3500.0, 3610.0, 3564.0]
#     )  # Adjusted closing prices
