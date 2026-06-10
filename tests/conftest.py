from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest


def build_fake_klines(
    close: np.ndarray,
    *,
    code: str = "TEST",
    start: date = date(2024, 1, 1),
) -> pl.DataFrame:
    n = len(close)
    dates = [start + timedelta(days=i) for i in range(n)]
    close_f = close.astype(np.float32)
    wobble = np.linspace(0.99, 1.01, n, dtype=np.float32)
    return pl.DataFrame(
        {
            "code": [code] * n,
            "date": dates,
            "open": (close_f * wobble * 0.995).astype(np.float32),
            "high": (close_f * 1.02).astype(np.float32),
            "low": (close_f * 0.98).astype(np.float32),
            "close": close_f,
            "vol": np.full(n, 1_000, dtype=np.int32),
            "amount": np.full(n, 100, dtype=np.int32),
            "pct_chg": np.zeros(n, dtype=np.float32),
        }
    )


@pytest.fixture
def fake_klines_short() -> pl.DataFrame:
    close = np.arange(1.0, 11.0, dtype=np.float64)
    return build_fake_klines(close)


@pytest.fixture
def fake_klines_convergence() -> pl.DataFrame:
    close = np.linspace(10.0, 200.0, 300, dtype=np.float64)
    return build_fake_klines(close)
