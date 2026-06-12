---
name: implement-new-indicator
description: Guides implementation of new TradePy strategy indicators. Use when adding, modifying, or testing indicators in tradepy/strategy/indicators.py, especially when comparing Polars calculations against TA-Lib.
disable-model-invocation: true
---

# Implement New Indicator

## Scope

Use this skill when the user asks to implement a new strategy indicator in TradePy.

Core rules from the project:

- Implementations go into `tradepy/strategy/indicators.py`
- Use `_fast_ewm` for exponential weighted averages. We deliberately avoid Wilder's Smoothing due to its complexity, and instead use Polars' built-in `ewm_mean` for best performance. As a result, more leading bars need to be discarded during warmup.
- For each new indicator implemented, verify it against TA-Lib. The dev dependency already includes TA-Lib. You need to confirm that the calculated indicator values converge with TA-Lib's results.

## Implementation Workflow

1. Read `tradepy/strategy/indicators.py`, `tradepy/strategy/__init__.py`, and relevant tests before editing.
2. Add the indicator as a typed `@dataclass(frozen=True)` subclass of `SeriesIndicator` and implement `compute(self, value: pl.Expr)`. The base class resolves the input: at the pipeline root, `value` is the adjusted `column` price; otherwise it is the upstream pipeline output.
3. Follow the existing output style:
   - Return `pl.Expr` for single-output indicators.
   - Return `dict[str, pl.Expr]` for multi-output indicators that require `Take(...)`.
   - Set `requires_upstream: ClassVar[bool] = True` for transforms that are meaningless without a piped input (e.g. `Lag`).
4. For exponential weighted averages, use `_fast_ewm(...)` and set a warmup long enough for convergence against TA-Lib. Declare the warmup multiplier as a `WARMUP_FACTOR: ClassVar[int]` on the indicator class.
5. Export the indicator from `tradepy/strategy/__init__.py`.
6. Add focused pytest coverage in `tests/test_indicators.py`.

## TA-Lib Verification

Tests must compare the new Polars indicator against the matching TA-Lib function and prove convergence after warmup.

Use a deterministic price series with enough rows for convergence, then compare only the non-null tail:

```python
import numpy as np
import talib


def test_new_indicator_converges_with_talib() -> None:
    close = np.linspace(10.0, 200.0, 300)
    # Build a Polars DataFrame and compute the project indicator.
    # Compute the TA-Lib reference with the same period/configuration.
    # Drop leading null/NaN values and assert the tail converges.
```

Prefer `pytest.approx(...)` or `numpy.testing.assert_allclose(...)` with tolerances that reflect floating-point convergence rather than exact equality.

## Commands

Use `uv` for verification:

```bash
uv run pytest tests/test_indicators.py
```

If type or lint behavior is relevant to the change, also run the existing project commands for pyright or ruff.
