---
name: implement-new-indicator
description: Guides implementation of new TradePy strategy indicators. Use when adding, modifying, or testing indicators in tradepy/strategy/indicators.py — series (time-series) or cross-sectional — especially when comparing Polars calculations against TA-Lib.
disable-model-invocation: true
---

# Implement New Indicator

## Scope

Use this skill when the user asks to implement a new strategy indicator in TradePy.

Core rules:

- Implementations go into `tradepy/strategy/indicators.py`
- Choose the correct base class (see below)
- Use `_fast_ewm` for exponential weighted averages. We deliberately avoid Wilder's Smoothing due to its complexity, and instead use Polars' built-in `ewm_mean` for best performance. As a result, more leading bars need to be discarded during warmup.
- Series indicators with a TA-Lib counterpart must be verified against TA-Lib (dev dependency already includes it)

## Base classes

| Base                    | Window                 | Use for                                   |
| ----------------------- | ---------------------- | ----------------------------------------- |
| `SeriesIndicator`       | `.over("code")`        | Time-series transforms (SMA, RSI, Lag, …) |
| `CrossSectionIndicator` | `.over("date", *over)` | Cross-sectional transforms (Rank, …)      |

`compute_indicators` applies each indicator's `partition_by` automatically. Do not call `.over(...)` inside `compute`.

Indicators are computation specifications, not scalar values. Bind them to strategy
`buy`/`sell` parameters with `Annotated`. Multiple metadata steps are evaluated
left-to-right:

```python
from typing import Annotated

def buy(
    self,
    sma5: Annotated[float, SMA(5)],
    sma5_ref1: Annotated[float, SMA(5), Lag(1)],
    macd_hist: Annotated[float, MACD(), Take("hist")],
    month: Annotated[int, DatePart(part="month")],
) -> float | None:
    ...
```

The leading type is the row value used inside the method body; the indicator
metadata is what `StrategyBase.collect_indicator_expressions` materializes.
Aliases compose via Annotated flattening: `MA10 = Annotated[float, SMA(10)]`
then `MA10_REF1 = Annotated[MA10, Lag(1)]`.

### Series indicators

Compose by listing steps in `Annotated` (or `resolve_indicators(SMA(20), Lag(1))`
in unit tests).

### Cross-sectional indicators

- Inherit `CrossSectionIndicator` and implement `compute(self, value: pl.Expr)`
- Optional `over=` adds grouping on top of `date` (e.g. `Rank(column="pe", over="industry_code")`)
- **Standalone only** — multi-step chains that include a cross-section raise `TypeError`

## Implementation Workflow

1. Read `tradepy/strategy/indicators.py`, `tradepy/strategy/__init__.py`, and relevant tests before editing.
2. Subclass `SeriesIndicator` or `CrossSectionIndicator` as a typed `@dataclass(frozen=True)` and implement `compute(self, value: pl.Expr)`.
   - At the chain root (or for standalone cross-section), `value` is `pl.col(self.column)`.
   - For later steps in a chain, `value` is the upstream output.
3. Follow the existing output style:
   - Return `pl.Expr` for single-output indicators.
   - Return `dict[str, pl.Expr]` for multi-output indicators that require `Take(...)`.
   - Set `requires_upstream: ClassVar[bool] = True` for series transforms that are meaningless without a prior step (e.g. `Lag`).
   - Set a concrete `dtype=` only when this step defines the output type (e.g. `RSI`, `Rank`). Leave `dtype=None` for passthrough steps so chains inherit upstream (`Take`, `Lag`).
4. For exponential weighted averages, use `_fast_ewm(...)` and set a warmup long enough for convergence against TA-Lib. Declare the warmup multiplier as a `WARMUP_FACTOR: ClassVar[int]` on the indicator class.
5. Export the indicator (and `CrossSectionIndicator` if newly relevant) from `tradepy/strategy/__init__.py`.
6. Add focused pytest coverage in `tests/test_indicators.py` (both series and cross-sectional).

## TA-Lib Verification (series indicators)

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

## Cross-section tests

TA-Lib does not apply. Put these in `tests/test_indicators.py` alongside series tests. Cover:

- Values are ranked/transformed **within each date** across codes (not within code over time)
- `over="industry_code"` (or similar) partitions by `(date, …)`
- Composition with series indicators raises `TypeError`

Evaluate with `resolve()` / `resolve_indicators(...)` then `.over(*indicator.partition_by)` on a small multi-code, multi-date frame — do **not** go through `StrategyBase.compute_indicators` for indicator unit tests.

## Commands

```bash
uv run pytest tests/test_indicators.py
```

If type or lint behavior is relevant to the change, also run the existing project commands for pyright or ruff.
