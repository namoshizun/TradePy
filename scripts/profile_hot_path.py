"""Benchmark and profile the backtest hot path (Backtester setup + run).

Typical workflow (mirrors repo-root test.py):

  uv run python scripts/profile_hot_path.py benchmark
  uv run python scripts/profile_hot_path.py cprofile
  uv run python scripts/profile_hot_path.py flamegraph
  uv run python scripts/profile_hot_path.py pyinstrument-html

On macOS, `py-spy` sampling needs root (`sudo uv run py-spy record ...`); the default
`flamegraph` command uses cProfile + flameprof instead (no special permissions).
"""

from __future__ import annotations

import cProfile
import gc
import platform
import pstats
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

import polars as pl
import typer
from pyinstrument import Profiler

from tradepy.backtest.backtester import Backtester
from tradepy.core.account import BacktestAccount
from tradepy.core.config import BacktestConf, StrategyConf
from tradepy.pipelines.assemble_dataset import AssembleDatasetPipeline

app = typer.Typer(add_completion=False, no_args_is_help=True)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PROFILE_DIR = ROOT / "profiles"
CACHE_DF = PROFILE_DIR / "hot_path_df.parquet"

DEFAULT_SINCE = date(2013, 1, 1)
DEFAULT_UNTIL = date(2026, 6, 1)

STRATEGY_CONF = StrategyConf(
    strategy_class="playground.test.MovingAverageCrossoverStrategy",
    stop_loss=4.5,
    take_profit=3,
    max_position_size=0.3,
    max_position_opens=10,
    min_trade_amount=5000,
)


def _strategy_conf() -> StrategyConf:
    return STRATEGY_CONF.model_copy(deep=True)


def prepare_dataframe(
    since: date = DEFAULT_SINCE,
    until: date = DEFAULT_UNTIL,
    *,
    use_cache: bool = True,
) -> pl.DataFrame:
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    if use_cache and CACHE_DF.is_file():
        return pl.read_parquet(CACHE_DF)

    p = AssembleDatasetPipeline(since, until)
    df = p.execute().collect()
    strategy = _strategy_conf().load_strategy()
    df = strategy.compute_indicators(df)
    df = df.with_columns(
        strategy.build_buy_expr().alias("buy_price"),
        strategy.build_sell_expr().alias("sell_price"),
    )
    df.write_parquet(CACHE_DF)
    return df


def run_hot_path(df: pl.DataFrame) -> None:
    bt_conf = BacktestConf(initial_capital=5e6, broker_commission_rate=0.01)
    bt = Backtester(bt_conf, _strategy_conf())
    bt.run(df)


def run_backtest_loop(df: pl.DataFrame) -> BacktestAccount:
    """Run only Backtester.run's trading phase (stable input for loop benchmarks)."""
    strategy = _strategy_conf().load_strategy()
    if not df["date"].is_sorted():
        df = df.sort("date", "code")
    bt_conf = BacktestConf(initial_capital=5e6, broker_commission_rate=0.01)
    bt = Backtester(bt_conf, _strategy_conf())
    bt.backtest(strategy, df)
    return bt.account


def _summarize_samples(samples: list[float], label: str) -> None:
    mean = sum(samples) / len(samples)
    if len(samples) > 1:
        var = sum((x - mean) ** 2 for x in samples) / (len(samples) - 1)
        stdev = var**0.5
        typer.echo(
            f"{label}: mean={mean:.3f}s stdev={stdev:.3f}s (n={len(samples)})"
        )
    else:
        typer.echo(f"{label}: {mean:.3f}s")


@app.command("benchmark-backtest")
def benchmark_backtest(
    repeats: int = typer.Option(5, min=1),
    since: str = typer.Option("2013-01-01"),
    until: str = typer.Option("2026-06-01"),
) -> None:
    """Wall-clock timing of Backtester._backtest only (loop optimizations)."""
    df = prepare_dataframe(_parse_date(since), _parse_date(until))
    _warmup(df)

    samples: list[float] = []
    for i in range(repeats):
        t0 = time.perf_counter()
        run_backtest_loop(df)
        elapsed = time.perf_counter() - t0
        samples.append(elapsed)
        typer.echo(f"  run {i + 1}/{repeats}: {elapsed:.3f}s")

    _summarize_samples(samples, "_backtest loop")


def _warmup(df: pl.DataFrame) -> None:
    gc.collect()
    gc.collect(2)


def _parse_date(value: str) -> date:
    y, m, d = (int(x) for x in value.split("-"))
    return date(y, m, d)


@app.command()
def cache(
    since: str = typer.Option("2013-01-01"),
    until: str = typer.Option("2026-06-01"),
) -> None:
    """Build and persist the prepared dataframe (slow; run once)."""
    t0 = time.perf_counter()
    prepare_dataframe(_parse_date(since), _parse_date(until), use_cache=False)
    typer.echo(
        f"Cached dataframe at {CACHE_DF} ({time.perf_counter() - t0:.1f}s)"
    )


@app.command()
def benchmark(
    repeats: int = typer.Option(3, min=1),
    since: str = typer.Option("2013-01-01"),
    until: str = typer.Option("2026-06-01"),
) -> None:
    """Wall-clock timing of the hot path only."""
    df = prepare_dataframe(_parse_date(since), _parse_date(until))
    _warmup(df)

    samples: list[float] = []
    for i in range(repeats):
        t0 = time.perf_counter()
        run_hot_path(df)
        elapsed = time.perf_counter() - t0
        samples.append(elapsed)
        typer.echo(f"  run {i + 1}/{repeats}: {elapsed:.3f}s")

    mean = sum(samples) / len(samples)
    if len(samples) > 1:
        var = sum((x - mean) ** 2 for x in samples) / (len(samples) - 1)
        stdev = var**0.5
        typer.echo(
            f"hot path: mean={mean:.3f}s stdev={stdev:.3f}s (n={repeats})"
        )
    else:
        typer.echo(f"hot path: {mean:.3f}s")


@app.command()
def cprofile(
    since: str = typer.Option("2013-01-01"),
    until: str = typer.Option("2026-06-01"),
    top: int = typer.Option(40, min=5),
    out: Path = typer.Option(PROFILE_DIR / "hot_path.prof"),
) -> None:
    """CPU profile (cProfile) of the hot path; prints top functions by cumtime."""
    df = prepare_dataframe(_parse_date(since), _parse_date(until))
    _warmup(df)

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    profiler = cProfile.Profile()
    profiler.enable()
    run_hot_path(df)
    profiler.disable()
    profiler.dump_stats(out)

    stats = pstats.Stats(profiler)
    stats.sort_stats(pstats.SortKey.CUMULATIVE)
    typer.echo(f"Wrote {out}")
    typer.echo(f"--- top {top} by cumulative time (tradepy first) ---")
    stats.print_stats(top)
    typer.echo("--- top tradepy/tradepy-only ---")
    stats.print_stats("tradepy", top)


@app.command()
def flamegraph(
    since: str = typer.Option("2013-01-01"),
    until: str = typer.Option("2026-06-01"),
    svg: Path = typer.Option(PROFILE_DIR / "hot_path_flamegraph.svg"),
    prof: Path = typer.Option(PROFILE_DIR / "hot_path.prof"),
    use_py_spy: bool = typer.Option(
        False,
        "--py-spy",
        help="Use py-spy sampling (Linux, or macOS with sudo) instead of cProfile+flameprof",
    ),
) -> None:
    """CPU flamegraph of the hot path (SVG). Default: deterministic cProfile + flameprof."""
    df = prepare_dataframe(_parse_date(since), _parse_date(until))
    _warmup(df)
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    if use_py_spy:
        speedscope_out = svg.with_suffix(".speedscope.json")
        _record_py_spy(speedscope_out)
        typer.echo(
            "py-spy writes Speedscope JSON; convert or open at https://www.speedscope.app"
        )
        return

    profiler = cProfile.Profile()
    profiler.enable()
    run_hot_path(df)
    profiler.disable()
    profiler.dump_stats(prof)

    svg_bytes = subprocess.check_output(["flameprof", str(prof)], cwd=ROOT)
    svg.write_bytes(svg_bytes)
    typer.echo(f"Profile stats: {prof.resolve()}")
    typer.echo(f"Flamegraph SVG: {svg.resolve()}")


def _record_py_spy(out: Path) -> None:
    env = {
        **dict(__import__("os").environ),
        "PROFILE_HOT_PATH_ONLY": "1",
        "PROFILE_HOT_PATH_DF": str(CACHE_DF),
    }
    cmd = [
        "py-spy",
        "record",
        "-f",
        "speedscope",
        "-o",
        str(out),
        "--",
        sys.executable,
        str(Path(__file__).resolve()),
        "exec-hot-path",
    ]
    if platform.system() == "Darwin":
        typer.echo(
            "On macOS, py-spy usually requires: sudo uv run python scripts/profile_hot_path.py flamegraph --py-spy"
        )
    subprocess.run(cmd, check=True, env=env, cwd=ROOT)


@app.command("pyinstrument-html")
def pyinstrument_html(
    since: str = typer.Option("2013-01-01"),
    until: str = typer.Option("2026-06-01"),
    out: Path = typer.Option(PROFILE_DIR / "hot_path_pyinstrument.html"),
) -> None:
    """Interactive HTML profile (pyinstrument) of the hot path."""
    df = prepare_dataframe(_parse_date(since), _parse_date(until))
    _warmup(df)
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    profiler = Profiler()
    profiler.start()
    run_hot_path(df)
    profiler.stop()
    out.write_text(profiler.output_html(), encoding="utf-8")
    typer.echo(f"Wrote {out.resolve()}")


@app.command("exec-hot-path", hidden=True)
def exec_hot_path() -> None:
    """Entry point used under py-spy; reads cached df from env."""
    import os

    if os.environ.get("PROFILE_HOT_PATH_ONLY") != "1":
        raise SystemExit("exec-hot-path is for internal profiling use only")
    path = Path(os.environ["PROFILE_HOT_PATH_DF"])
    df = pl.read_parquet(path)
    run_hot_path(df)


if __name__ == "__main__":
    app()
