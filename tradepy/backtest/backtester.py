import random

import polars as pl
from loguru import logger

from tradepy.core.config import BacktestConf, StrategyConf
from tradepy.strategy import StrategyBase
from tradepy.utils import Timer


class Backtester:
    def __init__(self, config: BacktestConf) -> None:
        self.config = config

    def _trade(self, df: pl.DataFrame, strategy: StrategyBase):
        random.seed()

    def run(self, df: pl.DataFrame | pl.LazyFrame, strategy_conf: StrategyConf):
        # [1] Prepare strategy instance
        logger.info("🔍 加载策略类...")
        with Timer("ms") as timer:
            strategy = strategy_conf.load_strategy()
        logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}ms)</g>")

        # [2] Sanity check
        logger.info("🧐 检查回测数据...")
        with Timer("s") as timer:
            if isinstance(df, pl.LazyFrame):
                df = df.collect()

            if df.is_empty():
                raise ValueError("回测数据为空")

            indicators_ready = set(
                strategy.infer_required_indicators()
            ).issubset(df.columns)
            if not indicators_ready:
                logger.info("🏋️ 计算技术指标...")
                df = strategy.compute_indicators(df)

            if not df["date"].is_sorted():
                df = df.sort("date", "code")

        logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}s)</g>")

        # [3] Compute buy signals
        with Timer("s") as timer:
            logger.info("🏋️ 计算买入信号...")
            df = df.with_columns(
                strategy.transpile_buy_expr().alias("buy_signal")
            )
        logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}s)</g>")

        # [3] Party begins!
        logger.info("📈 开始回测交易...")
        with Timer("s") as timer:
            self._trade(df, strategy)

        logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}s)</g>")

        return df
