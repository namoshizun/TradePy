import random

import polars as pl
from loguru import logger

from tradepy.core.account import BacktestAccount
from tradepy.core.config import BacktestConf, StrategyConf
from tradepy.core.position import Position
from tradepy.core.types import BarData
from tradepy.strategy import StrategyBase
from tradepy.utils import Timer


class Backtester:
    def __init__(self, config: BacktestConf) -> None:
        self.config = config
        self.account = BacktestAccount(
            free_cash_amount=config.initial_capital,
            frozen_cash_amount=0,
            broker_commission_rate=config.broker_commission_rate,
            min_broker_commission_fee=config.min_broker_commission_fee,
            stamp_duty_rate=config.stamp_duty_rate,
        )

    def _inday_trade(self, df: pl.DataFrame, strategy: StrategyBase):
        self.account.unfreeze_cash(self.account.frozen_cash_amount)
        opening_codes: set[str] = self.account.holdings.position_codes
        position_bars: dict[
            str, BarData
        ] = {  # TODO: build as `tradable bars`, includes all positions and buyable stocks
            _row["code"]: BarData(**_row)
            for _row in df.filter(pl.col("code").is_in(opening_codes))
            .select(BarData.__annotations__.keys())
            .iter_rows(named=True)
        }

        # Sell ---------------------------------------------------------------
        sell_positions: list[Position] = []

        for code, pos in self.account.holdings:
            if code not in position_bars:
                # Not a tradable day.
                continue

            bar = position_bars[code]
            stop_loss_price = strategy.should_stop_loss(bar, pos)
            take_profit_price = strategy.should_take_profit(bar, pos)
            sell_price = None

            if stop_loss_price or take_profit_price:
                # Static SL / TP
                if stop_loss_price and take_profit_price:
                    # This day's price movement meets both, so randomly choose one
                    sl_first = (
                        True
                        if self.config.sl_tf_order == "stop loss first"
                        else False
                        if self.config.sl_tf_order == "take profit first"
                        else random.randint(1, 10) <= 5
                    )
                    if sl_first:
                        # Unset take profit if the decided action is to stop loss
                        take_profit_price = None

                if stop_loss_price:
                    sell_price = strategy.apply_slippage(
                        stop_loss_price, ref_price=bar.orig_open
                    )
                else:
                    assert take_profit_price
                    sell_price = strategy.apply_slippage(
                        take_profit_price, ref_price=bar.orig_open
                    )

            else:
                # Sell by strategy
                sell_price = bar.sell_price

            if sell_price:
                # TODO: yield trade log
                pos.update_price(sell_price)
                sell_positions.append(pos)

        if sell_positions:
            self.account.sell(sell_positions)

        # Buy
        buys_df = df.filter(pl.col("buy_price").is_not_null())
        if buys_df.is_empty():
            return

        free_cash = self.account.free_cash_amount
        budget = free_cash - self.account.get_broker_commission_fee(free_cash)
        # TODO: yield trade log

    def _trade(self, df: pl.DataFrame, strategy: StrategyBase):
        random.seed()

        for date, date_df in df.group_by("date"):
            self._inday_trade(date_df, strategy)

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
                logger.info("🚀 计算技术指标...")
                df = strategy.compute_indicators(df)

            if not df["date"].is_sorted():
                df = df.sort("date", "code")

        logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}s)</g>")

        # [3] Compute buy prices
        with Timer("s") as timer:
            logger.info("🚀 计算买入价格...")
            df = df.with_columns(
                strategy.build_buy_expr().alias("buy_price"),
                strategy.build_sell_expr().alias("sell_price"),
            )
        logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}s)</g>")

        # [3] Party begins!
        logger.info("📈 开始回测交易...")
        with Timer("s") as timer:
            self._trade(df, strategy)

        logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}s)</g>")

        return df
