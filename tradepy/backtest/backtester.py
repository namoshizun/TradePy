import contextlib
import random
from collections.abc import Generator
from dataclasses import dataclass
from typing import Literal

import polars as pl
from loguru import logger
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn

from tradepy.core.account import BacktestAccount
from tradepy.core.config import BacktestConf, StrategyConf
from tradepy.core.position import Position
from tradepy.core.types import BarData
from tradepy.strategy import BacktestStrategyBase
from tradepy.utils import Timer


@dataclass
class TradingContext:
    backtest_conf: BacktestConf
    strategy_conf: StrategyConf
    strategy: BacktestStrategyBase
    account: BacktestAccount
    tradable_bars: dict[str, BarData]


@dataclass(frozen=True, slots=True)
class TradeAction:
    code: str
    price: float
    vol: int
    direction: Literal["buy", "sell"]


@dataclass(frozen=True, slots=True)
class SellAction(TradeAction):
    direction: Literal["sell"] = "sell"
    reason: Literal["stop loss", "take profit", "strategy"] = "strategy"


@dataclass(frozen=True, slots=True)
class BuyAction(TradeAction):
    direction: Literal["buy"] = "buy"


class IndayTrader:
    def __init__(self, context: TradingContext):
        self.ctx = context

    @property
    def account(self) -> BacktestAccount:
        return self.ctx.account

    @property
    def strategy(self) -> BacktestStrategyBase:
        return self.ctx.strategy

    @property
    def backtest_conf(self) -> BacktestConf:
        return self.ctx.backtest_conf

    @property
    def strategy_conf(self) -> StrategyConf:
        return self.ctx.strategy_conf

    def process_sells(self) -> Generator[SellAction, None, None]:
        for code in self.account.holdings.position_codes:
            if code not in self.ctx.tradable_bars:
                # Not a tradable day.
                continue

            pos = self.account.holdings[code]
            bar = self.ctx.tradable_bars[code]
            stop_loss_price = self.strategy.should_stop_loss(bar, pos)
            take_profit_price = self.strategy.should_take_profit(bar, pos)
            sell_price, reason = None, None

            if stop_loss_price or take_profit_price:
                # Static SL / TP
                if stop_loss_price and take_profit_price:
                    # This day's price movement meets both, so randomly choose one
                    sl_first = (
                        True
                        if self.backtest_conf.sl_tf_order == "stop loss first"
                        else False
                        if self.backtest_conf.sl_tf_order == "take profit first"
                        else random.randint(1, 10) <= 5
                    )
                    if sl_first:
                        # Unset take profit if the decided action is to stop loss
                        take_profit_price = None

                if take_profit_price:
                    sell_price, reason = (
                        self.strategy.apply_slippage(
                            take_profit_price, ref_price=bar.orig_open
                        ),
                        "take profit",
                    )
                else:
                    assert stop_loss_price
                    sell_price, reason = (
                        self.strategy.apply_slippage(
                            stop_loss_price, ref_price=bar.orig_open
                        ),
                        "stop loss",
                    )

            else:
                # Sell by strategy
                sell_price, reason = bar.sell_price, "strategy"

            if sell_price and reason:
                yield SellAction(
                    reason=reason, code=code, price=sell_price, vol=pos.vol
                )

    def process_buys(
        self, df: pl.DataFrame
    ) -> Generator[BuyAction, None, None]:
        init_codes: set[str] = self.account.holdings.position_codes
        init_capital: float = self.account.total_asset_value

        buys_df = df.filter(
            pl.col("buy_price").is_not_null()
            & ~pl.col("code").is_in(init_codes)
        )
        if buys_df.is_empty():
            return

        free_cash = self.account.free_cash_amount
        budget = free_cash - self.account.get_broker_commission_fee(free_cash)
        for alloc in self.strategy.optimize_portfolio(
            buys_df, budget, init_capital
        ):
            yield BuyAction(code=alloc.code, price=alloc.price, vol=alloc.vol)

    def trade(self, df: pl.DataFrame) -> Generator[TradeAction, None, None]:
        yield from self.process_buys(df)
        yield from self.process_sells()


class Backtester:
    def __init__(
        self, config: BacktestConf, strategy_conf: StrategyConf
    ) -> None:
        self.config = config
        self.strategy_conf = strategy_conf
        self.account = BacktestAccount(
            free_cash_amount=config.initial_capital,
            frozen_cash_amount=0,
            broker_commission_rate=config.broker_commission_rate,
            min_broker_commission_fee=config.min_broker_commission_fee,
            stamp_duty_rate=config.stamp_duty_rate,
        )

    def _backtest(self, strategy: BacktestStrategyBase, df: pl.DataFrame):
        random.seed()

        progress_columns = (
            TextColumn("[{task.description}]", markup=False),
            BarColumn(),
            TaskProgressColumn(),
        )
        n_days = df.select(pl.col("date").n_unique()).item()
        ctx = TradingContext(
            backtest_conf=self.config,
            strategy_conf=self.strategy_conf,
            strategy=strategy,
            account=self.account,
            tradable_bars={},
        )
        trader = IndayTrader(ctx)

        with Progress(*progress_columns) as progress:
            task_id = progress.add_task("回测交易日", total=n_days)

            # Per day...
            for (date,), date_df in df.group_by("date", maintain_order=True):
                # Build the tradable stocks' price lookup table
                ctx.tradable_bars = (
                    _bars := {
                        _row[0]: BarData(*_row)
                        for _row in date_df.filter(
                            pl.col("code").is_in(
                                self.account.holdings.position_codes
                            )
                            | pl.col("buy_price").is_not_null()
                        )
                        .select(BarData.__annotations__.keys())
                        .iter_rows(named=False)
                    }
                )

                # Trading...
                self.account.unfreeze_cash(self.account.frozen_cash_amount)
                pos_id = 1
                timestamp = date.isoformat()
                for action in trader.trade(date_df):
                    if action.direction == "buy":
                        pos_id += 1
                        pos = Position(
                            id=str(id),
                            timestamp=timestamp,
                            code=action.code,
                            price=action.price,
                            latest_price=action.price,
                            avail_vol=action.vol,
                            vol=action.vol,
                            yesterday_vol=action.vol,
                        )
                        self.account.buy(pos)
                    else:
                        pos = self.account.holdings[action.code]
                        pos.update_price(action.price)
                        self.account.sell(pos)

                    # Update holdings latest prices
                    for code, pos in self.account.holdings:
                        with contextlib.suppress(KeyError):
                            pos.update_price(_bars[code].orig_close)

                progress.advance(task_id)

    def run(self, df: pl.DataFrame | pl.LazyFrame):
        # Load strategy
        logger.info("🤗 加载策略...")
        strategy = self.strategy_conf.load_strategy()
        logger.opt(colors=True).info("<g>OK!</g>")

        # Sanity check
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

        # Compute buy prices
        with Timer("s") as timer:
            logger.info("🚀 计算买卖点位...")
            df = df.with_columns(
                strategy.build_buy_expr().alias("buy_price"),
                strategy.build_sell_expr().alias("sell_price"),
            )
        logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}s)</g>")

        # Party begins!
        logger.info("📈 开始回测交易...")
        with Timer("s") as timer:
            self._backtest(strategy, df)

        logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}s)</g>")

        return df
