import contextlib
import random
from collections.abc import Generator
from dataclasses import dataclass

import polars as pl
from loguru import logger
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn

from tradepy.core.account import BacktestAccount
from tradepy.core.config import BacktestConf, StrategyConf
from tradepy.core.position import Position
from tradepy.core.types import BarData, TradeActionType
from tradepy.strategy import BacktestStrategyBase
from tradepy.trade_book.trade_book import TradeBook
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
    type: TradeActionType
    code: str
    price: float
    vol: int


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

    def process_sells(self) -> Generator[TradeAction, None, None]:
        for code in list(self.account.holdings.avail_codes):
            if code not in self.ctx.tradable_bars:
                continue

            pos = self.account.holdings[code]
            bar = self.ctx.tradable_bars[code]
            stop_loss_price = self.strategy.should_stop_loss(bar, pos)
            take_profit_price = self.strategy.should_take_profit(bar, pos)
            sell_price, reason = None, None

            if stop_loss_price or take_profit_price:
                if stop_loss_price and take_profit_price:
                    sl_first = (
                        True
                        if self.backtest_conf.sl_tf_order == "stop loss first"
                        else False
                        if self.backtest_conf.sl_tf_order == "take profit first"
                        else random.randint(1, 10) <= 5
                    )
                    if sl_first:
                        take_profit_price = None

                if take_profit_price:
                    sell_price, reason = (
                        self.strategy.apply_slippage(
                            take_profit_price, ref_price=bar.open
                        ),
                        "止盈",
                    )
                else:
                    assert stop_loss_price
                    sell_price, reason = (
                        self.strategy.apply_slippage(
                            stop_loss_price, ref_price=bar.open
                        ),
                        "止损",
                    )

            else:
                sell_price, reason = bar.sell_price, "平仓"

            if sell_price and reason:
                yield TradeAction(
                    type=reason, code=code, price=sell_price, vol=pos.vol
                )

    def process_buys(self) -> Generator[TradeAction, None, None]:
        if self.account.free_cash_amount < self.strategy_conf.min_trade_amount:
            return

        init_capital: float = self.account.get_total_capital()

        buy_options: list[tuple[str, float]] = [
            (code, bar.buy_price)
            for code, bar in self.ctx.tradable_bars.items()
            if bar.buy_price and not bar.is_held
        ]

        if not buy_options:
            return

        free_cash = self.account.free_cash_amount
        budget = free_cash - self.account.get_broker_commission_fee(free_cash)
        for alloc in self.strategy.optimize_portfolio(
            buy_options, budget, init_capital
        ):
            yield TradeAction(
                type="开仓", code=alloc.code, price=alloc.price, vol=alloc.vol
            )

    def trade(self) -> Generator[TradeAction, None, None]:
        yield from self.process_buys()
        yield from self.process_sells()


class Backtester:
    def __init__(
        self, config: BacktestConf, strategy_conf: StrategyConf
    ) -> None:
        self.config = config
        self.strategy_conf = strategy_conf
        self.account = self.create_account()

    def create_account(self) -> BacktestAccount:
        return BacktestAccount(
            free_cash_amount=self.config.initial_capital,
            frozen_cash_amount=0,
            broker_commission_rate=self.config.broker_commission_rate,
            min_broker_commission_fee=self.config.min_broker_commission_fee,
            stamp_duty_rate=self.config.stamp_duty_rate,
        )

    def _build_tradable_bars(self, date_df: pl.DataFrame) -> dict[str, BarData]:
        held_codes = self.account.holdings.avail_codes
        filtered = date_df.filter(
            pl.col("code").is_in(held_codes) | pl.col("buy_price").is_not_null()
        )
        return {
            _r["code"]: BarData(
                code=_r["code"],
                open=_r["open"],
                close=_r["close"],
                high=_r["high"],
                low=_r["low"],
                vol=_r["vol"],
                pct_chg=_r["pct_chg"],
                sell_price=_r["sell_price"],
                buy_price=_r["buy_price"],
                is_held=_r["code"] in held_codes,
            )
            for _r in filtered.iter_rows(named=True)
        }

    def backtest(self, strategy: BacktestStrategyBase, df: pl.DataFrame):
        random.seed()
        self.account = self.create_account()

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
        trade_book = TradeBook.backtest()

        with Progress(*progress_columns) as progress:
            task_id = progress.add_task("回测交易日", total=n_days)
            pos_id = 1

            for (_day,), date_df in df.group_by("date", maintain_order=True):
                # Pre-open
                timestamp = _day.isoformat()
                self.account.pre_open()
                ctx.tradable_bars = _bars = self._build_tradable_bars(date_df)

                # Buy / Sell
                for action in trader.trade():
                    if action.type == "开仓":
                        pos_id += 1
                        pos = Position(
                            id=str(pos_id),
                            timestamp=timestamp,
                            code=action.code,
                            price=action.price,
                            latest_price=action.price,
                            vol=action.vol,
                        )
                        self.account.buy(pos)
                        trade_book.buy(timestamp, pos)
                    else:
                        pos = self.account.holdings[action.code]
                        assert pos.avail_vol >= action.vol
                        pos.update_price(action.price)
                        self.account.sell(pos)
                        trade_book.sell(timestamp, pos, action.type)

                # End of day
                for code, pos in list(self.account.holdings):
                    with contextlib.suppress(KeyError):
                        pos.update_price(_bars[code].close)

                trade_book.log_closing_capitals(timestamp, self.account)
                progress.advance(task_id)

        return trade_book

    def run(
        self, df: pl.DataFrame | pl.LazyFrame
    ) -> tuple[pl.DataFrame, TradeBook]:
        # [1] --
        logger.info("🤗 加载策略...")
        strategy = self.strategy_conf.load_strategy()
        logger.opt(colors=True).info("<g>OK!</g>")

        # [2] --
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

        # [3] --
        # Only if buy_price and sell_price columns are not available
        if "buy_price" not in df.columns or "sell_price" not in df.columns:
            with Timer("s") as timer:
                logger.info("🚀 计算买卖点位...")
                df = df.with_columns(
                    strategy.build_buy_expr().alias("buy_price"),
                    strategy.build_sell_expr().alias("sell_price"),
                )
            logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}s)</g>")

        # [4] --
        with Timer("s") as timer:
            logger.info("♻️ 预处理数据...")
            df = strategy.pre_process(df)
        logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}s)</g>")

        # [5] --
        logger.info("📈 开始回测交易...")
        with Timer("s") as timer:
            trade_book = self.backtest(strategy, df)

        logger.opt(colors=True).info(f"<g>OK! ({timer.duration:.1f}s)</g>")

        return df, trade_book
