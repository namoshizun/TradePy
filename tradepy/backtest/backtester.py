import sys
import random
import pandas as pd
import numpy as np
from typing import TYPE_CHECKING
from tqdm import tqdm

from tradepy import LOG, utils
from tradepy.blacklist import Blacklist
from tradepy.core.account import BacktestAccount
from tradepy.core.order import Order
from tradepy.core.position import Position
from tradepy.depot.stocks import StockMinuteBarsDepot
from tradepy.mixins import TradeMixin
from tradepy.trade_book import TradeBook
from tradepy.core.conf import BacktestConf, SlippageConf

if TYPE_CHECKING:
    from tradepy.strategy.base import StrategyBase


class Backtester(TradeMixin):
    def __init__(self, conf: BacktestConf) -> None:
        self.account = BacktestAccount(
            free_cash_amount=conf.cash_amount,
            broker_commission_rate=conf.broker_commission_rate,
            min_broker_commission_fee=conf.min_broker_commission_fee,
            stamp_duty_rate=conf.stamp_duty_rate,
        )
        self.strategy_conf = conf.strategy
        self.use_minute_k = conf.use_minute_k
        self.sl_tf_order = conf.sl_tf_order

    def _jit_sell_price(
        self, price: float, slip: SlippageConf, orig_open_price: float
    ) -> float:
        method, params = slip.method, slip.params

        if method == "max_jump":
            max_num_jumps = int(params)
            one_jump_pct_chg = 0.01 / orig_open_price
            pct_chgs = [one_jump_pct_chg * i for i in range(1, max_num_jumps + 1)]
            slip_pct = random.choice(pct_chgs + [0])
            return price * (1 - slip_pct)

        if method == "max_pct":
            max_pct_chg = float(params)
            jitter = random.uniform(0, max_pct_chg * 1e-2)
            return price * (1 - jitter)

        if method == "weibull":
            slip_pct_chg = (
                np.random.weibull(params["shape"]) * params["scale"] + params["shift"]
            )
            return price * (1 - slip_pct_chg * 1e-2)

        raise ValueError(f"无效的滑点配置: {slip}")

    def __orders_to_positions(self, orders: list[Order]) -> list[Position]:
        return [
            Position(
                id=o.id,
                code=o.code,
                price=o.price,
                latest_price=o.price,
                timestamp=o.timestamp,
                vol=o.vol,
                avail_vol=o.vol,
            )
            for o in orders
        ]

    def get_buy_options(
        self,
        bars_df: pd.DataFrame,
        strategy: "StrategyBase",
    ) -> pd.DataFrame:
        holding_codes = self.account.holdings.position_codes
        jitter_price = lambda p: p * random.uniform(
            1 - 1e-4 * 3, 1 + 1e-4 * 3
        )  # 0.03% slip

        # Looks ugly but it's fast...
        codes_and_prices = [
            (code, jitter_price(price_and_weight[0]), price_and_weight[1])
            for code, *indicators in bars_df[strategy.buy_indicators].itertuples(
                name=None
            )
            if (code not in holding_codes)
            and (not Blacklist.contains(code))
            and (price_and_weight := strategy.should_buy(*indicators))
        ]

        if not codes_and_prices:
            return pd.DataFrame()

        codes, prices, weights = zip(*codes_and_prices)
        return pd.DataFrame(
            {
                "order_price": prices,
                "weight": weights,
            },
            index=pd.Index(codes, name="code"),
        )

    def get_close_signals(
        self, df: pd.DataFrame, strategy: "StrategyBase"
    ) -> list[str]:
        if not strategy.close_indicators:
            return []

        curr_positions = self.account.holdings.position_codes
        if not curr_positions:
            return []

        return [
            code
            for code, *indicators in df[strategy.close_indicators].itertuples(name=None)
            if (code in curr_positions) and strategy.should_close(*indicators)
        ]

    def _trade_using_day_k(
        self,
        date: str,
        bars_df: pd.DataFrame,
        trade_book: TradeBook,
        strategy: "StrategyBase",
    ):
        # Sell
        buys_df = self.get_buy_options(bars_df, strategy)
        close_codes = self.get_close_signals(bars_df, strategy)
        sell_positions = []

        for code, pos in self.account.holdings:
            if code not in bars_df.index:
                # Not a tradable day, so nothing to do
                continue

            bar = bars_df.loc[code].to_dict()  # type: ignore
            stop_loss_price = self.should_stop_loss(strategy, bar, pos)
            take_profit_price = self.should_take_profit(strategy, bar, pos)

            if stop_loss_price or take_profit_price:
                should_stop_loss = False
                if stop_loss_price and take_profit_price:
                    # This day's price movement meets both, so randomly choose one
                    should_stop_loss = (
                        True
                        if self.sl_tf_order == "stop loss first"
                        else False
                        if self.sl_tf_order == "take profit first"
                        else random.randint(1, 10) <= 5
                    )
                else:
                    # Either stop loss or take profit
                    should_stop_loss = stop_loss_price is not None

                if should_stop_loss:
                    assert stop_loss_price
                    stop_loss_price = self._jit_sell_price(
                        stop_loss_price,
                        self.strategy_conf.stop_loss_slip,
                        bar["orig_open"],
                    )
                    pos.close(stop_loss_price)
                    trade_book.stop_loss(date, pos)
                    sell_positions.append(pos)
                else:
                    assert take_profit_price
                    take_profit_price = self._jit_sell_price(
                        take_profit_price,
                        self.strategy_conf.take_profit_slip,
                        bar["orig_open"],
                    )
                    pos.close(take_profit_price)
                    trade_book.take_profit(date, pos)
                    sell_positions.append(pos)

            # Close position in the market closing phase
            elif code in close_codes:
                pos.close(bar["close"])
                trade_book.close(date, pos)
                sell_positions.append(pos)

        if sell_positions:
            self.account.sell(sell_positions)

        # Buy
        if not buys_df.empty:
            free_cash = self.account.free_cash_amount
            budget = free_cash - self.account.get_broker_commission_fee(free_cash)

            buys_df, budget = strategy.adjust_portfolio_and_budget(
                port_df=buys_df,
                budget=budget,
                n_stocks=len(bars_df),
                total_asset_value=self.account.total_asset_value,
            )

            buy_orders = strategy.generate_buy_orders(buys_df, date, budget)
            buy_positions = self.__orders_to_positions(buy_orders)

            self.account.buy(buy_positions)
            for pos in buy_positions:
                trade_book.buy(date, pos)

    def _trade_using_minute_k(
        self,
        date: str,
        day_df: pd.DataFrame,
        min_df: pd.DataFrame,
        trade_book: TradeBook,
        strategy: "StrategyBase",
    ):
        buys_df = self.get_buy_options(day_df, strategy)
        suspending_codes = set()

        # Only look at the intraday bars of the stocks that are tradable (ones can be bought / sold)
        tradable_codes: list[str] = list(
            set(buys_df.index) | set(self.account.holdings.position_codes)
        )

        if not tradable_codes:
            return

        # Adjust minute bar's prices to match the day bar's prices
        compute_adjust_factors = lambda codes: (
            day_df.loc[codes, "open"] / day_df.loc[codes, "orig_open"]
        )  # type: ignore
        try:
            adjust_factors = compute_adjust_factors(tradable_codes)
        except KeyError:
            # Some stocks in holding may not be tradable this day
            suspending_codes = set(tradable_codes) - set(day_df.index)
            tradable_codes = list(set(tradable_codes) - suspending_codes)
            adjust_factors = compute_adjust_factors(tradable_codes)

        min_df = min_df.loc[tradable_codes].copy()
        min_df.sort_index(inplace=True)
        min_df["orig_open"] = min_df["open"].copy()
        min_df["open"] = (min_df["open"] * adjust_factors).values
        min_df["low"] = (min_df["low"] * adjust_factors).values
        min_df["high"] = (min_df["high"] * adjust_factors).values
        min_df["close"] = (min_df["close"] * adjust_factors).values

        for time, min_bars in min_df.groupby("time"):
            # Sell
            # if time < "1456":
            for code in self.account.holdings.position_codes:
                pos = self.account.holdings[code]
                if pos.timestamp == date or code in suspending_codes:
                    # The stocks just bought today or in suspension are not tradable
                    continue

                bar = min_bars.loc[code].to_dict()
                sell = False

                # [1] Take profit
                if take_profit_price := self.should_take_profit(strategy, bar, pos):
                    take_profit_price = self._jit_sell_price(
                        take_profit_price,
                        self.strategy_conf.take_profit_slip,
                        bar["orig_open"],
                    )
                    pos.close(take_profit_price)
                    trade_book.take_profit(date, pos)
                    sell = True

                # [2] Stop loss
                elif stop_loss_price := self.should_stop_loss(strategy, bar, pos):
                    stop_loss_price = self._jit_sell_price(
                        stop_loss_price,
                        self.strategy_conf.stop_loss_slip,
                        bar["orig_open"],
                    )
                    pos.close(stop_loss_price)
                    trade_book.stop_loss(date, pos)
                    sell = True

                if sell:
                    self.account.sell([pos])
                    buys_df.drop(pos.code, inplace=True, errors="ignore")

            # Buy
            # if time >= "1456":
            free_cash = self.account.free_cash_amount
            if free_cash >= self.strategy_conf.min_trade_amount and not buys_df.empty:
                # Get stocks whose buy signal price is between this minute bar's high and low
                selector = utils.between(
                    buys_df["order_price"], min_bars["low"], min_bars["high"]
                )
                assert isinstance(selector, pd.Series)
                if selector.any():
                    _buys_df = buys_df[selector]
                    # Buy them
                    budget = free_cash - self.account.get_broker_commission_fee(
                        free_cash
                    )

                    _buys_df, budget = strategy.adjust_portfolio_and_budget(
                        port_df=_buys_df,
                        budget=budget,
                        n_stocks=len(day_df),
                        total_asset_value=self.account.total_asset_value,
                    )

                    buy_orders = strategy.generate_buy_orders(_buys_df, date, budget)
                    buy_positions = self.__orders_to_positions(buy_orders)

                    self.account.buy(buy_positions)
                    for pos in buy_positions:
                        trade_book.buy(date, pos)

                    # Drop them from the buys df
                    buys_df.drop(_buys_df.index, inplace=True)

    def trade(self, df: pd.DataFrame, strategy: "StrategyBase") -> TradeBook:
        random.seed()
        if list(getattr(df.index, "names", [])) != ["timestamp", "code"]:
            LOG.info(">>> 重建索引 [timestamp, code]")
            try:
                df.reset_index(inplace=True)
            except ValueError:
                df.reset_index(inplace=True, drop=True)
            df.set_index(["timestamp", "code"], inplace=True, drop=False)
            df.sort_index(inplace=True)

        LOG.info(">>> 交易中 ...")
        trade_book = TradeBook.backtest()

        # Per day
        month, month_minute_df = None, pd.DataFrame()
        for date, bars_df in tqdm(df.groupby(level="timestamp"), file=sys.stdout):
            assert isinstance(date, str)

            # Opening
            bars_df = bars_df.loc[date]  # to remove the timestamp index
            price_lookup = lambda code: bars_df.loc[code, "close"]  # NOTE: slow
            self.account.update_holdings(price_lookup)

            # Trading
            if self.use_minute_k:
                if month != date[:7]:
                    month = date[:7]
                    month_minute_df = StockMinuteBarsDepot.load(month)

                self._trade_using_minute_k(
                    date, bars_df, month_minute_df.loc[(date,)], trade_book, strategy
                )
            else:
                self._trade_using_day_k(date, bars_df, trade_book, strategy)

            # Logging
            trade_book.log_closing_capitals(date, self.account)

        # That was quite a long story :D
        return trade_book

    def run(
        self, bars_df: pd.DataFrame, strategy: "StrategyBase"
    ) -> tuple[pd.DataFrame, TradeBook]:
        ind_df = strategy.compute_all_indicators_df(bars_df)
        trade_book = self.trade(ind_df, strategy)
        return ind_df, trade_book
