import sys
import random
import pandas as pd
from typing import TYPE_CHECKING, Any
from tqdm import tqdm

from tradepy import LOG
from tradepy.blacklist import Blacklist
from tradepy.core.account import BacktestAccount
from tradepy.core.order import Order
from tradepy.core.position import Position
from tradepy.mixins import TradeMixin
from tradepy.trade_book import TradeBook
from tradepy.core.conf import BacktestConf

if TYPE_CHECKING:
    from tradepy.strategy.base import StrategyBase


class Backtester(TradeMixin):
    def __init__(self, conf: BacktestConf) -> None:
        self.account = BacktestAccount(
            free_cash_amount=conf.cash_amount,
            broker_commission_rate=conf.broker_commission_rate,
            stamp_duty_rate=conf.stamp_duty_rate,
        )

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
        sell_positions: list[Position],
    ) -> pd.DataFrame:
        sold_or_holding_codes = self.account.holdings.position_codes | set(
            pos.code for pos in sell_positions
        )
        jitter_price = lambda p: p * random.uniform(
            1 - 1e-4 * 5, 1 + 1e-4 * 5
        )  # 0.05% slip

        # Looks ugly but it's fast...
        indices_and_prices = [
            (index, jitter_price(price_and_weight[0]), price_and_weight[1])
            for index, *indicators in bars_df[strategy.buy_indicators].itertuples(
                name=None
            )
            if (index[1] not in sold_or_holding_codes)
            and (not Blacklist.contains(index[1]))
            and (price_and_weight := strategy.should_buy(*indicators))
        ]

        if not indices_and_prices:
            return pd.DataFrame()

        indices, prices, weights = zip(*indices_and_prices)
        return pd.DataFrame(
            {
                "order_price": prices,
                "weight": weights,
            },
            index=pd.MultiIndex.from_tuples(indices, names=["timestamp", "code"]),
        )

    def get_close_signals(
        self, df: pd.DataFrame, strategy: "StrategyBase"
    ) -> list[Any]:
        if not strategy.close_indicators:
            return []

        curr_positions = self.account.holdings.position_codes
        if not curr_positions:
            return []

        return [
            index
            for index, *indicators in df[strategy.close_indicators].itertuples(
                name=None
            )
            if (index[1] in curr_positions) and strategy.should_close(*indicators)
        ]

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
        for timestamp, sub_df in tqdm(df.groupby(level="timestamp"), file=sys.stdout):
            assert isinstance(timestamp, str)

            # Opening
            price_lookup = lambda code: sub_df.loc[
                (timestamp, code), "close"
            ]  # NOTE: slow
            self.account.update_holdings(price_lookup)

            # Sell
            close_indices = self.get_close_signals(sub_df, strategy)
            sell_positions = []
            for code, pos in self.account.holdings:
                index = (timestamp, code)
                if index not in sub_df.index:
                    # Not a tradable day, so nothing to do
                    continue

                bar = sub_df.loc[index].to_dict()  # type: ignore

                # [1] Take profit
                if take_profit_price := self.should_take_profit(strategy, bar, pos):
                    pos.close(take_profit_price)
                    trade_book.take_profit(timestamp, pos)
                    sell_positions.append(pos)

                # [2] Stop loss
                elif stop_loss_price := self.should_stop_loss(strategy, bar, pos):
                    pos.close(stop_loss_price)
                    trade_book.stop_loss(timestamp, pos)
                    sell_positions.append(pos)

                # [3] Close
                elif index in close_indices:
                    pos.close(bar["close"])
                    trade_book.close(timestamp, pos)
                    sell_positions.append(pos)

            if sell_positions:
                self.account.sell(sell_positions)

            # Buy
            port_df = self.get_buy_options(
                sub_df, strategy, sell_positions
            )  # list[DF_Index, BuyPrice]
            if not port_df.empty:
                free_cash = self.account.free_cash_amount
                budget = free_cash - self.account.get_buy_commissions(free_cash)

                port_df, budget = strategy.adjust_portfolio_and_budget(
                    port_df=port_df,
                    budget=budget,
                    n_stocks=len(sub_df),
                    total_asset_value=self.account.total_asset_value,
                )

                buy_orders = strategy.generate_buy_orders(port_df, budget)
                buy_positions = self.__orders_to_positions(buy_orders)

                self.account.buy(buy_positions)
                for pos in buy_positions:
                    trade_book.buy(timestamp, pos)

            # Logging
            trade_book.log_closing_capitals(timestamp, self.account)

        # That was quite a long story :D
        return trade_book

    def run(
        self, bars_df: pd.DataFrame, strategy: "StrategyBase"
    ) -> tuple[pd.DataFrame, TradeBook]:
        ind_df = strategy.compute_all_indicators_df(bars_df)
        trade_book = self.trade(ind_df, strategy)
        return ind_df, trade_book
