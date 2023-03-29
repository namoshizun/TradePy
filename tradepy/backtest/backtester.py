import sys
import random
import pandas as pd
from typing import TYPE_CHECKING, Any
from tqdm import tqdm

from tradepy import LOG
from tradepy.core.account import BacktestAccount
from tradepy.core.order import Order
from tradepy.core.position import Position
from tradepy.core.trade_book import TradeBook
from tradepy.core.context import Context

if TYPE_CHECKING:
    from tradepy.core.strategy import StrategyBase, BarDataType


class Backtester:

    def __init__(self, ctx: Context) -> None:
        self.account = BacktestAccount(
            free_cash_amount=ctx.cash_amount,
            broker_commission_rate=ctx.broker_commission_rate,
            stamp_duty_rate=ctx.stamp_duty_rate
        )

        if ctx.hfq_adjust_factors is not None:
            _adf = ctx.hfq_adjust_factors.copy()
            _adf.reset_index(inplace=True)
            _adf.set_index("code", inplace=True)
            _adf.sort_values(["code", "timestamp"], inplace=True)
            self.adjust_factors_df = _adf
        else:
            self.adjust_factors_df = None

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

    def get_buy_options(self,
                        bars_df: pd.DataFrame,
                        strategy: "StrategyBase",
                        sell_positions: list[Position]) -> pd.DataFrame:
        sold_or_holding_codes = self.account.holdings.position_codes | \
            set(pos.code for pos in sell_positions)
        jitter_price = lambda p: p * random.uniform(1 - 1e-4 * 5, 1 + 1e-4 * 5)  # 0.05% slip

        indices_and_prices = [
            (index, jitter_price(price))
            for index, *indicators in bars_df[strategy.buy_indicators].itertuples(name=None)  # twice faster than the default .itertuples options
            if (index[1] not in sold_or_holding_codes) and (price := strategy.should_buy(*indicators))
        ]

        if not indices_and_prices:
            return pd.DataFrame()

        indices, prices = zip(*indices_and_prices)
        return pd.DataFrame({
            "order_price": prices,
        }, index=pd.Index(indices, names=["timestamp", "code"]))

    def get_close_signals(self, df: pd.DataFrame, strategy: "StrategyBase") -> list[Any]:
        if not strategy.close_indicators:
            return []

        curr_positions = self.account.holdings.position_codes
        if not curr_positions:
            return []

        return [
            index
            for index, *indicators in df[strategy.close_indicators].itertuples(name=None)
            if (index[1] in curr_positions) and strategy.should_close(*indicators)
        ]

    def trade(self, df: pd.DataFrame, strategy: "StrategyBase") -> TradeBook:
        if list(getattr(df.index, "names", [])) != ["timestamp", "code"]:
            LOG.info('>>> 重建索引 [timestamp, code]')
            # Index by timestamp: trade in the day order
            #          code: look up the current price for positions in holding
            df.reset_index(inplace=True)
            df.set_index(["timestamp", "code"], inplace=True)
            df.sort_index(inplace=True)

        LOG.info('>>> 交易中 ...')
        trade_book = TradeBook()

        # Per day
        for timestamp, sub_df in tqdm(df.groupby(level="timestamp"), file=sys.stdout):
            assert isinstance(timestamp, str)

            # Opening
            price_lookup = lambda code: sub_df.loc[(timestamp, code), "close"]
            self.account.update_holdings(price_lookup)

            # Sell
            close_indices = self.get_close_signals(sub_df, strategy)
            sell_positions = []
            for code, pos in self.account.holdings:
                index = (timestamp, code)
                if index not in sub_df.index:
                    # Not a tradable day, so nothing to do
                    continue

                bar: BarDataType = sub_df.loc[index].to_dict()  # type: ignore

                # [1] Take profit
                if take_profit_price := strategy.should_take_profit(bar, pos):
                    pos.close(take_profit_price)
                    trade_book.take_profit(timestamp, pos)
                    sell_positions.append(pos)

                # [2] Stop loss
                elif stop_loss_price := strategy.should_stop_loss(bar, pos):
                    pos.close(stop_loss_price)
                    trade_book.stop_loss(timestamp, pos)
                    sell_positions.append(pos)

                # [3] Close
                elif index in close_indices:
                    pos.close(bar["close"])
                    trade_book.close_position(timestamp, pos)
                    sell_positions.append(pos)

            if sell_positions:
                self.account.sell(sell_positions)

            # Buy
            port_df = self.get_buy_options(sub_df, strategy, sell_positions)  # list[DF_Index, BuyPrice]
            if not port_df.empty:
                port_df, budget = strategy.adjust_portfolio_and_budget(
                    port_df=port_df,
                    budget=self.account.free_cash_amount,
                    n_stocks=len(sub_df),
                    total_asset_value=self.account.total_asset_value
                )

                buy_orders = strategy.generate_buy_orders(port_df, budget)
                buy_positions = self.__orders_to_positions(buy_orders)

                self.account.buy(buy_positions)
                for pos in buy_positions:
                    trade_book.buy(timestamp, pos)

            # Logging
            trade_book.log_capitals(
                timestamp,
                self.account.free_cash_amount,
                self.account.holdings.get_total_worth()
            )

        # That was quite a long story :D
        return trade_book

    def run(self, bars_df: pd.DataFrame, strategy: "StrategyBase") -> tuple[pd.DataFrame, TradeBook]:
        ind_df = strategy.compute_all_indicators_df(bars_df)
        trade_book = self.trade(ind_df, strategy)
        return ind_df, trade_book
