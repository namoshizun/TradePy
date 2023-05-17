from tradepy.core.position import Position
from tradepy.strategy.base import StrategyBase


Number = float | int


class TradeMixin:
    def should_take_profit(
        self, strategy: StrategyBase, bar: dict[str, Number], position: Position
    ) -> float | None:
        args = (bar[ind] for ind in strategy.take_profit_indicators)
        return strategy.should_take_profit(bar, position, *args)

    def should_stop_loss(
        self, strategy: StrategyBase, bar: dict[str, Number], position: Position
    ) -> float | None:
        args = (bar[ind] for ind in strategy.stop_loss_indicators)
        return strategy.should_stop_loss(bar, position, *args)
