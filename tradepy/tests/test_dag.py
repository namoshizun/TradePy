from tradepy.core.context import china_market_context
from tradepy.strategy.ma60_support import MA60SupportStrategy
from tradepy.core.dag import IndicatorsResolver

ctx = ctx = china_market_context(
    stop_loss=2.5,
    take_profit=4,
    ma60_dist_thres=0.4,
    cash_amount=1e5,
    max_position_opens=15,
    max_position_size=0.2,
    signals_percent_range=(0, 2),
)
stra = MA60SupportStrategy(ctx)

indicators = MA60SupportStrategy.indicators_registry.get_specs(stra)
r = IndicatorsResolver(indicators)
r.sort_by_execute_order(stra._required_indicators)
# r.get_compute_order()
