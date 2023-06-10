from tradepy.core.context import china_market_context
from tradepy.core.indicator import IndicatorSet
from playground.ma60_support_v1 import MA60SupportStrategyV1

ctx = china_market_context(
    stop_loss=2.5,
    take_profit=4,
    ma60_dist_thres=0.4,
    cash_amount=1e5,
    max_position_opens=15,
    max_position_size=0.2,
    signals_percent_range=(0, 2),
)
stra = MA60SupportStrategyV1(ctx)

indicators = MA60SupportStrategyV1.indicators_registry.get_specs(stra)
r = IndicatorSet(*indicators)
print(r.sort_by_execute_order(stra._required_indicators))
