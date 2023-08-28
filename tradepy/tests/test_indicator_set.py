from tradepy.core.indicator import IndicatorSet
from tradepy.core.conf import StrategyConf
from playground.ma60_support_v1 import MA60SupportStrategyV1

conf = StrategyConf(
    stop_loss=2.5,
    take_profit=4,
    max_position_opens=15,
    max_position_size=0.2,
    custom_params={
        "ma60_dist_thres": 0.4,
    },
)


stra = MA60SupportStrategyV1(conf)
indicators = MA60SupportStrategyV1.indicators_registry.get_specs(stra)
r = IndicatorSet(*indicators)
print(r.sort_by_execute_order(stra._required_indicators))
