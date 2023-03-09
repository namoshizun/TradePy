from tradepy.backtesting.context import china_market_context
from tradepy.strategy.ma60_support_v1 import MA60SupportStrategyV1
from tradepy.backtesting.dag import IndicatorsResolver


ctx = china_market_context(stop_loss=1, take_profit=1, cash_amount=1)
st = MA60SupportStrategyV1(ctx)
resolv = IndicatorsResolver(st)

for ind, pred in resolv.get_compute_order().items():
    print(f'{ind}: {pred}')
