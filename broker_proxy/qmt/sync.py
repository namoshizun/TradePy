from datetime import date, datetime
from loguru import logger

from broker_proxy.decorators import with_redis_lock
from broker_proxy.qmt.connector import xt_conn
from broker_proxy.cache import AccountCache, PositionCache, OrderCache, use_redis
from broker_proxy.qmt.conversion import xtorder_to_tradepy, xtposition_to_tradepy

import tradepy
from tradepy.constants import CacheKeys
from tradepy.core.models import Order, Position, Account
from tradepy.trade_book import TradeBook, CapitalsLog


class AssetsSyncer:

    def __init__(self) -> None:
        self.trader = xt_conn.get_trader()
        self.account = xt_conn.get_account()

    def fetch_counter_orders(self) -> list[Order]:
        xt_orders = self.trader.query_stock_orders(self.account)
        return list(map(xtorder_to_tradepy, xt_orders))

    def fetch_counter_positions(self) -> list[Position]:
        xt_positions = self.trader.query_stock_positions(self.account)
        return list(map(xtposition_to_tradepy, xt_positions))

    def _delete_expired_orders_cache(self, orders: dict[str, Order]):
        expiry, now = 10, datetime.now()  # seconds
        for oid in list(orders.keys()):
            o = orders[oid]
            if o.status == "created" and (now - o.created_at).seconds > expiry:
                logger.warning(f'委托缓存已过期: {o}. 正常情况下是因为无效订单')
                del orders[oid]

    def _update_orders_cache(self, orders: dict[str, Order]):
        for o in self.fetch_counter_orders():
            assert o.id
            if o.id not in orders:
                logger.warning(f'柜台返回了未缓存的委托: {o}')
            orders[o.id] = o
        OrderCache.set_many(orders.values())

    def _recalculate_positions(self, orders: dict[str, Order]) -> list[Position]:
        if not (positions := self.fetch_counter_positions()):
            return []

        for p in positions:
            p.vol, p.avail_vol = p.yesterday_vol, p.yesterday_vol
            for o in orders.values():
                if o.code == p.code:
                    if o.is_buy and o.is_filled:
                        p.vol += o.filled_vol  # type: ignore
                    elif o.is_sell:
                        p.avail_vol -= o.vol
                        if o.is_filled:
                            p.vol -= o.filled_vol  # type: ignore

                    assert p.vol >= 0 and p.avail_vol >= 0, f'持仓计算错误: {p}'
        PositionCache.set_many(positions)
        return positions

    def _recalculate_account_assets(self,
                                    orders: dict[str, Order],
                                    positions: list[Position],
                                    open_capitals: CapitalsLog):
        free_cash_amount, frozen_cash_amount = open_capitals["free_cash_amount"], 0
        for o in orders.values():
            if o.is_buy:
                free_cash_amount -= o.trade_value
                if not o.is_filled:
                    frozen_cash_amount += o.trade_value
            elif o.is_sell and o.is_filled:
                free_cash_amount += o.trade_value

        assert free_cash_amount >= 0 and frozen_cash_amount >= 0, f"资金计算错误: 可用 = {free_cash_amount}, 冻结 = {frozen_cash_amount}"
        AccountCache.set(Account(
            free_cash_amount=free_cash_amount,
            frozen_cash_amount=frozen_cash_amount,
            market_value=sum(p.total_value for p in positions),
        ))

    @with_redis_lock(CacheKeys.update_assets, timeout=10, sleep=0.2)
    def run(self):
        # Retrieve opening capitals
        trade_book = TradeBook.live_trading()
        today = date.today().isoformat()
        if not (open_capitals := trade_book.get_opening(today)):
            logger.error('未找到今日开盘资金数据, 无法计算资产数据!')
            return

        rd = tradepy.config.get_redis_client()
        with use_redis(rd.pipeline()):
            if not (temp := OrderCache.get_many()):
                logger.info('今日没有委托订单, 无须更新资产数据')
                return

            cached_orders: dict[str, Order] = {
                o.id: o
                for o in temp
            }  # type: ignore
            self._delete_expired_orders_cache(cached_orders)
            self._update_orders_cache(cached_orders)
            positions = self._recalculate_positions(cached_orders)
            self._recalculate_account_assets(cached_orders, positions, open_capitals)
