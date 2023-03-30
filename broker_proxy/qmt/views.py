from datetime import datetime
from fastapi import APIRouter
from functools import wraps
from loguru import logger
from xtquant.xttype import XtOrder, XtPosition, XtAsset
from xtquant import xtconstant

from broker_proxy.cache import OrderCache, PositionCache, AccountCache, use_redis
from broker_proxy.qmt.connector import xt_conn
from broker_proxy.qmt.conversion import (
    xtorder_to_tradepy,
    xtposition_to_tradepy,
    xtaccount_to_tradepy,
    tradepy_order_direction_to_xtorder_status,
    tradepy_code_to_xtcode,
)

import tradepy
from tradepy.constants import CacheKeys
from tradepy.core.position import Position
from tradepy.core.order import Order
from tradepy.core.account import Account


router = APIRouter()


def use_cache(getter, setter):
    def decor(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            if (cached := getter()) is not None:
                return cached

            result = await func(*args, **kwargs)
            setter(result)
            return result
        return inner
    return decor


def with_lock(key: str, **lock_args):
    def decor(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            with tradepy.config.get_redis_client().lock(key, **lock_args):
                return await func(*args, **kwargs)
        return inner
    return decor


@router.get("/account", response_model=Account)
@use_cache(AccountCache.get, AccountCache.set)
async def get_account_info():
    logger.info("查询最新资产信息")
    trader = xt_conn.get_trader()
    account = xt_conn.get_account()
    assets: XtAsset | None = trader.query_stock_asset(account)

    if assets is None:
        logger.error('查询账户资产信息失败')
        return None

    return xtaccount_to_tradepy(assets)


@router.get("/positions", response_model=list[Position])
async def get_positions(available: bool = False):
    @use_cache(PositionCache.get_many, PositionCache.set_many)
    async def fetch():
        logger.info("查询并更新当前持仓")
        account = xt_conn.get_account()
        trader = xt_conn.get_trader()
        xt_positions: list[XtPosition] = trader.query_stock_positions(account)
        logger.info("完成")

        return [
            xtposition_to_tradepy(p)
            for p in xt_positions
        ]

    positions = await fetch()
    if available:
        return [p for p in positions if p.avail_vol > 0]

    return positions


@router.get("/orders", response_model=list[Order])
@use_cache(OrderCache.get_many, OrderCache.set_many)
async def get_orders():
    logger.info("查询并更新当前委托")
    account = xt_conn.get_account()
    trader = xt_conn.get_trader()
    orders: list[XtOrder] = trader.query_stock_orders(account)
    return [xtorder_to_tradepy(o) for o in orders]


@router.post("/orders")
@with_lock(CacheKeys.update_assets, sleep=0.25)
async def place_order(orders: list[Order]):
    logger.info("收到下单请求")

    xt_account = xt_conn.get_account()
    trader = xt_conn.get_trader()
    succ, fail = [], []
    rd = tradepy.config.get_redis_client()

    with use_redis(rd.pipeline()):
        # Pre-dudct account free cash
        buy_total = sum([round(o.price, 2) * o.vol for o in orders if o.direction == "buy"])
        if buy_total > 0:
            account: Account = AccountCache.get()  # type: ignore
            if buy_total > Account.free_cash_amount:
                logger.error(f'买入总额 = {buy_total}, 剩余可用金额 = {account.free_cash_amount}, 买入总额超过可用金额, 该委托预期将被拒绝')
            else:
                account.freeze_cash(buy_total)
                AccountCache.set(account)
                logger.info(f'买入总额 = {buy_total}, 剩余可用金额 = {account.free_cash_amount}')

        # Pre-deduct positions available positions
        sell_orders = [o for o in orders if o.direction == "sell"]
        positions: dict[str, Position] = {p.code: p for p in PositionCache.get_many()}  # type: ignore
        for order in sell_orders:
            if order.direction == "sell":
                if pos := positions.get(order.code):
                    if pos.avail_vol < order.vol:
                        logger.error(f'卖出数量超过可用数量: {order}, 该委托预期将被拒绝')
                    else:
                        pos.avail_vol -= order.vol

        # Create orders
        for order in orders:
            logger.info(f"提交委托: {order}")
            order_id = trader.order_stock(
                account=xt_account,
                stock_code=tradepy_code_to_xtcode(order.code),
                order_type=tradepy_order_direction_to_xtorder_status(order.direction),
                order_volume=order.vol,
                price_type=xtconstant.FIX_PRICE,
                price=round(order.price, 2),
            )

            if order_id == -1:
                logger.error(f'下单失败: {order}')
                fail.append(order.id)
            else:
                succ.append(order.id)
                logger.info(f"下单成功: {order}")

                order.id = str(order_id)
                order.tags = dict(created_at=datetime.now().isoformat())
                OrderCache.set(order)

        # Update positions if any of the orders is a sell order
        if sell_orders:
            PositionCache.set_many(list(positions.values()))

    return {
        "succ": succ,
        "fail": fail
    }
