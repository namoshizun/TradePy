from fastapi import APIRouter
from functools import wraps
from loguru import logger
from xtquant.xttype import XtOrder, XtPosition, XtAsset
from xtquant import xtconstant

from broker_proxy.cache import OrderCache
from broker_proxy.qmt.connector import xt_conn
from broker_proxy.qmt.conversion import (
    xtorder_to_tradepy,
    xtposition_to_tradepy,
    xtaccount_to_tradepy,
    tradepy_order_direction_to_xtorder_status,
    tradepy_code_to_xtcode,
)
from tradepy.core.position import Position
from tradepy.core.order import Order


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


@router.get("/account")
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
    logger.info("查询并更新当前持仓")
    account = xt_conn.get_account()
    trader = xt_conn.get_trader()
    xt_positions: list[XtPosition] = trader.query_stock_positions(account)
    logger.info("完成")

    positions = [
        xtposition_to_tradepy(p)
        for p in xt_positions
    ]

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
async def place_order(orders: list[Order]):
    logger.info("收到下单请求")
    account = xt_conn.get_account()
    trader = xt_conn.get_trader()

    succ, fail = [], []
    for order in orders:
        logger.info(f"提交委托: {order}")
        order_id = trader.order_stock(
            account=account,
            stock_code=tradepy_code_to_xtcode(order.code),
            order_type=tradepy_order_direction_to_xtorder_status(order.direction),
            order_volume=order.vol,
            price_type=xtconstant.FIX_PRICE,
            price=order.price,
        )

        if order_id == -1:
            logger.error(f'下单失败: {order}')
            fail.append(order.id)
            continue

        succ.append(order.id)
        logger.info(f"下单成功: {order}")

    return {
        "succ": succ,
        "fail": fail
    }
