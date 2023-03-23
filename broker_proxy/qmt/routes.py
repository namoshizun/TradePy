from fastapi import APIRouter
from loguru import logger
from xtquant.xttype import XtOrder, XtPosition, XtAsset
from xtquant import xtconstant

from broker_proxy.qmt.connector import xt_conn
from broker_proxy.qmt.conversion import (
    xtorder_to_tradepy,
    xtposition_to_tradepy,
    xtaccount_to_tradepy,
    tradepy_order_direction_to_xtorder_status
)
from tradepy.core.position import Position
from tradepy.core.order import Order


router = APIRouter()


@router.get("/positions", response_model=list[Position])
async def get_positions():
    account = xt_conn.get_account()
    trader = xt_conn.get_trader()
    positions: list[XtPosition] = trader.query_stock_positions(account)
    return [xtposition_to_tradepy(p) for p in positions]


@router.get("/orders", response_model=list[Order])
async def get_orders():
    account = xt_conn.get_account()
    trader = xt_conn.get_trader()
    orders: list[XtOrder] = trader.query_stock_orders(account)

    return [xtorder_to_tradepy(o) for o in orders]


@router.post("/orders", response_model=Order)
async def place_order(order: Order):
    account = xt_conn.get_account()
    trader = xt_conn.get_trader()
    order_id = trader.order_stock(
        account=account,
        stock_code=order.code,
        order_type=tradepy_order_direction_to_xtorder_status(order.direction),
        order_volume=order.vol,
        price_type=xtconstant.FIX_PRICE,
        price=order.price,
    )

    if order_id == -1:
        logger.error(f'Place order failed! Order request received {order}')
        return None

    xtorder: XtOrder | None = trader.query_stock_order(account, order_id)
    if xtorder is None:
        logger.error(f'Order was successsfully placed but the order query failed! Order id = {order_id}')
        return None

    return xtorder_to_tradepy(xtorder)


@router.get("/account")
async def get_account_info():
    trader = xt_conn.get_trader()
    account = xt_conn.get_account()
    assets: XtAsset | None = trader.query_stock_asset(account)

    if assets is None:
        logger.error('Query account stock asset failed')
        return None

    assert isinstance(assets, XtAsset)
    return xtaccount_to_tradepy(assets)
