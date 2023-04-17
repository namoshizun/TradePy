from collections import defaultdict
from datetime import datetime, date
from fastapi import APIRouter
from loguru import logger
from xtquant.xttype import XtOrder, XtPosition, XtAsset
from xtquant import xtconstant

from qmt_proxy.decorators import use_cache, with_redis_lock
from qmt_proxy.cache import OrderCache, PositionCache, AccountCache, use_redis
from qmt_proxy.connector import xt_conn
from qmt_proxy.conversion import (
    xtorder_to_tradepy,
    xtposition_to_tradepy,
    xtaccount_to_tradepy,
    tradepy_order_direction_to_xtorder_status,
    tradepy_code_to_xtcode,
)

import tradepy
from tradepy.core.order import SellRemark
from tradepy.trade_book import TradeBook
from tradepy.constants import CacheKeys
from tradepy.core.models import Position, Order, Account


router = APIRouter()


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
        logger.info("查询当前持仓")
        account = xt_conn.get_account()
        trader = xt_conn.get_trader()
        xt_positions: list[XtPosition] = trader.query_stock_positions(account)

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
    logger.info("查询当前委托")
    account = xt_conn.get_account()
    trader = xt_conn.get_trader()
    orders: list[XtOrder] = trader.query_stock_orders(account)
    return [xtorder_to_tradepy(o) for o in orders]


@router.post("/orders")
@with_redis_lock(CacheKeys.update_assets, sleep=0.2)
async def place_order(orders: list[Order]):
    logger.info("收到下单请求")

    xt_account = xt_conn.get_account()
    trader = xt_conn.get_trader()
    succ, fail = [], []

    with use_redis(tradepy.config.get_redis_client()):
        # Filter out buy orders whose volume is 500 (thanks to the weird behavior of QMT)
        if tradepy.config.mode == "mock-trading":
            n = len(orders)
            orders = [o for o in orders if o.vol != 500]
            m = len(orders)
            if n != m:
                logger.warning(f'过滤掉 {n - m} 个数量为 500 的委托')

        # Pre-dudct account free cash (buy orders)
        buy_total = sum([round(o.price, 2) * o.vol for o in orders if o.is_buy])
        if buy_total > 0:
            account: Account = AccountCache.get()  # type: ignore
            if buy_total > account.free_cash_amount:
                logger.error(f'买入总额 = {buy_total}, 剩余可用金额 = {account.free_cash_amount}, 买入总额超过可用金额, 该委托预期将被拒绝')
            else:
                account.freeze_cash(buy_total)
                AccountCache.set(account)
                logger.info(f'买入总额 = {buy_total}, 剩余可用金额 = {account.free_cash_amount}')

        # Pre-deduct positions available positions (sell orders)
        sell_orders = [o for o in orders if o.is_sell]
        positions: dict[str, Position] = {p.code: p for p in PositionCache.get_many()}  # type: ignore
        for order in sell_orders:
            if pos := positions.get(order.code):
                if pos.avail_vol < order.vol:
                    logger.error(f'卖出数量超过可用数量: {order}, 该委托预期将被拒绝')
                else:
                    pos.avail_vol -= order.vol

        if sell_orders:
            PositionCache.set_many(list(positions.values()))

        # Place orders
        for order in orders:
            logger.info(f"提交委托: {order}")
            order_id = trader.order_stock(
                account=xt_account,
                stock_code=tradepy_code_to_xtcode(order.code),
                order_type=tradepy_order_direction_to_xtorder_status(order.direction),
                order_volume=order.vol,
                price_type=xtconstant.FIX_PRICE,
                price=round(order.price, 2),
                order_remark=order.get_sell_remark(raw=True),  # type: ignore
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

    return {
        "succ": succ,
        "fail": fail
    }


@router.get("/control/warm-db")
async def warm_db():
    logger.info("预热缓存数据库")
    await get_orders()
    await get_positions()
    account = await get_account_info()
    assert account

    logger.info("预热SQL数据库")
    today = date.today().isoformat()
    trade_book = TradeBook.live_trading()
    trade_book.log_opening_capitals(today, account)

    logger.info("完成预热")
    return "ok"


@router.get("/control/flush-cache")
async def flush_cache():
    account = await get_account_info()
    today = date.today().isoformat()

    logger.info("导出资产总额")
    logger.info(account)
    trade_book = TradeBook.live_trading()
    trade_book.log_closing_capitals(today, account)

    logger.info("导出持仓变动记录")
    sell_orders: dict[str, list[Order]] = defaultdict(list)
    for order in await get_orders():
        if order.is_sell:
            sell_orders[order.code].append(order)

    positions = await get_positions()
    for pos in positions:
        if pos.is_new:
            logger.info(f'[开仓] {pos}')
            trade_book.buy(today, pos)

        elif pos.is_closed:
            orders = sell_orders[pos.code]
            if not orders:
                logger.error(f'未找到对应的卖出委托: {pos}')
                continue

            if len(orders) > 1:
                logger.error(f'找到多个对应的卖出委托: {pos}, {orders}')
                continue

            # Patch the position info
            order = orders[0]
            remark: SellRemark | None = order.get_sell_remark(raw=False)  # type: ignore
            if not remark:
                logger.error(f'未找到对应的卖出委托备注: {pos}, {order}')
                continue

            open_price = remark["price"] * (1 - remark["pct_chg"] * 1e-2)
            pos.price = open_price
            pos.latest_price = order.filled_price

            # Log it
            if remark["action"] == "平仓":
                logger.info(f'[平仓] {pos}')
                trade_book.close(today, pos)
            elif remark["action"] == "止损":
                logger.info(f'[止损] {pos}')
                trade_book.stop_loss(today, pos)
            elif remark["action"] == "止盈":
                logger.info(f'[止盈] {pos}')
                trade_book.take_profit(today, pos)
            else:
                logger.error(f'无法识别的卖出委托备注: {pos}, {order}')

    return "ok"


@router.get("/control/checks")
async def check():
    return {
        "today": date.today().isoformat(),
        "now": datetime.now().isoformat(),
        "cache_prefix": CacheKeys.prefix,
    }
