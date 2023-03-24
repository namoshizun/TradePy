from loguru import logger
from datetime import date, datetime
from xtquant.xttype import XtOrder, XtPosition, XtAsset
from xtquant import xtconstant

from tradepy.core.position import Position
from tradepy.core.order import Order, OrderDirection, OrderStatus
from tradepy.convertion import convert_code_to_exchange


def tradepy_order_direction_to_xtorder_status(dir: OrderDirection):
    match dir:
        case "buy":
            return xtconstant.STOCK_BUY
        case "sell":
            return xtconstant.STOCK_SELL


def xtorder_to_tradepy(o: XtOrder) -> Order:
    def to_direction(_type: int) -> OrderDirection:
        match _type:
            case xtconstant.STOCK_BUY:
                return "buy"
            case xtconstant.STOCK_SELL:
                return "sell"

        raise ValueError(f"Not recognized order type {_type}. No default can be decided")

    def to_status(_status: int) -> OrderStatus:
        cons = xtconstant

        match _status:
            case cons.ORDER_UNKNOWN:
                return "unknown"
            case cons.ORDER_JUNK:
                return 'invalid'
            case cons.ORDER_SUCCEEDED | cons.ORDER_PART_SUCC | cons.ORDER_PARTSUCC_CANCEL:
                return 'filled'
            case cons.ORDER_CANCELED | cons.ORDER_PART_CANCEL:
                return 'cancelled'
            case cons.ORDER_REPORTED_CANCEL | cons.ORDER_REPORTED | cons.ORDER_WAIT_REPORTING | cons.ORDER_UNREPORTED:
                return 'pending'

        logger.error(f'Not recognised order status type {_status}. Fall back to ORDER_KNOWN')
        return "unknown"

    return Order(
        id=str(o.order_id),
        timestamp=datetime.fromtimestamp(o.order_time).isoformat(),
        code=xtcode_to_tradepy_code(o.stock_code),
        price=o.price,
        vol=o.order_volume,
        filled_price=o.traded_price,
        filled_vol=o.traded_volume,
        status=to_status(int(o.order_status)),
        direction=to_direction(int(o.order_type)),
    )


def xtposition_to_tradepy(p: XtPosition) -> Position:
    curr_price = round(p.market_value / p.volume, 2)
    return Position(
        id=p.stock_code,
        timestamp=date.today().isoformat(),
        code=xtcode_to_tradepy_code(p.stock_code),
        price=p.open_price,
        latest_price=curr_price,
        vol=p.volume,
        avail_vol=p.can_use_volume,
    )


def xtaccount_to_tradepy(a: XtAsset) -> dict[str, float]:
    # TODO: design a common "Account" model
    return {
        "free_cash": a.cash,
        "frozen_cash": a.frozen_cash,
        "market_value": a.market_value
    }


def tradepy_code_to_xtcode(code: str) -> str:
    if "." in code:
        return code
    exchange = convert_code_to_exchange(code)
    return f'{code}.{exchange}'


def xtcode_to_tradepy_code(code: str) -> str:
    return code.split(".")[0]
