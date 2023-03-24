from loguru import logger
from xtquant.xttrader import XtQuantTraderCallback
from xtquant.xttype import XtOrder, XtAsset, XtPosition, XtOrderError

import tradepy
from tradepy.core.order import Order
from tradepy.core.position import Position
from broker_proxy.qmt.connector import xt_conn
from broker_proxy.qmt import conversion as xt_convert
from broker_proxy import cache


class CacheSyncCallback(XtQuantTraderCallback):

    def __init__(self) -> None:
        self.redis = tradepy.config.get_redis_client()

    def on_disconnected(self):
        logger.error("连接断开")

    def on_stock_order(self, xt_order: XtOrder):
        logger.info(f"委托状态更新: {xt_order.stock_code}, {xt_order.order_status}, {xt_order.order_sysid}")
        order: Order = xt_convert.xtorder_to_tradepy(xt_order)
        cache.OrderCache.set(order)

    def on_stock_asset(self, asset: XtAsset):
        logger.info(f"资金变动: {asset.account_id}, {asset.cash}, {asset.total_asset}")
        account = xt_convert.xtaccount_to_tradepy(asset)
        cache.AccountCache.set(account)

    def on_stock_position(self, xt_position: XtPosition):
        logger.info(f"持仓变动: {xt_position.stock_code}, {xt_position.volume}")
        position: Position = xt_convert.xtposition_to_tradepy(xt_position)
        cache.PositionCache.set(position)

    def on_order_error(self, order_error: XtOrderError):
        logger.info(f"委托失败: {order_error.order_id}, {order_error.error_id}, {order_error.error_msg}, {order_error.order_remark}")

        order = cache.OrderCache.get(order_error.order_id)
        if not order:
            logger.error('缓存中找不到该委托信息')
            return

        order.status = "invalid"
        cache.OrderCache.set(order)


def terminate_connection():
    xt_conn.disconnect()
    logger.info("交易线程已停止")


if __name__ == "__main__":
    logger.add("xtquant_proxy_subscriber.log", rotation="1 day", retention="100 days")
    xt_conn.connect()
    xt_conn.subscribe(CacheSyncCallback())

    # TODO: shutdown gracefully when terminated as a Windows service
    try:
        xt_conn.get_trader().run_forever()
    except KeyboardInterrupt:
        terminate_connection()
