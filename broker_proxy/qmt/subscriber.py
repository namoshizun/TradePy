from loguru import logger
from xtquant.xttrader import XtQuantTraderCallback
from xtquant.xttype import XtOrder, XtAsset, XtTrade, XtPosition, XtOrderError

from broker_proxy.qmt.connector import xt_conn
# from tradepy.constants import CacheKeys


class DataSyncCallback(XtQuantTraderCallback):

    def on_disconnected(self):
        logger.error("连接断开")

    def on_stock_order(self, order: XtOrder):
        logger.info(f"委托状态跟新: {order.stock_code}, {order.order_status}, {order.order_sysid}")

    def on_stock_asset(self, asset: XtAsset):
        logger.info(f"资金变动: {asset.account_id}, {asset.cash}, {asset.total_asset}")

    def on_stock_trade(self, trade: XtTrade):
        logger.info(f"成交变动: {trade.account_id}, {trade.stock_code}, {trade.order_id}")

    def on_stock_position(self, position: XtPosition):
        logger.info(f"持仓变动: {position.stock_code}, {position.volume}")

    def on_order_error(self, order_error: XtOrderError):
        logger.info(f"委托失败: {order_error.order_id}, {order_error.error_id}, {order_error.error_msg}")


def terminate_connection():
    xt_conn.disconnect()
    logger.info("交易线程已停止")


if __name__ == "__main__":
    logger.add("xtquant_proxy_subscriber.log", rotation="1 day", retention="100 days")
    xt_conn.connect()

    # TODO: shutdown gracefully when terminated as a Windows service
    try:
        xt_conn.get_trader().run_forever()
    except KeyboardInterrupt:
        terminate_connection()
