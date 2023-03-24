import os
import sys
import random
from loguru import logger

from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount


class XtQuantConnection:

    def __init__(self) -> None:
        self._trader = self.init_trader()
        self._account = self.init_account()

    @property
    def data_path(self) -> str:
        return os.environ["XTQUANT_QMT_DATA_PATH"]

    @property
    def account_id(self) -> str:
        return os.environ["XTQUANT_ACCOUNT_ID"]

    def init_trader(self) -> XtQuantTrader:
        session_id = random.randint(100000, 999999)
        return XtQuantTrader(self.data_path, session_id)

    def init_account(self) -> StockAccount:
        return StockAccount(self.account_id)

    def get_account(self) -> StockAccount:
        return self._account

    def get_trader(self) -> XtQuantTrader:
        return self._trader

    def subscribe(self, callback: XtQuantTraderCallback):
        self._trader.register_callback(callback)
        res = self._trader.subscribe(self._account)
        if res != 0:
            logger.error(f'订阅交易主推失败: {res}')
        logger.info(f"订阅交易主推成功: {res}")

    def connect(self):
        logger.info("启动交易线程")
        self._trader.start()

        res = self._trader.connect()
        if res != 0:
            logger.error(f"连接失败, 程序即将退出。QMT数据文件路径: {self.data_path}. 账户ID: {self.account_id} 错误码: {res}")
            sys.exit(1)

    def disconnect(self):
        logger.info("关闭交易线程")
        self._trader.stop()


xt_conn = XtQuantConnection()
