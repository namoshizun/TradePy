import os
import sys
import random
from loguru import logger

from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount


class XtQuantConnection:

    def __init__(self) -> None:
        self._trader = self.init_trader()
        self._account = self.init_account()

    @property
    def data_path(self) -> str:
        return os.environ["XQUANT_QMT_DATA_PATH"]

    @property
    def account_id(self) -> str:
        return os.environ["XQUANT_ACCOUNT_ID"]

    def init_trader(self) -> XtQuantTrader:
        session_id = random.randint(100000, 999999)
        return XtQuantTrader(self.data_path, session_id)

    def init_account(self) -> StockAccount:
        return StockAccount(self.account_id)

    def get_account(self) -> StockAccount:
        return self._account

    def get_trader(self) -> XtQuantTrader:
        return self._trader

    def connect(self):
        logger.info("启动交易线程")
        self._trader.start()

        res = self._trader.connect()
        if res != 0:
            logger.error(f"连接失败, 程序即将退出。QMT数据文件路径: {self.data_path}. 账户ID: {self.account_id} 错误码: {res}")
            sys.exit(1)

    def disconnect(self):
        self._trader.stop()


xt_conn = XtQuantConnection()
