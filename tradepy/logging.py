import os
import logging
import logging.handlers
from colorlog import ColoredFormatter

from tradepy.core.order import Order
from tradepy.core.position import Position


LOG_DIR = os.path.expanduser('~/.tradepy/logs')
LOG_FILENAME = os.path.join(LOG_DIR, 'tradepy.log')


if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)


class TradePyLogger:

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self.info = logger.info
        self.debug = logger.debug
        self.warn = logger.warn
        self.error = logger.error

    def log_orders(self, orders: list[Order]):
        self.info('\n' + '\n'.join([str(o) for o in orders]))

    def log_positions(self, positions: list[Position]):
        self.info('\n' + '\n'.join([str(p) for p in positions]))


class Logging:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._logger = None
        return cls._instance

    def get_logger(self) -> TradePyLogger:
        if self._logger is not None:
            return self._logger

        logger = logging.getLogger('tradepy')
        logger.setLevel(logging.DEBUG)

        # Set up colorlog formatter
        color_formatter = ColoredFormatter(
            fmt='%(log_color)s[%(module)s] [%(asctime)s] [%(levelname)s]: %(message)s%(reset)s')

        # Get root logger and add colorlog formatter to console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(color_formatter)
        console_handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(console_handler)

        # Set up file handler
        file_handler = logging.handlers.TimedRotatingFileHandler(
            LOG_FILENAME,
            when='D',
            interval=1,
            backupCount=365
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '[%(module)s] [%(asctime)s] [%(levelname)s]: %(message)s')

        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        self._logger = TradePyLogger(logger)
        return self._logger


LOG = Logging().get_logger()
