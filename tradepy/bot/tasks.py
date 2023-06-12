import io
import pandas as pd
from datetime import datetime
from celery import shared_task
from loguru import logger

import tradepy
from tradepy.bot.broker import BrokerAPI
from tradepy.bot.engine import TradingEngine
from tradepy.core.exchange import AStockExchange
from tradepy.decorators import timeit
from tradepy.types import MarketPhase
from tradepy.bot.celery_app import app as celery_app
from tradepy.collectors.stock_day_bars import StockDayBarsCollector
from tradepy.collectors.market_index import (
    EastMoneySectorIndexCollector,
    BroadBasedIndexCollector,
)
from tradepy.collectors.adjust_factor import AdjustFactorCollector
from tradepy.collectors.release_restricted_shares import (
    EastMoneyRestrictedSharesReleaseCollector,
)


@shared_task(name="tradepy.warm_broker_db", expires=10)
def warm_broker_db():
    phase = AStockExchange.market_phase_now()
    if phase == MarketPhase.CLOSED:
        logger.warning("已休市，不预热数据库")
        return

    if (res := BrokerAPI.warm_db()) != '"ok"':
        logger.error(f"数据库预热失败, 可能导致未预期结果! {res}")

    logger.info("数据库预热成功")


@shared_task(
    name="tradepy.fetch_market_quote",
    expires=tradepy.config.trading.periodic_tasks.tick_fetch_interval * 0.95,
)
def fetch_market_quote():
    phase = AStockExchange.market_phase_now()
    if phase not in (
        MarketPhase.PRE_OPEN_CALL_P2,
        MarketPhase.CONT_TRADE,
        MarketPhase.CONT_TRADE_PRE_CLOSE,
    ):
        return

    with timeit() as timer:
        df = AStockExchange.get_quote()

    # Serialize the quote frame to a string and send it to the trading engine
    content_buff = io.StringIO()
    df.to_csv(content_buff)
    content_buff.seek(0)

    celery_app.send_task(
        "tradepy.handle_tick",
        kwargs=dict(
            payload={
                "timestamp": datetime.now(),
                "market_phase": phase,
                "market_quote": content_buff.read(),
            }
        ),
    )

    tradepy.LOG.info(f'行情获取API 耗时: {timer["seconds"]}s')


@shared_task(name="tradepy.handle_tick")
def handle_tick(payload):
    quote_df_reader = io.StringIO(payload["market_quote"])

    TradingEngine().handle_tick(
        market_phase=payload["market_phase"],
        quote_df=pd.read_csv(quote_df_reader, index_col="code", dtype={"code": str}),
    )


@shared_task(name="tradepy.flush_broker_cache")
def flush_broker_db():
    phase = AStockExchange.market_phase_now()
    if phase != MarketPhase.CLOSED:
        logger.warning("未休市, 不应将当日缓存导出到数据库")
        return

    if not AStockExchange.is_today_trade_day():
        logger.warning("非交易日, 不更新数据库")
        return

    if (res := BrokerAPI.flush_cache()) != '"ok"':
        logger.error(f"缓存落盘失败! {res}")
        return

    logger.info("缓存落盘成功")


@shared_task(name="tradepy.update_data_sources", expires=60 * 60)
def update_data_sources():
    StockDayBarsCollector().run(batch_size=25, iteration_pause=1)
    EastMoneySectorIndexCollector().run(start_date="2016-01-01")
    BroadBasedIndexCollector().run()
    AdjustFactorCollector().run()
    EastMoneyRestrictedSharesReleaseCollector().run(start_date="2016-01-01")
