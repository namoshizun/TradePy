import io
import subprocess
import pandas as pd
from datetime import datetime
from celery import shared_task
from loguru import logger

import tradepy
from tradepy.constants import TRADABLE_PHASES, CacheKeys
from tradepy.bot.broker import BrokerAPI
from tradepy.bot.engine import TradingEngine
from tradepy.conversion import convert_code_to_market
from tradepy.core.exchange import AStockExchange
from tradepy.decorators import notify_failure, timeit
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
from tradepy.depot.stocks import StocksDailyBarsDepot
from tradepy.vendors.types import AskBid


@shared_task(name="tradepy.warm_database", expires=60 * 5)
@notify_failure(title="数据库预热失败")
def warm_database():
    phase = AStockExchange.market_phase_now()
    if phase == MarketPhase.CLOSED:
        logger.warning("已休市，不预热数据库")
        return

    if (res := BrokerAPI.warm_db()) != '"ok"':
        logger.error(f"交易端数据库预热失败, 可能导致未预期结果! {res}")
    logger.info("交易端数据库预热成功")

    logger.info("预加载策略端日K")
    engine = TradingEngine()
    df = StocksDailyBarsDepot.load(markets=tradepy.config.trading.markets)  # type: ignore
    df.to_pickle(engine.workspace_dir / f"{CacheKeys.hist_k}.pkl")
    logger.info("策略端日K预加载成功")


@shared_task(
    name="tradepy.fetch_market_quote",
    expires=tradepy.config.trading.periodic_tasks.tick_fetch_interval * 0.95,
)
@notify_failure(title="行情获取失败")
def fetch_market_quote():
    phase = AStockExchange.market_phase_now()
    if phase not in TRADABLE_PHASES:
        return

    with timeit() as timer:
        tradable_markets = tradepy.config.trading.markets  # type: ignore
        df = AStockExchange.get_quote()
        df["market"] = df.index.map(convert_code_to_market)
        df = df.query("market in @tradable_markets").copy()

    # Serialize the quote frame to a string and send it to the trading engine
    buff = io.StringIO()
    df.to_csv(buff)
    buff.seek(0)

    celery_app.send_task(
        "tradepy.handle_tick",
        kwargs=dict(
            payload={
                "timestamp": datetime.now(),
                "market_phase": phase,
                "market_quote": buff.read(),
            }
        ),
    )

    tradepy.LOG.info(f'行情获取API 耗时: {timer["seconds"]}s')


@shared_task(name="tradepy.handle_tick")
@notify_failure(title="交易引擎处理行情失败")
def handle_tick(payload):
    quote_df_reader = io.StringIO(payload["market_quote"])

    TradingEngine().handle_tick(
        market_phase=payload["market_phase"],
        quote_df=pd.read_csv(quote_df_reader, index_col="code", dtype={"code": str}),
    )


@shared_task(name="tradepy.cancel_expired_orders")
@notify_failure(title="交易引擎处理过期订单失败")
def cancel_expired_orders():
    pending_orders = []
    stock_ask_bids: dict[str, AskBid] = dict()

    for o in BrokerAPI.get_orders():
        if o.status != "cancelled" and o.pending_vol > 0:
            pending_orders.append(o)
            stock_ask_bids[o.code] = AStockExchange.get_bid_ask(o.code)

    if pending_orders:
        TradingEngine().handle_cancel_expired_orders(
            pending_orders=pending_orders,
            stock_ask_bids=stock_ask_bids,
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
@notify_failure(title="数据源更新失败")
def update_data_sources():
    StockDayBarsCollector().run(batch_size=25, iteration_pause=1)
    EastMoneySectorIndexCollector().run(start_date="2016-01-01")
    BroadBasedIndexCollector().run()
    AdjustFactorCollector().run()
    EastMoneyRestrictedSharesReleaseCollector().run(start_date="2016-01-01")


@shared_task(name="tradepy.vacuum", expires=60 * 60)
@notify_failure(title="数据清理失败")
def vacuum():
    for sub_command, days in [
        ["redis", tradepy.config.trading.cache_retention],
        ["workspace", tradepy.config.trading.cache_retention],
        ["database", tradepy.config.trading.indicators_window_size],
    ]:
        subprocess.run(
            ["python", "-m", "tradepy.cli.vacuum", sub_command, "--days", str(days)]
        )
