import os
from loguru import logger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

import tradepy
from broker_proxy.qmt.views import router as api_router
from broker_proxy.qmt.connector import xt_conn
from broker_proxy.decorators import repeat_every
from broker_proxy.qmt.sync import AssetsSyncer
from tradepy.constants import CacheKeys
from tradepy.decorators import timeit
from tradepy.core.exchange import AStockExchange
from tradepy.types import MarketPhase


# TODO: The API Server should be moved to an upper level. The actual
# broker API router to be included should be specified in the env vars
app = FastAPI(title="QMT Broker Proxy")

app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "localhost",
        os.environ["TRADE_BROKER_HOST"]
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(TrustedHostMiddleware, allowed_hosts=[
    "localhost",
    os.environ["TRADE_BROKER_HOST"],
])


@app.on_event("startup")
async def app_startup() -> None:
    xt_conn.connect()


@app.on_event("shutdown")
async def app_shutdown() -> None:
    xt_conn.disconnect()
    tradepy.config.exit()


@app.on_event("startup")
@repeat_every(seconds=tradepy.config.assets_sync_interval)
async def sync_assets() -> None:
    not_trading = AStockExchange.market_phase_now() in (
        MarketPhase.CLOSED,
        MarketPhase.LUNCHBREAK,
        MarketPhase.PRE_OPEN,
    )
    if not_trading:
        return

    if not xt_conn.connected:
        logger.info('交易终端未连接, 无法同步资产信息')
        return

    if tradepy.config.get_redis_client().get(CacheKeys.update_assets):
        logger.warning('存在尚未完成的资产同步或下单任务')
        return

    with timeit() as timer:
        await AssetsSyncer().run()  # type: ignore

    logger.debug(f'资产同步完成, 耗时: {timer["millseconds"]}ms')
