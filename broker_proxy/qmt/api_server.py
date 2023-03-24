import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

import tradepy
from broker_proxy.qmt.routes import router as api_router
from broker_proxy.qmt.connector import xt_conn


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
    from broker_proxy.qmt.subscriber import CacheSyncCallback
    xt_conn.connect()
    xt_conn.subscribe(CacheSyncCallback())


@app.on_event("shutdown")
async def app_shutdown() -> None:
    xt_conn.disconnect()
    tradepy.config.exit()
