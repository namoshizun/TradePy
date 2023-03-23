import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from broker_proxy.qmt.routes import router as api_router
from broker_proxy.qmt.connector import xt_conn


# TODO: The API Server should be moved to an upper level. The actual
# broker API router to be included should be specified in the env vars
app = FastAPI(title="QMT Broker Proxy")

app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        os.environ["TRADE_BOT_HOST"]
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(TrustedHostMiddleware, allowed_hosts=[
    os.environ["TRADE_BOT_HOST"]
])


@app.on_event("startup")
async def app_startup() -> None:
    xt_conn.connect()


@app.on_event("shutdown")
async def app_shutdown() -> None:
    xt_conn.disconnect()
