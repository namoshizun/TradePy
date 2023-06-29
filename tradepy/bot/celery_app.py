from celery import Celery
from celery.schedules import crontab
from kombu import Queue

import tradepy

redis_conf = tradepy.config.common.redis
trade_conf = tradepy.config.trading
schedules_conf = tradepy.config.schedules

assert trade_conf and schedules_conf

app = Celery("tradepy-tradebot", fixups=[])

app.conf.broker_url = f"redis://:{redis_conf.password}@{redis_conf.host}:{redis_conf.port}/{redis_conf.db}"
app.conf.task_routes = {"tradepy.*": {"queue": "tradepy.tasks"}}
app.conf.task_queues = (Queue("tradepy.tasks"),)

app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 1
app.conf.timezone = "Asia/Shanghai"  # type: ignore

app.autodiscover_tasks(packages=["tradepy"], related_name="bot")

app.conf.beat_schedule = {
    "market-tick": {
        "task": "tradepy.fetch_market_quote",
        "schedule": trade_conf.periodic_tasks.tick_fetch_interval,
        "args": (),
    },
    "update-data-sources": {
        "task": "tradepy.update_data_sources",
        "schedule": crontab(
            **schedules_conf.parse_cron(schedules_conf.update_datasets)
        ),
        "args": (),
    },
    "warm-broker-db": {
        "task": "tradepy.warm_broker_db",
        "schedule": crontab(**schedules_conf.parse_cron(schedules_conf.warm_broker_db)),
        "args": (),
    },
    "flush-broker-cache": {
        "task": "tradepy.flush_broker_cache",
        "schedule": crontab(
            **schedules_conf.parse_cron(schedules_conf.flush_broker_cache)
        ),
        "args": (),
    },
}

if trade_conf.pending_order_expiry > 0:
    app.conf.beat_schedule["cancel-expired-orders"] = {
        "task": "tradepy.cancel_expired_orders",
        "schedule": trade_conf.periodic_tasks.cancel_expired_orders_interval,
        "args": (),
    }
