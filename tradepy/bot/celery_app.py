from celery import Celery
from celery.schedules import crontab
from kombu import Queue

import tradepy

crontab(hour=20, minute=0)

tradepy.initialize()


tconf = tradepy.config
app = Celery('tradepy', fixups=[])

app.conf.broker_url = f"redis://:{tconf.redis_password}@{tconf.redis_host}:{tconf.redis_port}/{tconf.redis_db}"
app.conf.task_routes = {
    'tradepy.*': {
        'queue': "tradepy.tasks"
    }
}
app.conf.task_queues = (Queue("tradepy.tasks"),)

app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 1
app.conf.timezone = 'Asia/Shanghai'

app.autodiscover_tasks(packages=["tradepy"], related_name="bot")

app.conf.beat_schedule = {
    'market-tick': {
        'task': 'tradepy.fetch_market_quote',
        'schedule': tconf.tick_fetch_interval,
        'args': ()
    },
    "update-data-sources": {
        "task": "tradepy.update_data_sources",
        "schedule": crontab(hour=20, minute=0),  # type: ignore
        "args": ()
    },
    "warm-broker-db": {
        "task": "tradepy.warm_broker_db",
        "schedule": crontab(hour=9, minute=0),  # type: ignore
        "args": ()
    },
    "flush-broker-cache": {
        "task": "tradepy.flush_broker_cache",
        "schedule": crontab(hour=15, minute=5),  # type: ignore
        "args": ()
    },
}
