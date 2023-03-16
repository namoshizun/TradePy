from celery import Celery
from kombu import Queue

import tradepy


tradepy.initialize("trading")


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
app.conf.broker_transport_options = {
    'visibility_timeout': 10  # default task visibility
}
app.conf.timezone = 'Asia/Shanghai'

app.autodiscover_tasks(packages=["tradepy"], related_name="bot")

app.conf.beat_schedule = {
    'market-tick': {
        'task': 'tradepy.fetch_market_quote',
        'schedule': tconf.tick_fetch_interval,
        'args': ()
    },
}
