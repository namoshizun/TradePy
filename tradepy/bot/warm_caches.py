from celery import shared_task
from tradepy.bot.broker import BrokerAPI


@shared_task(name="tradepy.warm_broker_caches", expires=10)
def warm_broker_caches():
    BrokerAPI.get_orders()
    BrokerAPI.get_positions()
    BrokerAPI.get_account()
