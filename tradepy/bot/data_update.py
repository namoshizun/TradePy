from celery import shared_task

from tradepy.collectors.day_bars import StockDayBarsCollector
from tradepy.collectors.market_index import EastMoneySectorIndexCollector, BroadBasedIndexCollector
from tradepy.collectors.adjust_factor import AdjustFactorCollector


@shared_task(name="tradepy.update_data_sources", expires=60 * 60)
def update_data_sources():
    StockDayBarsCollector().run(batch_size=30, iteration_pause=3)
    EastMoneySectorIndexCollector().run(since_date="2016-01-01")
    BroadBasedIndexCollector().run()
    AdjustFactorCollector().run()
