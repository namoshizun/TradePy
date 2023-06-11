import tradepy
from tradepy.collectors import DataCollector
from tradepy.depot.misc import RestrictedSharesReleaseDepot


class EastMoneyRestrictedSharesReleaseCollector(DataCollector):
    def run(self, start_date: str, end_date: str | None = None):
        df = tradepy.ak_api.get_restricted_releases(start_date, end_date)
        df.to_csv(out_path := RestrictedSharesReleaseDepot.file_path())
        tradepy.LOG.info(f"已下载至 {out_path}")
