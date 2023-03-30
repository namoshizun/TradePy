from broker_proxy.qmt.connector import xt_conn


class AssetsSyncer:

    def __init__(self) -> None:
        self.trader = xt_conn.get_trader()
        self.account = xt_conn.get_account()

    def run(self):
        ...
