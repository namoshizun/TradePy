from trade.stocks import StocksPool
from trade.client import TushareClientV1, TushareClientPro


v1_api: TushareClientV1 | None = None

pro_api : TushareClientPro | None = None

listing: StocksPool = StocksPool()


def init_clients(token=None):
    global v1_api, pro_api
    if not token:
        with open('token.txt', 'r') as fh:
            token = fh.read()

    v1_api = TushareClientV1()
    pro_api = TushareClientPro(token)
    print("Tushare clients initialized")
