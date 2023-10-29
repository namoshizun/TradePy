from typing import Literal


AskBid = dict[Literal["buy", "buy_vol", "sell", "sell_vol"], list[float]]
