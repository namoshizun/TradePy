from pydantic import BaseModel

from tradepy.core.order import Order
from tradepy.utils import calc_pct_chg, round_val


class Position(BaseModel):

    id: str
    timestamp: str
    code: str
    price: float
    vol: int
    latest_price: float
    avail_vol: int
    yesterday_vol: int = 0

    def to_sell_order(self, timestamp) -> Order:
        assert self.latest_price, f'Position is not yet closed: {self}'
        return Order(
            id=Order.make_id(self.code),
            timestamp=timestamp,
            code=self.code,
            price=self.latest_price,
            vol=self.avail_vol,
            direction="sell",
            status="pending"
        )

    @property
    @round_val
    def cost(self):
        return self.total_value_at(self.price)

    @round_val
    def total_value_at(self, price: float) -> float:
        return price * self.vol

    @round_val
    def profit_or_loss_at(self, price: float) -> float:
        return self.total_value_at(price) - self.cost

    @round_val
    def chg_at(self, price: float) -> float:
        return price - self.price

    @round_val
    def pct_chg_at(self, price: float) -> float:
        if self.chg_at(price) == 0:
            return 0
        return calc_pct_chg(self.price, price)

    @round_val
    def price_at_pct_change(self, pct: float):
        return self.price * (1 + pct * 1e-2)

    def update_price(self, price: float):
        self.latest_price = price

    def close(self, price: float):
        # NOTE: the actual closing price might be different from the daily close price, which is
        # used to update the latest price when the position is still in holding
        self.latest_price = price

    def __hash__(self):
        return hash(self.code)

    def __str__(self):
        pct_chg = self.pct_chg_at(self.latest_price)
        msg = f'[{self.timestamp}] {self.code}: {self.price} ({pct_chg}%) * {self.vol}'
        return msg

    def __repr__(self):
        return str(self)
