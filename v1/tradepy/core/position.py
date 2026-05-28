from pydantic import BaseModel

from tradepy.core.order import Order
from tradepy.types import TradeActionType
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

    def to_sell_order(self, timestamp, action: TradeActionType) -> Order:
        assert (
            self.avail_vol
        ), f"Position does not have available volume for sell: {self}"
        price = self.latest_price
        order = Order(
            id=Order.make_id(self.code),
            timestamp=timestamp,
            code=self.code,
            price=price,
            vol=self.avail_vol,
            direction="sell",
            status="pending",
        )

        pct_chg = self.pct_chg_at(price)
        order.set_sell_remark(action, price, self.avail_vol, pct_chg)
        return order

    @property
    @round_val
    def cost(self):
        return self.total_value_at(self.price)

    @property
    @round_val
    def total_value(self) -> float:
        return self.total_value_at(self.latest_price)

    @property
    @round_val
    def yesterday_total_value(self) -> float:
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

    @property
    def is_closed(self) -> bool:
        return self.vol == 0 and self.avail_vol == 0

    @property
    def is_new(self) -> bool:
        return self.yesterday_vol == 0

    def __hash__(self):
        return hash(self.code)

    def __str__(self):
        pct_chg = self.pct_chg_at(self.latest_price)
        msg = f"[{self.timestamp}] {self.code}: {self.price} ({pct_chg}%) * {self.vol} ({self.avail_vol}可用, {self.yesterday_vol}隔夜)"
        return msg

    def __repr__(self):
        return str(self)
