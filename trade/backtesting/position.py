import uuid
from dataclasses import dataclass, field

from trade.utils import calc_pct_chg, round_val


@dataclass
class Position:

    timestamp: str
    code: str
    company: str
    price: float
    shares: int

    latest_price: float = field(init=False)
    id: str = field(init=False)
    days_held = 0

    def __post_init__(self):
        self.latest_price = self.price

        uuid_piece = str(uuid.uuid4()).split('-')[1]
        self.id = f'{self.company}-{uuid_piece}'

    @property
    @round_val
    def cost(self):
        return self.total_value_at(self.price)

    @round_val
    def total_value_at(self, price: float) -> float:
        return price * self.shares

    @round_val
    def profit_or_loss_at(self, price: float) -> float:
        return self.total_value_at(price) - self.cost

    @round_val
    def chg_at(self, price: float) -> float:
        return price - self.price

    @round_val
    def pct_chg_at(self, price: float) -> float:
        return calc_pct_chg(self.price, price)

    @round_val
    def price_at_pct_change(self, pct: float):
        return self.price * (1 + pct * 1e-2)

    def tick(self, price: float):
        self.days_held += 1
        self.latest_price = price
    
    def close(self, price: float):
        # NOTE: the actual closing price might be different from the daily close price, which is
        # used to update the latest price when the position is still in holding
        self.latest_price = price

    def __hash__(self):
        return hash(self.code)
    
    def __str__(self):
        pct_chg = self.pct_chg_at(self.latest_price)
        msg = f'[{self.timestamp} + {self.days_held or 0}] {self.code}: {self.price} ({pct_chg}%) * {self.shares}'
        return msg

    def __repr__(self):
        return str(self)
