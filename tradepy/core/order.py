from typing import Literal
from pydantic import BaseModel

OrderDirection = Literal['buy', 'sell']

OrderStatus = Literal['pending', 'filled', 'cancelled']


class Order(BaseModel):
    id: str
    timestamp: str
    code: str
    price: float
    filled_price: float | None = None
    vol: int
    status: OrderStatus
    direction: Literal["buy", "sell"]

    @property
    def slip_points(self) -> float:
        assert self.filled_price is not None
        return self.price - self.filled_price

    def __str__(self) -> str:
        return f'[{self.timestamp}] {self.code} @{self.price} * {self.vol}. [{self.direction}, {self.status}]'
