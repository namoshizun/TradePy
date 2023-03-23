import uuid
from datetime import date
from typing import Literal
from pydantic import BaseModel

OrderDirection = Literal['buy', 'sell']

OrderStatus = Literal[
    'pending',
    'filled',
    'cancelled',
    'unknown',
    'invalid'
]


class Order(BaseModel):
    id: str | None = None  # Can be null when creating an order
    timestamp: str
    code: str
    price: float
    vol: int
    filled_price: float | None = None
    filled_vol: int | None = None
    status: OrderStatus = "pending"
    direction: Literal["buy", "sell"]

    @property
    def slip_points(self) -> float:
        assert self.filled_price is not None
        return self.price - self.filled_price

    @classmethod
    def fake(cls) -> "Order":
        return cls(
            id=str(uuid.uuid4()),
            timestamp=date.today().isoformat(),
            code="000333",
            direction="buy",
            price=50,
            vol=100,
        )

    def __str__(self) -> str:
        return f'[{self.timestamp}] {self.code} @{self.price} * {self.vol}. [{self.direction}, {self.status}]'
