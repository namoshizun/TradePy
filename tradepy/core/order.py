import uuid
from datetime import date, datetime
from typing import Literal
from pydantic import BaseModel, Field

OrderDirection = Literal['buy', 'sell']

OrderStatus = Literal[
    "created",
    'pending',
    'filled',
    'cancelled',
    'unknown',
    'invalid'
]


class Order(BaseModel):
    id: str | None = None  # Can be null when creating an order
    timestamp: str
    code: str  # e.g., 000333
    price: float
    vol: int
    filled_price: float | None = None
    filled_vol: int | None = None
    status: OrderStatus = "created"
    direction: Literal["buy", "sell"]
    tags: dict = Field(default_factory=dict)

    @property
    def created_at(self) -> datetime:
        return datetime.fromisoformat(self.tags["created_at"])

    @property
    def slip_points(self) -> float:
        assert self.filled_price is not None
        return self.price - self.filled_price

    @classmethod
    def fake(cls) -> "Order":
        return cls(
            id=cls.make_id("000333"),
            timestamp=date.today().isoformat(),
            code="000333",
            direction="buy",
            price=50,
            vol=100,
        )

    @staticmethod
    def make_id(code) -> str:
        return code + "-" + str(uuid.uuid4()).split('-')[1]

    def __str__(self) -> str:
        return f'[{self.timestamp}] {self.code} @{self.price} * {self.vol}. [{self.direction}, {self.status}]'
