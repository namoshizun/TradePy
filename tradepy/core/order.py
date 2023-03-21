import uuid
from dataclasses import dataclass
from typing import Literal


OrderDirection = Literal['buy', 'sell']

OrderStatus = Literal['pending', 'filled', 'cancelled']


@dataclass
class Order:
    timestamp: str
    code: str
    company: str
    price: float
    shares: int
    direction: OrderDirection
    status: OrderStatus
    filled_price: float | None = None
    id: str = ""

    def __post_init__(self):
        if not self.id:
            uuid_piece = str(uuid.uuid4()).split('-')[1]
            self.id = f'{self.company}-{uuid_piece}'

    @property
    def slip_points(self) -> float:
        assert self.filled_price is not None
        return self.price - self.filled_price

    def __str__(self) -> str:
        return f'[{self.timestamp}] {self.company} @{self.price} * {self.shares}. [{self.direction}, {self.status}]'
