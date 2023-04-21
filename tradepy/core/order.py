import uuid
from loguru import logger
from datetime import date, datetime
from typing import Literal, TypedDict
from pydantic import BaseModel, Field

from tradepy.types import TradeActionType

OrderDirection = Literal['buy', 'sell']

OrderStatus = Literal[
    "created",
    'pending',
    'filled',
    'cancelled',
    'unknown',
    'invalid'
]


class SellRemark(TypedDict):
    action: TradeActionType
    price: float
    vol: int
    pct_chg: float


ACTION_TO_CODE: dict[TradeActionType, str] = {
    "平仓": "EX",
    "止盈": "TP",
    "止损": "SL",
}


class Order(BaseModel):
    id: str | None = None  # Can be null when creating an order
    timestamp: str
    code: str  # e.g., 000333
    price: float
    vol: int
    filled_price: float = 0
    filled_vol: int = 0
    status: OrderStatus = "created"
    direction: Literal["buy", "sell"]
    tags: dict = Field(default_factory=dict)

    @property
    def created_at(self) -> datetime:
        return datetime.fromisoformat(self.tags["created_at"])

    @property
    def cancelled_vol(self) -> int:
        if self.status == "cancelled":
            return self.vol - self.filled_vol
        return 0

    @property
    def filled_value(self) -> float:
        return self.filled_price * self.filled_vol

    @property
    def cancelled_value(self) -> float:
        if self.status == "cancelled":
            return self.placed_value - self.filled_value
        return 0

    @property
    def placed_value(self):
        return self.price * self.vol

    @property
    def is_buy(self) -> bool:
        return self.direction == "buy"

    @property
    def is_filled(self) -> bool:
        yes = self.status == "filled"
        if yes and (self.filled_price is None or self.filled_vol is None):
            raise ValueError('订单已成交, 但没有成交价格或成家笔数!')
        return yes

    @property
    def is_sell(self) -> bool:
        return self.direction == "sell"

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

    def serialize_tags(self) -> str:
        if not self.tags:
            return ''
        return ';'.join(f'{k}={v}' for k, v in self.tags.items())

    def set_sell_remark(self,
                        action: TradeActionType,
                        price: float,
                        vol: int,
                        pct_chg: float):
        action_code = ACTION_TO_CODE[action]
        self.tags["sell_remark"] = f'{action_code}:{price:.2f},{vol},{pct_chg:.2f}'

    def get_sell_remark(self, raw=True) -> SellRemark | str:
        if "sell_remark" not in self.tags:
            logger.warning(f'委托单 {self.id} 没有卖出备注')
            return ""

        remark_slug = self.tags["sell_remark"]
        assert isinstance(remark_slug, str), remark_slug
        if raw:
            return remark_slug

        return Order.parse_sell_remark(remark_slug)

    @staticmethod
    def parse_sell_remark(remark_slug: str) -> SellRemark:
        action_code, rest = remark_slug.split(':')
        price, vol, pct_chg = rest.split(',')
        action = {v: k for k, v in ACTION_TO_CODE.items()}[action_code]

        return {
            "action": action,  # type: ignore
            "price": float(price),
            "vol": int(vol),
            "pct_chg": float(pct_chg),
        }

    def __str__(self) -> str:
        return f'[{self.timestamp}] {self.code} @{self.price} * {self.vol}. [{self.direction}, {self.status}] ' + self.serialize_tags()
