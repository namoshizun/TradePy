import pytest
import tempfile

from tradepy.core.models import Position, Account
from tradepy.trade_book import TradeBook
from tradepy.trade_book.storage import SQLiteTradeBookStorage, InMemoryTradeBookStorage
from tradepy.types import TradeActions


@pytest.fixture
def sqlite_db_location():
    with tempfile.NamedTemporaryFile() as f:
        yield f.name


@pytest.fixture
def in_memory_trade_book():
    return TradeBook(InMemoryTradeBookStorage())


@pytest.fixture
def sqlite_trade_book(sqlite_db_location):
    return TradeBook(SQLiteTradeBookStorage(sqlite_db_location))


@pytest.fixture
def sample_position():
    return Position(
        id="1",
        timestamp="2023-03-05",
        code="000333",
        vol=100,
        price=100.0,
        latest_price=115.0,
        avail_vol=100,
        yesterday_vol=100,
    )


@pytest.fixture
def sample_account():
    return Account(
        frozen_cash_amount=1000.0, market_value=5000.0, free_cash_amount=4000.0
    )


def test_backtest_initialization():
    trade_book = TradeBook.backtest()
    assert isinstance(trade_book, TradeBook)
    assert isinstance(trade_book.storage, InMemoryTradeBookStorage)


def test_live_trading_initialization(sqlite_db_location):
    trade_book = TradeBook.live_trading(sqlite_db_location)
    assert isinstance(trade_book, TradeBook)
    assert isinstance(trade_book.storage, SQLiteTradeBookStorage)


def test_trade_book_clone(
    in_memory_trade_book: TradeBook, sample_account: Account, sample_position: Position
):
    in_memory_trade_book.log_closing_capitals(date="2023-09-17", account=sample_account)
    in_memory_trade_book.buy(
        timestamp="2023-09-17 10:00:00",
        pos=sample_position,
    )
    clone = in_memory_trade_book.clone()

    assert type(clone.storage) is type(in_memory_trade_book.storage)
    assert clone.cap_logs_df.equals(in_memory_trade_book.cap_logs_df)
    assert clone.trade_logs_df.equals(in_memory_trade_book.trade_logs_df)


@pytest.mark.parametrize(
    "trade_book_fixture", ["in_memory_trade_book", "sqlite_trade_book"]
)
@pytest.mark.parametrize("pct_chg", [-5, 0, 5])
def test_buy_and_sell(
    trade_book_fixture: str, sample_position: Position, pct_chg: int, request
):
    # Buy
    buy_timestamp = "2023-09-17 10:00:00"
    trade_book: TradeBook = request.getfixturevalue(trade_book_fixture)
    trade_book.buy(buy_timestamp, sample_position)

    # Sell
    sell_timestamp = "2023-09-18 15:00:00"
    sell_price = sample_position.price_at_pct_change(pct_chg)
    sample_position.update_price(sell_price)
    trade_book.sell(sell_timestamp, sample_position, "平仓")

    # Check sell record
    trade_logs_df = trade_book.trade_logs_df
    assert len(trade_logs_df) == 2
    sell_record = trade_logs_df.query("timestamp == @sell_timestamp").iloc[0]
    assert sell_record["action"] == TradeActions.CLOSE
    assert sell_record["price"] == sell_price
    assert sell_record["total_value"] == sell_price * sample_position.vol
    assert sell_record["pct_chg"] == pct_chg


def test_sqlite_tradebook_log_capitals(
    sqlite_trade_book: TradeBook, sample_account: Account
):
    date = "2023-09-17"

    # Log the opening capitals
    sqlite_trade_book.log_opening_capitals(date, sample_account)

    # Change the account balance, and log the closing capitals
    expect_closing_free_cash = (
        sample_account.free_cash_amount + sample_account.frozen_cash_amount
    )

    sample_account.free_cash(sample_account.frozen_cash_amount)
    sqlite_trade_book.log_closing_capitals(date, sample_account)

    # Check the final capitals
    capitals_df = sqlite_trade_book.cap_logs_df
    assert len(capitals_df) == 1
    assert str(capitals_df.iloc[0].name.date()) == date  # type: ignore
    assert capitals_df.iloc[0]["free_cash_amount"] == expect_closing_free_cash
