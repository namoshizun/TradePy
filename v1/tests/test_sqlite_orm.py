import sqlite3
from typing import TypedDict
import pytest
from typing_extensions import NotRequired
from tradepy.trade_book.storage.sqlite_orm import (
    Table,
)


class UserActivity(TypedDict):
    name: str
    age: NotRequired[int | None]
    nickname: NotRequired[str | None]
    timestamp: str
    action: str


@pytest.fixture(scope="class")
def table_instance():
    table = Table.from_typed_dict(UserActivity)
    conn = sqlite3.connect(":memory:")
    table.create_table(conn)

    yield table, conn
    conn.close()


@pytest.mark.usefixtures("table_instance")
class TestSQLiteORMTable:
    def test_insert_and_select(self, table_instance):
        table, conn = table_instance

        # Insert a row
        row = {
            "timestamp": "2023-03-20 10:00:00",
            "action": "login",
            "name": "John",
            "nickname": None,
            "age": 10,
        }
        table.insert(conn, row)

        # Select the inserted row
        query = {"timestamp": "2023-03-20 10:00:00", "name": "John"}
        selected_rows = table.select(conn, **query)

        assert len(selected_rows) == 1
        assert selected_rows[0] == row

    def test_update(self, table_instance):
        table, conn = table_instance

        # Insert a row
        row = {
            "timestamp": "2023-03-21 10:15:00",
            "action": "logout",
            "name": "John",
        }
        table.insert(conn, row)

        # Update the inserted row
        update_args = {
            "where": row,
            "update": {"action": "post"},
        }
        updated_count = table.update(conn, **update_args)
        assert updated_count == 1

        # Verify the updated row
        selected_rows = table.select(conn, timestamp=row["timestamp"], name=row["name"])

        assert len(selected_rows) == 1
        assert selected_rows[0]["action"] == "post"

    def test_delete(self, table_instance):
        table, conn = table_instance

        # Insert a row
        row = {
            "timestamp": "2023-03-22 10:15:00",
            "action": "logout",
            "name": "John",
        }
        table.insert(conn, row)

        # Delete the inserted row
        delete_args = {"timestamp": row["timestamp"]}
        deleted_count = table.delete(conn, **delete_args)

        assert deleted_count == 1

        # Verify that the row has been deleted
        selected_rows = table.select(conn, **row)
        assert len(selected_rows) == 0

    def test_drop_table(self, table_instance):
        table, conn = table_instance

        # Drop the table
        table.drop_table(conn)

        # Verify that the table has been dropped
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table.table_name,),
        )
        result = cursor.fetchone()

        assert result is None

    def test_schema(self, table_instance):
        table, _ = table_instance
        expected_schema = (
            "UserActivity(\n"
            "name text NOT NULL,"
            "age integer,"
            "nickname text,"
            "timestamp text NOT NULL,"
            "action text NOT NULL)"
        )

        assert table.schema().replace("\n", "") == expected_schema.replace("\n", "")
