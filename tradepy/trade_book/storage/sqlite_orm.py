import sqlite3
from types import UnionType, NoneType
from typing import Any, Type, TypedDict, TypeVar, Generic, cast, get_args, get_origin
from typing_extensions import NotRequired


class TypeAnnotation:

    def __init__(self, annot: Type[Any]) -> None:
        self.anont = annot

    def infer_primitive_type(self):
        if self.is_primitive():
            return self.anont

        if self.is_union():
            types = get_args(self.anont)

            if len(types) == 0:
                return types[0]

            if len(types) == 2 and types[1] is NoneType:
                return types[0]

            raise TypeError(f'Cannot decide an unique primitive type for {self.anont}')

        if self.is_not_required():
            return TypeAnnotation(get_args(self.anont)[0]).infer_primitive_type()

        raise TypeError(f'Cannot infer primitive type for {self.anont}')

    def is_primitive(self):
        return get_origin(self.anont) is None

    def is_required(self):
        return not self.is_not_required()

    def is_not_required(self):
        return get_origin(self.anont) is NotRequired

    def is_nullable(self):
        if self.is_union():
            args = get_args(self.anont)
            return any(arg is NoneType for arg in args)

        if self.is_not_required():
            return True

        return False

    def is_union(self):
        return get_origin(self.anont) is UnionType


class Field:

    def __init__(self, name: str, descriptions: list[str]) -> None:
        self.name = name
        self.descriptions = descriptions

    def __str__(self) -> str:
        return f'{self.name} {" ".join(self.descriptions)}'

    def __repr__(self) -> str:
        return str(self)


RowDataType = TypeVar("RowDataType", bound=TypedDict)


class Table(Generic[RowDataType]):

    def __init__(self, table_name: str, fields: list[Field]) -> None:
        self.table_name: str = table_name
        self.fields: list[Field] = fields

    def __serialize(self, v) -> str:
        return f"'{v}'" if isinstance(v, str) else str(v)

    def __deserialize(self, row: list[Any]) -> RowDataType:
        return cast(RowDataType, {
            f.name: row[i]
            for i, f in enumerate(self.fields)
        })

    def schema(self) -> str:
        return self.table_name + "(\n" + ",\n".join(str(f) for f in self.fields) + ")"

    # CREATE -------
    def build_create_table_sql(self) -> str:
        return f'''
create table if not exists {self.schema()};
        '''

    def create_table(self, conn: sqlite3.Connection) -> sqlite3.Cursor:
        return conn.execute(self.build_create_table_sql())

    # DROP -------
    def build_drop_table_sql(self) -> str:
        return f"drop table if exists {self.table_name}"

    def drop_table(self, conn: sqlite3.Connection) -> sqlite3.Cursor:
        cursor = conn.execute(self.build_drop_table_sql())
        conn.commit()
        return cursor

    # INSERT -------
    def build_insert_sql(self, row: RowDataType) -> str:
        columns, values = [], []
        for col, val in row.items():
            columns.append(col)
            values.append(val)

        return f"""
insert into {self.table_name} ({", ".join(columns)})
values ({", ".join(map(self.__serialize, values))})
        """

    def insert(self, conn: sqlite3.Connection, row: RowDataType) -> sqlite3.Cursor:
        cursor = conn.execute(self.build_insert_sql(row).strip())
        conn.commit()
        return cursor

    # SELECT -------
    def build_select_sql(self, **query) -> str:
        return f"""
select * from {self.table_name}
where {" AND ".join(
    f"{col} = {self.__serialize(val)}" for col, val in query.items()
)}
        """

    def select(self, conn: sqlite3.Connection, **query) -> list[RowDataType]:
        cursor = conn.execute(self.build_select_sql(**query))
        rows = cursor.fetchall()
        return [self.__deserialize(row) for row in rows]

    # DELETE ------
    def build_delete_sql(self, **query) -> str:
        return f"""
delete from {self.table_name}
where {" AND ".join(
    f"{col} = {self.__serialize(val)}" for col, val in query.items()
)}
        """

    def delete(self, conn: sqlite3.Connection, **query) -> int:
        cursor = conn.execute(self.build_delete_sql(**query))
        return cursor.rowcount

    @classmethod
    def from_typed_dict(cls, typed_dict_type: Type[TypedDict]) -> "Table":
        fields = []
        type_to_sqlite_type = {
            str: "text",
            int: "integer",
            float: "real",
        }

        for name, annot in typed_dict_type.__annotations__.items():
            annot = TypeAnnotation(annot)
            python_type = annot.infer_primitive_type()
            sqlite_type = type_to_sqlite_type[python_type]

            descriptions = [sqlite_type]
            if annot.is_nullable():
                descriptions.append("NOT NULL")

            fields.append(Field(name, descriptions))

        return Table(
            table_name=typed_dict_type.__name__,
            fields=fields,
        )

    def __str__(self) -> str:
        return self.table_name + ": " + str(self.fields)

    def __repr__(self) -> str:
        return str(self)


if __name__ == '__main__':
    from tradepy.trade_book import TradeLog
    print('-' * 30)
    table: Table[TradeLog] = Table.from_typed_dict(TradeLog)
    print(table.schema())

    conn = sqlite3.connect("/tmp/test.db")
    print('-' * 30)
    print(table.build_create_table_sql())
    print(table.create_table(conn))

    print('-' * 30)
    row: TradeLog = {
        "timestamp": "2023-03-20",
        "action": "止损",
        "id": "1110",
        "code": "000333",
        "vol": 100,
        "price": 50,
        "total_value": 5000,
        "chg": 1,
        "pct_chg": 2,
        "total_return": 3,
    }
    print(table.build_insert_sql(row))
    print(table.insert(conn, row))

    print('-' * 30)
    query = {
        "timestamp": "2023-03-20",
        "code": "000333"
    }
    print(table.build_select_sql(**query))
    print(table.select(conn, **query))

    print('-' * 30)
    print(table.build_delete_sql(
        timestamp="2023-03-20",
    ))
    print(table.delete(conn, timestamp="2023-03-20"))

    print('-' * 30)
    print(table.build_drop_table_sql())
    print(table.drop_table(conn))

    conn.close()
