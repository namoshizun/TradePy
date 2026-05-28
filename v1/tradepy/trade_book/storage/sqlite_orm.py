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

            raise TypeError(f"Cannot decide an unique primitive type for {self.anont}")

        if self.is_not_required():
            return TypeAnnotation(get_args(self.anont)[0]).infer_primitive_type()

        raise TypeError(f"Cannot infer primitive type for {self.anont}")

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
        if isinstance(v, str):
            return f"'{v}'"
        if v is None:
            return "null"
        return str(v)

    def __deserialize(self, row: list[Any]) -> RowDataType:
        return cast(RowDataType, {f.name: row[i] for i, f in enumerate(self.fields)})

    def schema(self) -> str:
        return self.table_name + "(\n" + ",\n".join(str(f) for f in self.fields) + ")"

    # CREATE -------
    def build_create_table_sql(self) -> str:
        return f"""
create table if not exists {self.schema()};
        """

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
        select_clause = f"select * from {self.table_name}"
        if not query:
            return select_clause

        return select_clause + self.build_where_sql(**query)

    def build_where_sql(self, **query) -> str:
        return f"""
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
        delete_clause = f"delete from {self.table_name}"
        if not query:
            return delete_clause
        return delete_clause + self.build_where_sql(**query)

    def delete(self, conn: sqlite3.Connection, **query) -> int:
        cursor = conn.execute(self.build_delete_sql(**query))
        return cursor.rowcount

    # UPDATE ------
    def build_update_sql(self, where: dict[str, Any], update: dict[str, Any]) -> str:
        set_clause = ", ".join(
            f"{col} = {self.__serialize(val)}" for col, val in update.items()
        )
        update_clause = f"update {self.table_name} set {set_clause}"
        where_clause = self.build_where_sql(**where)
        return update_clause + where_clause

    def update(
        self, conn: sqlite3.Connection, where: dict[str, Any], update: dict[str, Any]
    ) -> int:
        cursor = conn.execute(self.build_update_sql(where, update))
        conn.commit()
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
            if not annot.is_nullable():
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
