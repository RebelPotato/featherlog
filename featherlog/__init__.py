from dataclasses import dataclass
from typing import Set, List, Dict, Tuple, Iterable, Any
import abc
import sqlite3
from contextlib import contextmanager
from functools import cached_property


@dataclass
class Cursor:
    cur: sqlite3.Cursor

    def Relation(self, name: str, *args: str, **kwargs: str) -> "Relation":
        p = Relation(name, make_args([*args], kwargs), distinct=False)
        self.create(p)
        return p

    def RelationSet(self, name: str, *args: str, **kwargs: str) -> "Relation":
        p = Relation(name, make_args([*args], kwargs), distinct=True)
        self.create(p)
        return p

    def create(self, rel: "Relation") -> sqlite3.Cursor:
        return self.cur.execute(rel.create_sql)

    def insert(self, rel: "Relation", rows: Iterable[Any]) -> sqlite3.Cursor:
        """Insert multiple rows into the relation."""
        return self.cur.executemany(rel.insert_sql, rows)

    def select(self, vars: List["Var"], stmt: "Query") -> sqlite3.Cursor:
        """Select variables using a query."""
        sql = stmt.sql
        code = f"SELECT {','.join(v.name for v in vars)}\nFROM ({sql.code})"
        return self.cur.execute(code, sql.args)

    def run(self, sql: "Sql") -> sqlite3.Cursor:
        return self.cur.execute(sql.code, sql.args)


@dataclass
class Connection:
    conn: sqlite3.Connection

    def close(self):
        self.conn.close()

    @contextmanager
    def cursor(self):
        cur = self.conn.cursor()
        try:
            yield Cursor(cur)
            self.conn.commit()
        except:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def __del__(self):
        self.close()  # Close connection to prevent ResourceWarning


@dataclass
class Sql:
    code: str
    args: List[Any]


@dataclass(frozen=True, order=True)
class Var:
    name: str


def vars(*names: str) -> List[Var]:
    return [Var(n) for n in names]


def make_args(args: List[str], kwargs: Dict[str, str]):
    return [(f"_x{i}", arg) for i, arg in enumerate(args)] + list(kwargs.items())


@dataclass
class Relation:
    name: str
    args: List[Tuple[str, str]]
    distinct: bool

    @cached_property
    def create_sql(self) -> str:
        cols = ", ".join(f"{k} {v}" for k, v in self.args)
        code = f"CREATE TABLE IF NOT EXISTS {self.name} ({cols}"
        if self.distinct:
            code += ", PRIMARY KEY (" + ", ".join(k for k, _ in self.args) + ")"
        code += ")"
        return code

    @cached_property
    def insert_sql(self) -> str:
        placeholders = ", ".join("?" for _ in self.args)
        code = f"INSERT INTO {self.name} VALUES ({placeholders})"
        if self.distinct:
            code += " ON CONFLICT DO NOTHING"
        return code

    def __call__(self, *args: Any, **kwargs: Any) -> "Single":
        """Create a query on the database."""
        assert len(args) + len(kwargs) == len(self.args), "Wrong number of arguments"
        values = list(args) + [kwargs[k] for k, _ in self.args[len(args) :]]
        return Single(self, values)


class Query(abc.ABC):
    @cached_property
    @abc.abstractmethod
    def cols(self) -> Set[Var]:
        pass

    @cached_property
    @abc.abstractmethod
    def sql(self) -> Sql:
        pass

    def __and__(self, other: "Query") -> "And":
        return And(self, other)

    def __or__(self, other: "Query") -> "Or":
        return Or(self, other)


@dataclass(frozen=True)
class And(Query):
    left: Query
    right: Query

    @cached_property
    def cols(self) -> Set[Var]:
        return self.left.cols | self.right.cols

    @cached_property
    def sql(self) -> Sql:
        left_cols = self.left.cols
        right_cols = self.right.cols
        on_clause = " AND ".join(
            f"_t1.{c.name} = _t2.{c.name}" for c in (left_cols & right_cols)
        )
        if on_clause != "":
            on_clause = "\nWHERE " + on_clause
        selects = [f"_t1.{c.name} AS {c.name}" for c in left_cols] + [
            f"_t2.{c.name} AS {c.name}" for c in right_cols if c not in left_cols
        ]
        left_sql = self.left.sql
        right_sql = self.right.sql
        code = Sql(
            f"SELECT {', '.join(selects)}\nFROM ({left_sql.code}) AS _t1,\n({right_sql.code}) AS _t2{on_clause}",
            left_sql.args + right_sql.args,
        )
        return code


@dataclass(frozen=True)
class Or(Query):
    left: Query
    right: Query

    @cached_property
    def cols(self) -> Set[Var]:
        return self.left.cols & self.right.cols

    @cached_property
    def sql(self) -> Sql:
        cols = self.cols
        names = ", ".join(c.name for c in cols)
        left_sql = self.left.sql
        right_sql = self.right.sql
        left_sql_code = f"SELECT {names} FROM ({left_sql.code})"
        right_sql_code = f"SELECT {names} FROM ({right_sql.code})"
        return Sql(
            f"SELECT {names}\nFROM ({left_sql_code}\nUNION ALL {right_sql_code})",
            left_sql.args + right_sql.args,
        )


@dataclass(frozen=True)
class Single(Query):
    rel: Relation
    values: List[Any]

    @cached_property
    def cols(self) -> Set[Var]:
        return set(a for a in self.values if isinstance(a, Var))

    @cached_property
    def sql(self) -> Sql:
        col_names = [k for k, _ in self.rel.args]
        min_arg = {
            a: min(j for j, b in enumerate(self.values) if a == b)
            for a in self.values
            if isinstance(a, Var)
        }
        wheres = [
            f"{col_names[i]} = {col_names[min_arg[a]]}"
            for i, a in enumerate(self.values)
            if isinstance(a, Var) and min_arg[a] != i
        ]
        value_args = [a for a in self.values if not isinstance(a, Var)]
        wheres += [f"{col_names[i]} = ?" for i in range(len(value_args))]
        wheres = "WHERE " + (" AND ".join(wheres) if len(wheres) > 0 else "true")
        selects = (
            ", ".join(f"{col_names[j]} AS {a.name}" for a, j in min_arg.items())
            if len(min_arg) > 0
            else "NULL"
        )
        return Sql(f"SELECT {selects}\nFROM {self.rel.name}\n{wheres}", value_args)

    def __le__(self, body: "Query") -> Sql:
        assert all(
            isinstance(a, Var) for a in self.values
        ), "All values of a query head must be variables"
        for v in self.values:
            assert v in body.cols, f"Variable {v.name} in head but not in body"
        selects = [a.name for a in self.values]
        body_sql = body.sql
        code = f"INSERT INTO {self.rel.name}\nSELECT {','.join(selects)}\nFROM ({body_sql.code}) WHERE true"
        if self.rel.distinct:
            code += " ON CONFLICT DO NOTHING"
        return Sql(code, body_sql.args)
