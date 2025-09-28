import sqlite3
import featherlog as fl
import pytest


def test_query(snapshot):
    x, y, z = fl.vars("x", "y", "z")
    conn = fl.Connection(sqlite3.connect(":memory:", autocommit=False))
    with conn.cursor() as cur:
        edge = cur.Relation("edge", x="INT", y="INT")
        path = cur.RelationSet("path", x="INT", y="INT")
        cur.insert(edge, [(1, 2), (2, 3), (3, 4), (4, 5), (5, 5)])
    with conn.cursor() as cur:
        clos = path(x, z) <= edge(x, z) | (edge(x, y) & path(y, z))
        cur.run(clos)
        cur.run(clos)
    with conn.cursor() as cur:
        assert list(cur.select([x, y], path(x, y))) == snapshot


def test_throw(snapshot):
    x, y, z = fl.vars("x", "y", "z")
    x, y, z = fl.vars("x", "y", "z")
    conn = fl.Connection(sqlite3.connect(":memory:", autocommit=False))
    with conn.cursor() as cur:
        edge = cur.Relation("edge", x="INT", y="INT")
        path = cur.RelationSet("path", x="INT", y="INT")
        cur.insert(edge, [(1, 2), (2, 3), (3, 4), (4, 5), (5, 5)])

    with pytest.raises(NotImplementedError):
        with conn.cursor() as cur:
            cur.run(path(x, y) <= edge(x, y))
            cur.run(path(x, z) <= edge(x, y) & path(y, z))
            raise NotImplementedError

    with conn.cursor() as cur:
        assert list(cur.select([x, y], path(x, y))) == snapshot
