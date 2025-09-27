# Featherlog.py

Datalog for SQLite in python. It is:

- Simple, just a thin layer over sqlite3.
- Practical, represents a large subset of SQL
- Pretty, almost valid datalog

Here's an example:

```python
edge = fl.Relation("edge", x="INT", y="INT")
path = fl.RelationSet("path", x="INT", y="INT")
x, y, z = fl.vars("x", "y", "z")
with conn.cursor() as cur:
    cur.insert(edge, [(1, 2), (2, 3), (3, 4), (4, 5), (5, 5)])
    clos = path(x, z) <= edge(x, z) | (edge(x, y) & path(y, z))
    [cur.run(clos) for _ in range(2)]
    assert (3, 5) in cur.select([x, y], path(x, y))
```

This implementation is based on techniques from [Philip Zucker's blog post](https://www.philipzucker.com/compose_datalog/).
