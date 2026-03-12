# prus

Python bindings for querying StarRocks and exporting Arrow data to JSON.

## JSON Export

`write_arrow_ndjson` writes one JSON object per line.

```python
import pyarrow as pa
import prus

table = pa.table(
    {
        "id": [1, 2],
        "name": ["alice", "bob"],
    }
)

prus.write_arrow_ndjson(table, "rows.json")
```

`write_arrow_json_array` writes a single JSON array.

```python
import pyarrow as pa
import prus

batch = pa.record_batch(
    [
        pa.array([1, 2]),
        pa.array(["alice", "bob"]),
    ],
    names=["id", "name"],
)

prus.write_arrow_json_array(batch, "rows_array.json")
```

You can also use the generic interface:

```python
prus.write_arrow_json(table, "rows.json", lines=True)
prus.write_arrow_json(table, "rows_array.json", lines=False)
```

To return JSON as a string instead of writing a file:

```python
import prus

json_lines = prus.dumps_arrow_ndjson(table)
json_array = prus.dumps_arrow_json_array(table)

# Generic interface
json_text = prus.dumps_arrow_json(table, lines=False)
```

## StarRocks Query

`query_starrocks_arrow` returns a `pyarrow.Table`.

```python
import prus

table = prus.query_starrocks_arrow(
    base_url="http://127.0.0.1:8030",
    database="test",
    query="select * from test_types limit 10",
    username="root",
    password="",
    catalog="default_catalog",
)

print(table)
```

You can write the query result directly to JSON:

```python
import prus

table = prus.query_starrocks_arrow(
    base_url="http://127.0.0.1:8030",
    database="test",
    query="select * from test_types limit 10",
)

prus.write_arrow_ndjson(table, "starrocks_rows.json")
prus.write_arrow_json_array(table, "starrocks_rows_array.json")
```
