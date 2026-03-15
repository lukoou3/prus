# prus

High-performance Python extension for data I/O with ClickHouse, StarRocks, and Kafka. Built with Rust and Apache Arrow for zero-copy data transfer.

## Features

- **ClickHouse**: Query and write data via HTTP interface with ArrowStream format
- **StarRocks**: Query and write data via Stream Load API
- **Kafka**: Read and write messages with JSON or raw mode
- **JSON**: Serialize Arrow data to NDJSON or JSON array format
- **Zero-copy**: All operations use Apache Arrow for efficient memory usage
- **GIL-free**: I/O and serialization release the Python GIL for better multithreading

## Installation

```bash
pip install prus
```

## Quick Start

### ClickHouse

```python
import prus
import pyarrow as pa

# Query ClickHouse
table = prus.query_clickhouse_arrow(
    "http://localhost:8123",
    "my_db",
    "SELECT * FROM events LIMIT 1000"
)

# Write to ClickHouse
table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
rows = prus.write_arrow_to_clickhouse(
    table,
    "http://localhost:8123",
    "my_db",
    "my_table"
)
print(f"Wrote {rows} rows")
```

### StarRocks

```python
import prus
import pyarrow as pa

# Query StarRocks
table = prus.query_starrocks_arrow(
    "http://localhost:8030",
    "my_db",
    "SELECT * FROM events LIMIT 1000"
)

# Write to StarRocks
table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
response = prus.write_arrow_to_starrocks(
    table,
    "http://localhost:8030",
    "my_db",
    "my_table",
    label="load-2024-01-01"  # Optional label for idempotency
)
print(response)
```

### Kafka

```python
import prus
import pyarrow as pa

# Read from Kafka (raw mode)
table = prus.read_kafka_to_arrow(
    "localhost:9092",
    "my_topic",
    mode="raw",
    max_messages=1000
)

# Read from Kafka (JSON mode)
schema = pa.schema([
    ("id", "int64"),
    ("name", "utf8"),
    ("timestamp", "timestamp_ms")
])
table = prus.read_kafka_to_arrow(
    "localhost:9092",
    "my_topic",
    mode="json",
    schema=schema,
    properties={"group.id": "my-group", "auto.offset.reset": "earliest"}
)

# Write to Kafka (JSON mode)
table = pa.table({"id": [1, 2], "name": ["a", "b"]})
rows = prus.write_arrow_to_kafka(
    table,
    "localhost:9092",
    "my_topic",
    mode="json"
)

# Write to Kafka (raw mode)
raw_table = pa.table({"value": ["msg1", "msg2"]})
rows = prus.write_arrow_to_kafka(
    raw_table,
    "localhost:9092",
    "my_topic",
    mode="raw"
)
```

### JSON Export

```python
import prus
import pyarrow as pa

table = pa.table({"id": [1, 2], "name": ["a", "b"]})

# Write as NDJSON
prus.write_arrow_ndjson(table, "output.json")

# Write as JSON array
prus.write_arrow_json_array(table, "output.json")

# Serialize to string
json_str = prus.dumps_arrow_ndjson(table)
print(json_str)
```

## API Reference

### ClickHouse

#### `query_clickhouse_arrow`

Query ClickHouse and return a PyArrow Table.

```python
query_clickhouse_arrow(
    base_url: str,
    database: str,
    query: str,
    username: str = "default",
    password: str = ""
) -> pyarrow.Table
```

**Parameters:**
- `base_url`: ClickHouse HTTP endpoint (e.g., "http://localhost:8123")
- `database`: Database name to query
- `query`: SQL query (FORMAT clause is automatically appended as "FORMAT ArrowStream")
- `username`: ClickHouse username (default: "default")
- `password`: ClickHouse password (default: "")

**Returns:** PyArrow Table containing the query results

#### `write_arrow_to_clickhouse`

Write PyArrow data to ClickHouse table.

```python
write_arrow_to_clickhouse(
    data: pyarrow.Table | pyarrow.RecordBatch,
    base_url: str,
    database: str,
    table: str,
    username: str = "default",
    password: str = "",
    datetime_format: str | None = None
) -> int
```

**Parameters:**
- `data`: PyArrow RecordBatch or Table to write
- `base_url`: ClickHouse HTTP endpoint (e.g., "http://localhost:8123")
- `database`: Database name
- `table`: Target table name
- `username`: ClickHouse username (default: "default")
- `password`: ClickHouse password (default: "")
- `datetime_format`: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")

**Returns:** Number of rows written to ClickHouse

### StarRocks

#### `query_starrocks_arrow`

Query StarRocks and return a PyArrow Table.

```python
query_starrocks_arrow(
    base_url: str,
    database: str,
    query: str,
    username: str = "root",
    password: str = "",
    catalog: str = "default_catalog"
) -> pyarrow.Table
```

**Parameters:**
- `base_url`: StarRocks HTTP endpoint (e.g., "http://localhost:8030")
- `database`: Database name to query
- `query`: SQL query
- `username`: StarRocks username (default: "root")
- `password`: StarRocks password (default: "")
- `catalog`: Catalog name (default: "default_catalog")

**Returns:** PyArrow Table containing the query results

#### `write_arrow_to_starrocks`

Write PyArrow data to StarRocks table using Stream Load API.

```python
write_arrow_to_starrocks(
    data: pyarrow.Table | pyarrow.RecordBatch,
    base_url: str,
    database: str,
    table: str,
    username: str = "root",
    password: str = "",
    datetime_format: str | None = None,
    label: str | None = None
) -> str
```

**Parameters:**
- `data`: PyArrow RecordBatch or Table to write
- `base_url`: StarRocks HTTP endpoint (e.g., "http://localhost:8030")
- `database`: Database name
- `table`: Target table name
- `username`: StarRocks username (default: "root")
- `password`: StarRocks password (default: "")
- `datetime_format`: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")
- `label`: Optional Stream Load label for idempotency and tracking

**Returns:** StarRocks Stream Load response as JSON string

### Kafka

#### `read_kafka_to_arrow`

Read Kafka messages into a PyArrow Table.

```python
read_kafka_to_arrow(
    brokers: str,
    topic: str,
    mode: str = "raw",
    max_messages: int | None = None,
    max_duration_seconds: int | None = None,
    schema: pyarrow.Schema | list[tuple] | None = None,
    properties: dict | None = None
) -> pyarrow.Table
```

**Parameters:**
- `brokers`: Kafka bootstrap servers (e.g., "localhost:9092")
- `topic`: Kafka topic name to consume from
- `mode`: Read mode, either "raw" or "json" (default: "raw")
- `max_messages`: Maximum number of messages to read (default: 500,000)
- `max_duration_seconds`: Maximum duration in seconds to poll (default: 180)
- `schema`: Required for "json" mode. Can be PyArrow schema or list of (name, type_str) tuples.
  Supported type_str: bool/int8/int16/int32/int64/uint8/uint16/uint32/uint64/float32/float64/utf8/date32/date64/timestamp_ms/timestamp_us
- `properties`: Optional dict of Kafka consumer config (e.g., {"group.id": "my-group"})

**Returns:** PyArrow Table containing the consumed messages

#### `write_arrow_to_kafka`

Write Arrow data to Kafka topic.

```python
write_arrow_to_kafka(
    data: pyarrow.Table | pyarrow.RecordBatch,
    brokers: str,
    topic: str,
    mode: str = "json",
    properties: dict | None = None,
    datetime_format: str | None = None
) -> int
```

**Parameters:**
- `data`: PyArrow RecordBatch or Table to write
- `brokers`: Kafka bootstrap servers (e.g., "localhost:9092")
- `topic`: Target Kafka topic name
- `mode`: Serialization mode, either "json" or "raw" (default: "json")
- `properties`: Optional dict of Kafka producer config (e.g., {"acks": "all"})
- `datetime_format`: Optional datetime format string for JSON mode (default: "%Y-%m-%d %H:%M:%S")

**Returns:** Number of rows written to Kafka

### JSON Export

#### `write_arrow_json`

Write PyArrow data to a JSON file.

```python
write_arrow_json(
    data: pyarrow.Table | pyarrow.RecordBatch,
    path: str,
    lines: bool = True,
    datetime_format: str | None = None
) -> int
```

**Parameters:**
- `data`: PyArrow RecordBatch or Table to write
- `path`: Output file path
- `lines`: If True, write as NDJSON. If False, write as JSON array (default: True)
- `datetime_format`: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")

**Returns:** Number of rows written

#### `write_arrow_ndjson`

Write PyArrow data to a file in NDJSON format (one JSON object per line).

```python
write_arrow_ndjson(
    data: pyarrow.Table | pyarrow.RecordBatch,
    path: str,
    datetime_format: str | None = None
) -> int
```

**Parameters:**
- `data`: PyArrow RecordBatch or Table to write
- `path`: Output file path
- `datetime_format`: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")

**Returns:** Number of rows written

#### `write_arrow_json_array`

Write PyArrow data to a file as JSON array.

```python
write_arrow_json_array(
    data: pyarrow.Table | pyarrow.RecordBatch,
    path: str,
    datetime_format: str | None = None
) -> int
```

**Parameters:**
- `data`: PyArrow RecordBatch or Table to write
- `path`: Output file path
- `datetime_format`: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")

**Returns:** Number of rows written

#### `dumps_arrow_json`

Serialize PyArrow data to JSON string.

```python
dumps_arrow_json(
    data: pyarrow.Table | pyarrow.RecordBatch,
    lines: bool = True,
    datetime_format: str | None = None
) -> str
```

**Parameters:**
- `data`: PyArrow RecordBatch or Table to serialize
- `lines`: If True, return NDJSON. If False, return JSON array (default: True)
- `datetime_format`: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")

**Returns:** JSON string

#### `dumps_arrow_ndjson`

Serialize PyArrow data to NDJSON string (one JSON object per line).

```python
dumps_arrow_ndjson(
    data: pyarrow.Table | pyarrow.RecordBatch,
    datetime_format: str | None = None
) -> str
```

**Parameters:**
- `data`: PyArrow RecordBatch or Table to serialize
- `datetime_format`: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")

**Returns:** NDJSON string

#### `dumps_arrow_json_array`

Serialize PyArrow data to JSON array string.

```python
dumps_arrow_json_array(
    data: pyarrow.Table | pyarrow.RecordBatch,
    datetime_format: str | None = None
) -> str
```

**Parameters:**
- `data`: PyArrow RecordBatch or Table to serialize
- `datetime_format`: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")

**Returns:** JSON array string

## Performance Tips

1. **Batch operations**: Process multiple rows in a single Arrow Table for better performance
2. **Release GIL**: All I/O operations release the Python GIL, enabling true multithreading
3. **Zero-copy**: Use Arrow format throughout your pipeline to avoid serialization overhead
4. **Kafka bulk writes**: Use JSON mode with larger batches for better throughput
5. **Stream processing**: For large datasets, process in chunks rather than loading all data at once

## Development

Build from source:

```bash
# Install maturin
pip install maturin

# Build the Python extension
maturin develop
maturin develop --release
```

Run tests:

```bash
cargo test
```

## License

MIT
