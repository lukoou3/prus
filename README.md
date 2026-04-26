# prus

High-performance Python extension for data I/O with ClickHouse, StarRocks, and optional Kafka. Built with Rust and Apache Arrow for zero-copy data transfer. Includes a small **Faker** module to synthesize PyArrow `RecordBatch` data from JSON field configs (useful for tests and benchmarks).

## Features

- **ClickHouse**: Query and write data via HTTP interface with ArrowStream format
- **StarRocks**: Query and write data via Stream Load API
- **Kafka** (optional build): Read and write messages with JSON or raw mode (enable with `--features kafka` when building from source)
- **JSON**: Serialize Arrow data to NDJSON or JSON array format
- **Faker**: Generate a PyArrow `RecordBatch` from a JSON description of per-column generators (`int`, `bigint`, `float`, `double`, ranges, option lists, null/array wrappers)
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

When installing from source, Kafka APIs require building with **`--features kafka`** (see Development). Prebuilt wheels may or may not include Kafka depending on how they were published.

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

# Read from Kafka (raw mode) with SASL_PLAINTEXT
table = prus.read_kafka_to_arrow(
    "localhost:9094",
    "my_topic",
    mode="raw",
    max_messages=1000,
    properties= {"security.protocol": "SASL_PLAINTEXT", "sasl.mechanisms": "SCRAM-SHA-256", "sasl.username": "xxx", "sasl.password": "xxx"}
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

# Kafka Tools

# Get topic information
topic_info = prus.get_kafka_topic_info("localhost:9092", "my_topic")
print(topic_info)

# Get consumer group information
group_info = prus.get_kafka_group_info("localhost:9092", "my_topic", "my-group")
print(group_info)

# Reset group offset to latest
reset_result = prus.reset_kafka_group_offset_latest("localhost:9092", "my_topic", "my-group")
print(reset_result)

# Reset group offset to specific timestamp
timestamp_ms = int(time.time() * 1000) - 3600000  # 1 hour ago
reset_result = prus.reset_kafka_group_offset_for_ts("localhost:9092", "my_topic", "my-group", timestamp_ms)
print(reset_result)
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

### Faker (synthetic RecordBatch)

Describe each column as a JSON object (a **JSON array** of such objects). Every object has a **`name`** column label and a **`type`** discriminator (`int`, `bigint`, `float`, `double`). Remaining keys are flattened on the same object (same pattern as `serde` internally tagged configs).

- **`int` / `bigint`**: Either set **`options`** to a JSON array of values (optional `null` entries) to cycle or sample from a list, or leave `options` empty and use half-open integer range **`min`..`max`** with **`random`** (sequential vs uniform). Optional wrappers: **`null_rate`**, **`array`** (with **`array_len_min`** / **`array_len_max`**, **`array_item_null_rate`**).
- **`float` / `double`**: With empty **`options`**, values are uniform in **`[min, max)`**. With **`options`**, use **`random`** for list mode. Same optional wrapper fields as above.

```python
import json
import prus

cfg = [
    {"name": "id", "type": "int", "min": 0, "max": 3, "random": False},
    {"name": "score", "type": "float", "min": 0.0, "max": 1.0},
    {"name": "label", "type": "int", "options": [10, 20], "random": False, "min": 0, "max": 1},
]
batch = prus.fake_arrow_record_batch_from_json(json.dumps(cfg), num_rows=100)
# batch is a pyarrow.RecordBatch
```

Heavy generation runs **without holding the GIL** (same pattern as other CPU-bound entry points). Invalid JSON or inconsistent configs raise **`RuntimeError`**.

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
    properties: dict | None = None,
    start_timestamp_ms: int | None = None,
    include_metadata: bool = False
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
- `properties`: Optional dict of Kafka consumer config (e.g., {"group.id": "my-group", "auto.offset.reset": "earliest"}, {"security.protocol": "SASL_PLAINTEXT", "sasl.mechanisms": "SCRAM-SHA-256", "sasl.username": "xx", "sasl.password": "xx"})
- `start_timestamp_ms`: Optional timestamp (in milliseconds) to start consuming from. If set, will seek to the earliest offset at or after this timestamp.
- `include_metadata`: Whether to include Kafka metadata columns (partition_id, offset, timestamp) (default: False)

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

#### `get_kafka_topic_info`

Get Kafka topic information including partitions and their offsets.

```python
get_kafka_topic_info(
    brokers: str,
    topic: str,
    properties: dict | None = None
) -> dict
```

**Parameters:**
- `brokers`: Kafka bootstrap servers (e.g., "localhost:9092")
- `topic`: Kafka topic name
- `properties`: Optional dict of Kafka consumer config

**Returns:** Dict with topic information, including partitions and their offsets

#### `get_kafka_group_info`

Get Kafka consumer group information including offsets and lag.

```python
get_kafka_group_info(
    brokers: str,
    topic: str,
    group_id: str,
    properties: dict | None = None
) -> dict
```

**Parameters:**
- `brokers`: Kafka bootstrap servers (e.g., "localhost:9092")
- `topic`: Kafka topic name
- `group_id`: Consumer group ID
- `properties`: Optional dict of Kafka consumer config

**Returns:** Dict with group information, including partitions, offsets, and lag

#### `reset_kafka_group_offset_latest`

Reset Kafka consumer group offset to latest (high watermark).

```python
reset_kafka_group_offset_latest(
    brokers: str,
    topic: str,
    group_id: str,
    properties: dict | None = None
) -> dict
```

**Parameters:**
- `brokers`: Kafka bootstrap servers (e.g., "localhost:9092")
- `topic`: Kafka topic name
- `group_id`: Consumer group ID
- `properties`: Optional dict of Kafka consumer config

**Returns:** Dict with reset status

#### `reset_kafka_group_offset_for_ts`

Reset Kafka consumer group offset to specific timestamp.

```python
reset_kafka_group_offset_for_ts(
    brokers: str,
    topic: str,
    group_id: str,
    timestamp_ms: int,
    properties: dict | None = None
) -> dict
```

**Parameters:**
- `brokers`: Kafka bootstrap servers (e.g., "localhost:9092")
- `topic`: Kafka topic name
- `group_id`: Consumer group ID
- `timestamp_ms`: Timestamp in milliseconds
- `properties`: Optional dict of Kafka consumer config

**Returns:** Dict with reset status and offset information

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

### Faker

#### `fake_arrow_record_batch_from_json`

Build a PyArrow `RecordBatch` from a JSON string describing each column’s faker.

```python
fake_arrow_record_batch_from_json(
    field_configs_json: str,
    num_rows: int,
) -> pyarrow.RecordBatch
```

**Parameters:**
- `field_configs_json`: UTF-8 JSON **array** of per-field objects. Each object must include **`name`** (Arrow field name) and **`type`** (`int`, `bigint`, `float`, or `double`). Other keys depend on `type` (e.g. `min`, `max`, `random`, `options`, and optional `null_rate` / `array` / `array_len_min` / `array_len_max` / `array_item_null_rate` for wrappers).
- `num_rows`: Number of logical rows to generate. Each row runs all column fakers in order, so row `i` is built from the `i`-th draw of every column.

**Returns:** A `pyarrow.RecordBatch` with nullable columns (`null` bitmaps may be all-valid depending on generators).

**Raises:** `RuntimeError` if JSON is malformed or batch construction fails.

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
# Optional: Kafka client (requires CMake / librdkafka toolchain)
maturin develop --features kafka
maturin develop --release
maturin develop --release --features kafka
```

Run tests:

```bash
cargo test
```

## License

MIT
