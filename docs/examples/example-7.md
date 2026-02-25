# Example 7: Multi-Edge Weights with Filters and `dress` in Transform

This example demonstrates a compact pattern for transforming one tabular row into multiple metric vertices and weighted edges, while keeping only valid values with declarative filters.

## Overview

The dataset contains stock observations (`Date`, `Open`, `Close`, `Volume`, `ticker`, ...).  
Instead of storing all columns directly on one vertex, this schema creates:

- one `ticker` vertex (e.g. `AAPL`)
- multiple `metric` vertices (one per metric name/value pair)
- edges `ticker -> metric` weighted by observation date (`t_obs`)

The two novelties in this example are:

1. **Using `dress` in transform** to normalize scalar columns into `(name, value)` pairs
2. **Using vertex `filters`** to keep only valid metric rows (positive values for selected metric names)

## Data

Input CSV (`examples/7-multi-edges-weights/data.csv`) includes columns such as:

```csv
Date,Open,High,Low,Close,Volume,Dividends,Stock Splits,ticker
2014-04-15,17.899999618530273,17.920000076293945,15.149999618530273,15.350000381469727,3531700,0,0,AAPL
2014-04-16,15.350000381469727,16.09000015258789,15.210000038146973,15.619999885559082,266500,0,0,AAPL
2014-04-17,-15.35000,16.09000015258789,15.210000038146973,15.619999885559082,-5,0,0,AAPL
```

## Core Schema Ideas

### 1) `dress` in transform

`dress` reshapes each transformed scalar into a standardized object:

- `key: name`
- `value: value`

Applied on `Open`, `Close`, and `Volume`, this turns one row into metric-like records:

- `{name: "Open", value: 17.9, ...}`
- `{name: "Close", value: 15.35, ...}`
- `{name: "Volume", value: 3531700, ...}`

Example from `schema.yaml`:

```yaml
resources:
-   resource_name: ticker_data
    apply:
    -   foo: round_str
        module: graflo.util.transform
        params: {ndigits: 3}
        input: [Open]
        dress: {key: name, value: value}
    -   foo: round_str
        module: graflo.util.transform
        params: {ndigits: 3}
        input: [Close]
        dress: {key: name, value: value}
    -   foo: int
        module: builtins
        input: [Volume]
        dress: {key: name, value: value}
```

This is especially useful when you want a generic `metric` vertex model instead of fixed columns.

### 2) Vertex filters

The `metric` vertex defines filters that keep only `Open`, `Close`, and `Volume`, and only when `value > 0`:

```yaml
-   name: metric
    fields: [name, value]
    filters:
    -   if_then:
        - or:
          - {field: name, foo: __eq__, value: Open}
          - {field: name, foo: __eq__, value: Close}
          - {field: name, foo: __eq__, value: Volume}
        - {field: value, foo: __gt__, value: 0}
```

So negative values (for example the test row with negative `Open` / `Volume`) are dropped before ingestion.

## Graph Structure

Ticker-to-metric relationships:

![Ticker to Metric](../assets/7-multi-edges-weights/figs/ticker_vc2vc.png){ width="240" }

Metric fields:

![Metric Fields](../assets/7-multi-edges-weights/figs/ticker_vc2fields.png){ width="360" }

Resource pipeline view:

![Resource Pipeline](../assets/7-multi-edges-weights/figs/ticker.resource-ticker_data.png){ width="780" }

## Edge Weights

Edges from `ticker` to `metric` include a direct weight:

```yaml
edge_config:
    edges:
    -   source: ticker
        target: metric
        weights:
            direct: [t_obs]
```

`t_obs` comes from:

```yaml
-   foo: parse_date_yahoo
    module: graflo.util.transform
    map: {Date: t_obs}
```

This preserves time context for each metric relationship.

## Run the Example

```python
from suthing import FileHandle
from graflo import Patterns, Schema
from graflo.db import Neo4jConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams
from graflo.util.onto import FilePattern
import pathlib

schema = Schema.from_dict(FileHandle.load("schema.yaml"))
conn_conf = Neo4jConfig.from_docker_env()
db_type = conn_conf.connection_type

patterns = Patterns()
patterns.add_file_pattern(
    "ticker_data",
    FilePattern(regex="^data.*\\.csv$", sub_path=pathlib.Path("."), resource_name="ticker_data"),
)

engine = GraphEngine(target_db_flavor=db_type)
engine.define_and_ingest(
    schema=schema,
    target_db_config=conn_conf,
    patterns=patterns,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
```

## Key Takeaways

1. **`dress` enables schema normalization** from wide tabular columns into reusable `(name, value)` metric records.
2. **`filters` provide declarative data quality checks** directly in the schema.
3. **Edge weights (`t_obs`) preserve temporal information** for repeated metric observations.
4. **Multiple edges between the same vertex types** naturally model evolving time-series measurements.

