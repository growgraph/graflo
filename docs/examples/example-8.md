# Example 8: Multi-Edge Weights with Filters and `dress` Transforms

This example ingests ticker CSV data into Neo4j with:

- **Two vertex types** — `ticker` (by `oftic`) and `metric` (by `name` + `value`), where `metric` rows are filtered so only Open, Close, and Volume with positive values become vertices.
- **One edge** — `ticker` → `metric` with **multiple weights** (`direct` on `t_obs` plus nested `vertices` metadata on the metric endpoint).
- **Transforms with `dress`** — `round_str` and `int` transforms targeted at specific `(name, value)` pairs via `dress: { key: name, value: value }`, plus a date parse that emits `t_obs`.

## Layout

- `examples/8-multi-edges-weights/manifest.yaml` — logical schema, DB profile (Neo4j indexes, edge specs), transforms, and `ticker_data` resource pipeline.
- `examples/8-multi-edges-weights/ingest.py` — `FileConnector` + `Bindings`, then `GraphEngine.define_and_ingest(...)`.
- `examples/8-multi-edges-weights/data.csv` — sample OHLCV-style rows.

## Run locally

From the example directory, with Neo4j running (see repo `docker/neo4j`), run:

```bash
uv run python ingest.py
```

## Related

- [Polymorphic routing (Example 7)](example-7.md) uses `vertex_router` + dynamic `edge` for type-discriminated tables; this example uses **filters** on a vertex type and **multi-weight** edges instead.
