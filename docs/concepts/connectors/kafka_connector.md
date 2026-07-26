# Kafka connector

GraFlo ingests **finite batches** of JSON object messages from Kafka topics via bindings + `conn_proxy`, the same credential-free pattern as SQL and REST API connectors.

- **`KafkaConnector`** (manifest) — topics, consumer group, decode/stop options (secret-free).
- **`KafkaConnConfig`** / **`KafkaGeneralizedConnConfig`** (runtime) — bootstrap servers and optional SASL/SSL settings, registered on a **`ConnectionProvider`**.
- **`KafkaDataSource`** — polls until a stop condition, yields document dicts to the bound resource pipeline.

This is **consume-only** and **batch-bounded** (not a never-ending daemon consumer). Produce/emit and Schema Registry / Avro are out of scope for this surface.

## Manifest shape

```yaml
bindings:
  connectors:
    - name: events_kafka
      topics:
        - graflo.events
      group_id: graflo-ingest
      auto_offset_reset: earliest   # or latest
      value_encoding: json
      idle_ms: 2000
      # max_wait_ms: 30000
      # include_headers: true
      # row_annotations:
      #   _source: kafka
  resource_connector:
    - resource: events
      connector: events_kafka
  connector_connection:
    - connector: events_kafka
      conn_proxy: kafka_local
```

`BoundSourceKind.KAFKA` selects this path in **`RegistryBuilder`**.

### Connector fields

| Field | Default | Meaning |
| ----- | ------- | ------- |
| **`topics`** | *(required)* | Non-empty list of topic names to subscribe |
| **`group_id`** | *(required)* | Consumer group id |
| **`auto_offset_reset`** | `earliest` | Start when no committed offset exists (`earliest` \| `latest`) |
| **`value_encoding`** | `json` | Payload decode mode (`json` only) |
| **`include_headers`** | `false` | Attach decoded headers under **`_kafka_headers`** |
| **`idle_ms`** | `2000` | Stop after this many ms with **no messages** once at least one message was seen (`0` disables) |
| **`max_wait_ms`** | `null` | Optional wall-clock cap for one `iter_batches` run |
| **`poll_timeout_ms`** | `500` | Per-poll timeout passed to the client |
| **`row_annotations`** | `{}` | Constant fields merged into every decoded row (doc wins) |

## Runtime credentials

Keep secrets out of YAML. Register bootstrap (and optional auth) on the **`conn_proxy`** label:

```python
from graflo.hq.connection_provider import InMemoryConnectionProvider
from graflo.hq.ingestion_parameters import IngestionParams

provider = InMemoryConnectionProvider()
provider.register_all_kafka_configs_from_env(bindings=bindings)

engine.define_and_ingest(
    manifest=manifest,
    target_db_config=conn_conf,
    connection_provider=provider,
    ingestion_params=IngestionParams(),
)
```

Each `conn_proxy` maps to an uppercase env prefix (`kafka_local` → `KAFKA_LOCAL_`):

| Variable | Required | Meaning |
| -------- | -------- | ------- |
| `{PREFIX}BOOTSTRAP_SERVERS` | yes | Broker list (e.g. `localhost:9092`) |
| `{PREFIX}SECURITY_PROTOCOL` | no (default `PLAINTEXT`) | `PLAINTEXT`, `SASL_PLAINTEXT`, `SASL_SSL`, or `SSL` |
| `{PREFIX}CLIENT_ID` | no | Client id |
| `{PREFIX}SASL_MECHANISM` / `{PREFIX}SASL_USERNAME` / `{PREFIX}SASL_PASSWORD` | when using SASL | Auth settings |

Manual registration:

```python
from graflo.connection_models import KafkaConnConfig, KafkaGeneralizedConnConfig

provider.register_generalized_config(
    conn_proxy="kafka_local",
    config=KafkaGeneralizedConnConfig(
        config=KafkaConnConfig(bootstrap_servers="localhost:9092"),
    ),
)
provider.bind_from_bindings(bindings=bindings)
```

## Message → document shape

Only JSON **objects** become rows. Arrays, scalars, and invalid UTF-8/JSON are skipped with a warning.

Each yielded document merges:

1. Optional **`row_annotations`** (defaults; payload keys win)
2. Kafka metadata: **`_kafka_topic`**, **`_kafka_partition`**, **`_kafka_offset`**, **`_kafka_key`**
3. Optional **`_kafka_headers`** when **`include_headers: true`**
4. Decoded JSON object fields

Offsets are committed after each yielded batch (**at-least-once**).

## Stop conditions

One consume run ends when any of the following holds:

1. **`iter_batches(..., limit=N)`** reached (total records across batches)
2. **`max_wait_ms`** elapsed since the run started
3. **`idle_ms`** elapsed with no new messages **after** at least one message was received (avoids exiting during group assignment before backlog is readable)

## Local broker and live tests

`docker/kafka` runs Apache Kafka ([`apache/kafka:4.3.1`](https://hub.docker.com/r/apache/kafka)) in KRaft single-broker mode. Bootstrap: **`localhost:9092`**. Included in `docker/start-all.sh` / `stop-all.sh` / `cleanup-all.sh`.

```bash
cd docker
./start-all.sh   # or: cd kafka && docker compose --env-file .env up -d
```

Live integration tests are marked `kafka` and skipped unless opted in:

```bash
uv run pytest test -m kafka --run-kafka
```

`confluent-kafka` is a default package dependency (lazy import surfaces a clear error if the native library is missing).

## Related

- [API connector and pagination](api_connector.md) — same `conn_proxy` pattern for REST
- [Data source reference — Kafka](../../reference/data_source/index.md#kafka-data-sources)
- [Runtime connector updates](runtime_updates.md)

Implementation: `graflo.architecture.contract.bindings.KafkaConnector`, `graflo.data_source.kafka.KafkaDataSource`, `graflo.connection_models.KafkaConnConfig`, `graflo.hq.connection_provider`.
