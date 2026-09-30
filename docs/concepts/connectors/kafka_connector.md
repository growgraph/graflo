# Kafka connector

A Kafka [connector](../glossary.md#connector) reads JSON messages from one or more Kafka topics, such as a plant's sensor feed, and hands each message to a [resource](../glossary.md#resource) as one record. A run reads what the topic holds and then stops; it is not a consumer that runs forever. Use it to load a graph from a topic, or to top it up on a schedule. This page shows how to declare the topics, when a run stops, what a message becomes, and how to try it against a local broker.

## A first example

Suppose the topic `sensor-readings` carries messages such as:

```json
{"sensor_id": "TMP-12", "machine_serial": "SN-4471", "value": 71.3}
```

Add this `bindings` block to a [manifest](../glossary.md#manifest) that has a `readings` resource:

```yaml
bindings:
  connectors:
    - name: sensor_readings
      topics: [sensor-readings]
      group_id: graflo-readings
      resource_name: readings
  connector_connection:
    - connector: sensor_readings
      conn_proxy: sensor_feed
```

- `topics` lists the topics to subscribe to.
- `group_id` names the Kafka consumer group. The offsets that the group has committed decide where the next run starts.
- `resource_name` sends every message to the `readings` resource.
- `conn_proxy: sensor_feed` is a [connection proxy](../glossary.md#connection-proxy): a label that stands for the broker address and credentials, so the manifest holds neither.

Set the broker address for that label:

```bash
export SENSOR_FEED_BOOTSTRAP_SERVERS=localhost:9092
```

Then load the manifest, register the connection from the environment, and run the ingestion:

```python
from pathlib import Path

from suthing import FileHandle

from graflo import GraphEngine, GraphManifest
from graflo.connections import GraFloBackendConfig, InMemoryConnectionProvider

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
bindings = manifest.require_bindings()

# Reads SENSOR_FEED_BOOTSTRAP_SERVERS and the optional SENSOR_FEED_* variables
provider = InMemoryConnectionProvider()
provider.register_all_kafka_configs_from_env(bindings=bindings)

target = GraFloBackendConfig(output_dir=Path("artifacts/readings"))
engine = GraphEngine(target_db_flavor=target.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=target,
    connection_provider=provider,
    recreate_schema=True,
)
```

A new consumer group has no committed offsets, so the first run reads the topic from its oldest message. The run stops two seconds after the last message arrives and writes the graph to the [file backend](../glossary.md#file-backend), a directory on disk. To write to a database, pass its config, such as a `Neo4jConfig`, as `target_db_config`. A second run with the same `group_id` reads only the messages that arrived after the first.

## Try it with a local broker

A checkout of the GraFlo repository has a single-broker Kafka setup in `docker/kafka`, with a plaintext listener on `localhost:9092`. Start it on its own:

```bash
cd docker/kafka
docker compose --env-file .env --profile graflo.kafka up -d
```

`docker/start-all.sh` starts it together with the other services in `docker/`.

Create the topic:

```bash
docker exec --workdir /opt/kafka/bin/ -it graflo.kafka \
  ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensor-readings
```

Put a few messages on it with the `confluent-kafka` client, which is installed with GraFlo:

```python
import json

from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})
for reading in [
    {"sensor_id": "TMP-12", "machine_serial": "SN-4471", "value": 71.3},
    {"sensor_id": "VIB-03", "machine_serial": "SN-4471", "value": 0.42},
    {"sensor_id": "TMP-07", "machine_serial": "SN-2210", "value": 64.8},
]:
    producer.produce(
        "sensor-readings", key=reading["machine_serial"], value=json.dumps(reading)
    )
producer.flush()
```

Run the ingestion above: it reads the three messages, waits two seconds for more, and stops.

## When a run stops

A run ends at the first of these events:

1. `idle_ms` milliseconds pass without a new message, counted from the last message received. The default is 2000; `idle_ms: 0` turns this off.
2. The run has lasted `max_wait_ms` milliseconds. It is unset by default.
3. `IngestionParams.max_items` records have been read. You pass `IngestionParams` to `define_and_ingest` as `ingestion_params`.

The idle clock starts only with the first message, so that a run does not end while the broker is assigning partitions to the consumer group. As a consequence, a run on a topic with no new messages never ends on its own. Set `max_wait_ms` whenever the topic may be empty, and whenever you set `idle_ms: 0`.

## What a message becomes

Each message value must be a JSON object encoded in UTF-8. A message whose value is an array, a single value, invalid JSON or empty is skipped with a warning in the log.

The record holds the fields of the JSON object plus these fields:

| Field | Value |
|---|---|
| `_kafka_topic` | Topic of the message |
| `_kafka_partition` | Partition of the message |
| `_kafka_offset` | Offset of the message in its partition |
| `_kafka_key` | Message key as text, or `null` when the message has no key |
| `_kafka_headers` | Message headers as `{name: text}`; present only with `include_headers: true` |

The message from the first example, sent with the key `SN-4471`, becomes:

```json
{
  "_kafka_topic": "sensor-readings",
  "_kafka_partition": 0,
  "_kafka_offset": 42,
  "_kafka_key": "SN-4471",
  "sensor_id": "TMP-12",
  "machine_serial": "SN-4471",
  "value": 71.3
}
```

`row_annotations` adds constant fields to every record, for example to mark which feed a record came from. When names collide, the message's own fields win over the `_kafka_*` fields, and those win over `row_annotations`.

## Offsets and consumer groups

GraFlo commits the offsets of the consumer group itself; automatic commits are off. It commits after each batch of records it reads, so the next run with the same `group_id` starts after the last committed batch.

A group with no committed offsets starts where `auto_offset_reset` says: `earliest` reads the topic from its oldest message, and `latest` reads only messages that arrive after the consumer joins. To read a topic again from the start, run with a new `group_id`.

GraFlo commits a batch as soon as it asks for the next one, and it reads ahead of writing (see `batch_prefetch` in [Parallelism](../ingestion/parallelism.md)). A batch can therefore be committed before it is written to the database. If a run fails part way, messages whose offsets were committed can be missing from the graph; rerun with a new `group_id` to read them again.

## Connector fields

| Field | Default | Meaning |
|---|---|---|
| `topics` | required | Topics to subscribe to; at least one non-empty name |
| `group_id` | required | Kafka consumer group |
| `auto_offset_reset` | `earliest` | Where a group with no committed offset starts: `earliest` or `latest` |
| `idle_ms` | `2000` | Stop after this many milliseconds without a message, counted from the last message; `0` turns it off |
| `max_wait_ms` | `null` | Stop this many milliseconds after the run started |
| `row_annotations` | `{}` | Constant fields added to every record; the message's own fields win |
| `include_headers` | `false` | Add the message headers under `_kafka_headers` |
| `poll_timeout_ms` | `500` | How long each poll of the broker waits for a message |
| `value_encoding` | `json` | How message values are decoded; `json` is the only value |

## Broker address and credentials

At run time a [connection provider](../glossary.md#connection-provider) maps each `conn_proxy` label to a `KafkaConnConfig`. `register_all_kafka_configs_from_env` finds every label that a Kafka connector uses, reads the variables for each label, and binds each connector to its label. The variable prefix is the label in upper case, with `-` replaced by `_`, followed by `_`: the label `sensor_feed` reads `SENSOR_FEED_*`.

| Variable | Required | Meaning |
|---|---|---|
| `{PREFIX}BOOTSTRAP_SERVERS` | yes | Brokers to connect to, as a comma-separated list of `host:port` |
| `{PREFIX}SECURITY_PROTOCOL` | no, default `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL` |
| `{PREFIX}SASL_MECHANISM` | with SASL | For example `PLAIN` or `SCRAM-SHA-512` |
| `{PREFIX}SASL_USERNAME`, `{PREFIX}SASL_PASSWORD` | with SASL | User name and password |
| `{PREFIX}CLIENT_ID` | no | Client id that the broker sees |

A missing `{PREFIX}BOOTSTRAP_SERVERS`, or a `SECURITY_PROTOCOL` other than these four, raises a `ValueError` that names the variable. As with API connectors, `env_prefix_map` maps a label to a different prefix, and `register_kafka_config_from_env(conn_proxy=...)` registers a single label and needs a `bind_from_bindings` call after it.

To register the connection in Python, for example with a password from a secret store:

```python
from graflo.connections import (
    InMemoryConnectionProvider,
    KafkaConnConfig,
    KafkaGeneralizedConnConfig,
)

provider = InMemoryConnectionProvider()
provider.register_generalized_config(
    conn_proxy="sensor_feed",
    config=KafkaGeneralizedConnConfig(
        config=KafkaConnConfig(
            bootstrap_servers="kafka.plant.example:9093",
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_username="graflo",
            sasl_password="your-password",
        )
    ),
)
provider.bind_from_bindings(bindings=bindings)
```

## Limits

- Messages must be JSON objects. There is no Avro and no schema registry support.
- The connector only reads. GraFlo does not write to Kafka.
- `KafkaConnConfig` has no fields for certificate files or a custom certificate authority.
- An error reported by the broker while polling raises an exception and ends the ingestion.
- A connector whose label has no registered configuration fails the run before anything is read, with a `ValueError` that names the resource and the connector. With `IngestionParams(strict_registry=False)` it is skipped with a warning in the log and the other connectors run as usual.

## What to read next

- [Credentials outside the manifest (example 11)](../../examples/connection-proxy/index.md): the connection proxy pattern with a database source.
- [API connector](api_connector.md): read records from a REST endpoint, with the same labels and providers.
- [Parallelism](../ingestion/parallelism.md): how batches are read ahead, cast and written.
