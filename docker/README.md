# Running docker images of various graph databases

## Quick start

Each service is a compose project in its own folder. Its settings (image, ports, credentials, `SPEC`) are in `<service>/.env.example`. The scripts pass that file to compose with `--env-file`, and `XConfig.from_docker_env()` reads the same file, so the containers and the tests agree.

```shell
./start-all.sh                  # every service; waits for health checks
./start-all.sh neo4j postgres   # just these
./stop-all.sh [service ...]     # stop, keeping containers and volumes
./cleanup-all.sh [service ...]  # remove containers and volumes (asks first; --yes skips)
./cleanup-all.sh --images       # also remove the images
./cleanup-all.sh --no-volumes   # containers only
```

After an image upgrade that changes a database's store format (Neo4j 5.26 to 2026.x, for instance), run `./cleanup-all.sh <service>` before starting it again.

| Service | Host port |
|---|---|
| ArangoDB | 8535 |
| Neo4j | 7475 (HTTP), 7688 (Bolt) |
| TigerGraph | 14241 |
| FalkorDB | 6379 (Redis), 3001 (browser) |
| Memgraph | 7687 |
| NebulaGraph | 9669 |
| PostgreSQL | 5432 |
| Fuseki | 3032 |
| SeaweedFS | 8333 (S3 API) |
| Kafka | 9092 |

## One service by hand

From the service's folder, with its profile (`<SPEC>.<service>`, e.g. `graflo.neo4j`):

```shell
docker compose --env-file .env.example --profile graflo.neo4j up -d
```

to stop containers from docker compose

```shell
docker compose stop <container_name>
```

to bash into a container

```shell
docker exec -it <container_name> sh
```

## TigerGraph

TigerGraph does not provide their Community Edition on dockerhub, so (as of 2025-10) select your version here [https://download.tigergraph.com/](https://download.tigergraph.com/), fill out the form and download a `.tar.gz` image file following the link in an email. Then follow:

```shell
docker load -i ./tigergraph-4.2.0-community-docker-image.tar.gz
```

After this, run TigerGraph in the same way as other DBs:

```shell
docker compose --env-file .env.example up <container_spec> -d
```

NB: we only use default password `tigergraph` in this repo (our docker-compose script does not change default password).

Alternatively, you can run TigerGraph manually:

```shell
docker load -i ./tigergraph-4.2.0-community-docker-image.tar.gz  # Change the .gz file name depending on what you have downloaded
docker images  # Find image id
docker run -d --init -p 14240:14240 --name tigergraph tigergraph/community:4.2.0  # Run a container named tigergraph from the imported image
docker exec -it tigergraph /bin/bash  # Start a shell on this container
gadmin start all  # Start all tigergraph component services
gadmin status  # Should see all services are up
```

Follow instructions given [here](https://github.com/tigergraph/ecosys/blob/master/demos/guru_scripts/docker/README.md#quick-start-for-community-edition) to work with community edition.

## NebulaGraph

NebulaGraph requires several services (metad, storaged, graphd, and graphstudio). Run as:

```shell
cd nebula
docker compose --env-file .env.example --profile graflo.nebula up -d
```

**Connection Details:**
- GraphD port: `9669` (default, configurable via `NEBULA_PORT` in `.env.example`)
- GraphStudio web interface: `http://localhost:7001` (configurable via `GRAPHSTUDIO_PORT` in `.env.example`)
- Use `nebula-graphd` hostname to connect using GraphStudio or programmatically
- Default credentials: `root` / `test!passfortesting` (configurable via `NEBULA_USER` and `NEBULA_PASSWORD` in `.env.example`)

**Programmatic Connection:**
You can use `NebulaConfig.from_docker_env()` in your Python code to connect:

```python
from graflo.connections import NebulaConfig

config = NebulaConfig.from_docker_env()
```

## ArangoDB

ArangoDB web interface: [http://localhost:8535](http://localhost:8535)

NB: The standard ArangoDB port is 8529, but the `.env.example` config in graflo uses 8535.

**Programmatic Connection:**
```python
from graflo.connections import ArangoConfig

config = ArangoConfig.from_docker_env()
```

## Neo4j

Neo4j web interface: [http://localhost:7475](http://localhost:7475)

NB: The standard Neo4j port is 7474, but the `.env.example` config in graflo uses 7475.

**Programmatic Connection:**
```python
from graflo.connections import Neo4jConfig

config = Neo4jConfig.from_docker_env()
```

## PostgreSQL

PostgreSQL can be used as a source database for ingesting data into graph databases.

**Programmatic Connection:**
```python
from graflo.connections import PostgresConfig

config = PostgresConfig.from_docker_env()
```

## FalkorDB

FalkorDB is a Redis-based graph database that supports OpenCypher.

**Programmatic Connection:**
```python
from graflo.connections import FalkordbConfig

config = FalkordbConfig.from_docker_env()
```

## SeaweedFS (S3)

SeaweedFS provides S3-compatible object storage ([Docker Hub image](https://hub.docker.com/r/chrislusf/seaweedfs)). Use it for file sources with an `s3://` root.

**Run:**
```shell
cd seaweedfs
docker compose --profile graflo.seaweedfs up -d
```

**Endpoint:** S3 API at `http://127.0.0.1:8333` (host port `S3_API_PORT`). No identities are configured, so the API accepts any key pair; the bucket is created on first use.

Settings go in `docker/seaweedfs/.env.example`; every one is optional: `S3_ENDPOINT` (or `S3_PROTOCOL`, `S3_HOSTNAME`, `S3_API_PORT`), `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET` (default `graflo-staging`), `AWS_REGION`.

**Programmatic connection (boto3):**
```python
from graflo.object_storage import S3EndpointConfig
from graflo.object_storage import ensure_staging_bucket_for_config

config = S3EndpointConfig.from_docker_env()
ensure_staging_bucket_for_config(config)
# config.endpoint_url, config.access_key, config.secret_key, config.bucket, …
```

Boto3 client factories and bucket ensure live in **`graflo.object_storage`**. See **docs/concepts/operations/object_storage.md** in the repo.


## Kafka

Apache Kafka in KRaft combined mode ([Docker Hub image](https://hub.docker.com/r/apache/kafka)). Single-broker fixture for local ingest / streaming tests.

**Run:**
```shell
cd kafka
docker compose --env-file .env.example --profile graflo.kafka up -d
```

**Connection Details:**
- Bootstrap servers: `localhost:9092` (host port via `KAFKA_PORT` in `.env.example`)
- Plaintext listener only (no auth) — local testing
- Create a topic from the container:
  ```shell
  docker exec --workdir /opt/kafka/bin/ -it graflo.kafka \
    ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-topic
  ```

**Port conflicts:** If `9092` is already allocated, set `KAFKA_PORT` (and matching `KAFKA_BOOTSTRAP_SERVERS` / advertised host port) in `docker/kafka/.env.example`, then recreate the container.

## Memgraph

Memgraph is a high-performance, in-memory graph database that supports OpenCypher and uses the Bolt protocol.

The default image is `memgraph/memgraph-mage:3.14.0`; GraFlo needs Memgraph 3.2 or later.

**Connection Details:**
- Bolt port: `7687` (default, configurable via `MEMGRAPH_PORT` in `.env.example`)
- Uses Bolt protocol (bolt://) for connections
- Optional authentication (configurable via `MEMGRAPH_USER` and `MEMGRAPH_PASSWORD` in `.env.example`)

**Programmatic Connection:**
```python
from graflo.connections import MemgraphConfig

config = MemgraphConfig.from_docker_env()
```
