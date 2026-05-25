# Running docker images of various graph databases

## Quick Start - All Services

The easiest way to run all graph databases at once is using the convenience scripts:

**Start all services:**
```shell
./start-all.sh
```

This will start all available graph database services:
- ArangoDB (port 8535)
- Neo4j (port 7475)
- TigerGraph (port 14241)
- FalkorDB (port 3001)
- Memgraph (port 7687)
- NebulaGraph (port 9669)
- PostgreSQL (port 5432)
- MinIO (API port 9000, console 9001)

**Stop all services:**
```shell
./stop-all.sh
```

**Cleanup (remove containers, volumes, and optionally images):**
```shell
./cleanup-all.sh              # Remove containers and volumes
./cleanup-all.sh --images     # Also remove docker images
./cleanup-all.sh --no-volumes # Remove containers only (keep volumes)
```

These scripts will automatically:
- Detect the `SPEC` variable from each `.env` file (defaults to `graflo`)
- Use the appropriate profile for each service
- Handle services with or without `.env` files
- Start services in the correct order (e.g., NebulaGraph requires multiple services)

## General instruction

To run individual services, use:

```shell
docker compose --env-file .env up <container_spec> -d
```

**Note:** For convenience, you can also use `./start-all.sh` to start all services at once (see Quick Start section above).

```shell
docker compose --env-file .env up <container_spec> -d
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
docker compose --env-file .env up <container_spec> -d
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
docker compose --env-file .env --profile graflo.nebula up -d
```

**Connection Details:**
- GraphD port: `9669` (default, configurable via `NEBULA_PORT` in `.env`)
- GraphStudio web interface: `http://localhost:7001` (configurable via `GRAPHSTUDIO_PORT` in `.env`)
- Use `nebula-graphd` hostname to connect using GraphStudio or programmatically
- Default credentials: `root` / `test!passfortesting` (configurable via `NEBULA_USER` and `NEBULA_PASSWORD` in `.env`)

**Programmatic Connection:**
You can use `NebulaConfig.from_docker_env()` in your Python code to connect:

```python
from graflo.db.connection.onto import NebulaConfig
config = NebulaConfig.from_docker_env()
```

## ArangoDB

ArangoDB web interface: [http://localhost:8535](http://localhost:8535)

NB: The standard ArangoDB port is 8529, but the `.env` config in graflo uses 8535.

**Programmatic Connection:**
```python
from graflo.db.connection.onto import ArangoConfig
config = ArangoConfig.from_docker_env()
```

## Neo4j

Neo4j web interface: [http://localhost:7475](http://localhost:7475)

NB: The standard Neo4j port is 7474, but the `.env` config in graflo uses 7475.

**Programmatic Connection:**
```python
from graflo.db.connection.onto import Neo4jConfig
config = Neo4jConfig.from_docker_env()
```

## PostgreSQL

PostgreSQL can be used as a source database for ingesting data into graph databases.

**Programmatic Connection:**
```python
from graflo.db.connection.onto import PostgresConfig
config = PostgresConfig.from_docker_env()
```

## FalkorDB

FalkorDB is a Redis-based graph database that supports OpenCypher.

**Programmatic Connection:**
```python
from graflo.db.connection.onto import FalkordbConfig
config = FalkordbConfig.from_docker_env()
```

## MinIO

MinIO provides S3-compatible object storage ([Docker Hub image](https://hub.docker.com/r/minio/minio)). Use it for bulk staging (e.g. TigerGraph examples that upload staged CSVs).

**Run:**
```shell
cd minio
docker compose --env-file .env --profile graflo.minio up -d
```

**Endpoints:**
- S3 API: `http://127.0.0.1:9000` (host port configurable via `MINIO_API_PORT` in `.env`)
- Web console: `http://127.0.0.1:9001` (configurable via `MINIO_CONSOLE_PORT`)

The S3 API and the console use **different** host ports. Tools such as boto3 and `examples/10-tigergraph-bulk-s3` must reach the **API** port (`MINIO_API_PORT` / `MINIO_ENDPOINT`), not the console URL (e.g. `/endpoints` on the console port).

**Port conflicts:** If `docker compose` fails with `port is already allocated` (often **9001**), another process is using that host port. Set `MINIO_CONSOLE_PORT` (and `MINIO_API_PORT` if needed) to free values in `docker/minio/.env`, remove any stuck container (`docker rm -f graflo.minio`), then `docker compose --env-file .env --profile graflo.minio up -d` again. While the MinIO container is not running, you will see connection refused on the API port even if something unrelated responds on 9001.

When TigerGraph runs in Docker and GraFlo uploads from the host, set **`MINIO_LOADER_ENDPOINT`** (or **`MINIO_TIGERGRAPH_ENDPOINT`**) to the MinIO URL **as seen from the TigerGraph container** (e.g. `http://172.17.0.1:9003` or `http://host.docker.internal:9003`). That value is used only in the GSQL `CREATE DATA_SOURCE` for bulk loads; boto3 on the host still uses `MINIO_HOSTNAME` / `MINIO_API_PORT`.

Default credentials are `minioadmin` / `minioadmin` (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`). Staging bucket name defaults (`MINIO_STAGING_BUCKET`, etc.) live in `docker/minio/.env`; you can still create buckets in the console or with `mc`.

**Programmatic connection (boto3 / bulk staging):**
```python
from graflo.db import MinioConfig
from graflo.object_storage import ensure_staging_bucket_for_config

config = MinioConfig.from_docker_env()
ensure_staging_bucket_for_config(config)
# config.endpoint_url, config.access_key, config.secret_key, config.bucket, …
```

Implementation helpers (`upload_staged_csvs`, boto3 client factories, bucket ensure) live in **`graflo.object_storage`**. See **docs/concepts/object_storage.md** in the repo for staging vs ingestion connectors.

## Memgraph

Memgraph is a high-performance, in-memory graph database that supports OpenCypher and uses the Bolt protocol.

**Connection Details:**
- Bolt port: `7687` (default, configurable via `MEMGRAPH_PORT` in `.env`)
- Uses Bolt protocol (bolt://) for connections
- Optional authentication (configurable via `MEMGRAPH_USER` and `MEMGRAPH_PASSWORD` in `.env`)

**Programmatic Connection:**
```python
from graflo.db.connection.onto import MemgraphConfig
config = MemgraphConfig.from_docker_env()
```
