# Running docker images of various graph databases

## General instruction

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
