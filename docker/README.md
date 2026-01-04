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
docker exec -it <containter_name sh
```

## Tigergraph Instruction

Tigergraph does not provide their Community Edition on dockerhub, so (as of 2025-10) select your version here [https://download.tigergraph.com/](https://download.tigergraph.com/), fill out the form and download a `.tar.gz` image file following the link in an email. Then follow:

```shell
   docker load -i ./tigergraph-4.2.0-community-docker-image.tar.gz
```

after this run tigergraph in the same way as other DBs:

```shell
docker compose --env-file .env up <container_spec> -d
```

NB: we only use default password `tigergraph` in this repo (our docker-compose script does not change default password).

## Nebula
Nebula requires several services, run as 

```sh
docker compose --env-file .env --profile test.nebula up
```

Use `nebula-graphd` host to connect using GraphStudio.

## Arangoshell

Arango web interface [http://localhost:ARANGO_PORT](http://localhost:8535). NB: the standard arango port is 8529, `.env` config in graflo uses 8535.


## neo4j shell

Neo4j web interface [http://localhost:NEO4J_PORT](http://localhost:7475). NB: the standard neo4j port is 7474, `.env` config in graflo uses 7475.


## Tigergraph

```shell
       docker load -i ./tigergraph-4.2.0-community-docker-image.tar.gz # the xxx.gz file name are what you have downloaded. Change the gz file name depending on what you have downloaded
       docker images #find image id
       docker run -d --init -p 14240:14240 --name tigergraph tigergraph/community:4.2.0 #Run a container named tigergraph from the imported image
       docker exec -it tigergraph /bin/bash #start a shell on this container. 
       gadmin start all  #start all tigergraph component services
       gadmin status #should see all services are up.
```
Follow instructions given [here](https://github.com/tigergraph/ecosys/blob/master/demos/guru_scripts/docker/README.md#quick-start-for-community-edition) to work with community edition.


To 