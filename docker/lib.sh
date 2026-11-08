#!/bin/bash
# Shared by start-all.sh, stop-all.sh and cleanup-all.sh; source it, don't run it.
#
# Each service is a compose project in its own folder. Its settings live in
# <service>/.env.example, which compose reads (--env-file) and graflo's
# from_docker_env() reads too, so the containers and the tests agree.

DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE=".env.example"
ALL_SERVICES=("arango" "neo4j" "postgres" "falkordb" "memgraph" "nebula" "tigergraph" "fuseki" "seaweedfs" "kafka")

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# The services named on the command line, or all of them; exits on an unknown name.
select_services() {
    if [ "$#" -eq 0 ]; then
        SERVICES=("${ALL_SERVICES[@]}")
        return
    fi
    SERVICES=()
    for name in "$@"; do
        if [[ ! " ${ALL_SERVICES[*]} " =~ " ${name} " ]]; then
            echo -e "${RED}Unknown service: $name${NC} (known: ${ALL_SERVICES[*]})"
            exit 1
        fi
        SERVICES+=("$name")
    done
}

# A setting of <service>'s settings file, quotes stripped.
setting() {
    grep -E "^$2=" "$DOCKER_DIR/$1/$ENV_FILE" | head -1 | cut -d'=' -f2- | tr -d '"' | tr -d "'"
}

# docker compose for <service> with its settings file and profile, then the rest of the arguments.
compose() {
    local service="$1"
    shift
    local spec
    spec="$(setting "$service" SPEC)"
    (cd "$DOCKER_DIR/$service" && docker compose --env-file "$ENV_FILE" --profile "${spec:-graflo}.${service}" "$@")
}

# Exits unless <service> has a compose file and a settings file.
require_service() {
    if [ ! -f "$DOCKER_DIR/$1/docker-compose.yml" ] || [ ! -f "$DOCKER_DIR/$1/$ENV_FILE" ]; then
        echo -e "${RED}$1: needs docker-compose.yml and $ENV_FILE in $DOCKER_DIR/$1${NC}"
        exit 1
    fi
}
