#!/bin/bash
# Script to start all docker compose services for graflo

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Database directories
DATABASES=("arango" "neo4j" "postgres" "falkordb" "memgraph" "nebula" "tigergraph" "fuseki" "minio" "kafka")

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "Starting all GraFlo docker compose services..."
echo ""

for db in "${DATABASES[@]}"; do
    if [ ! -d "$db" ]; then
        echo -e "${YELLOW}Warning: Directory $db not found, skipping...${NC}"
        continue
    fi

    echo -e "${GREEN}Starting $db...${NC}"
    cd "$db"

    # Check if .env file exists
    if [ -f ".env" ]; then
        # Extract SPEC from .env file (default to 'graflo' if not found)
        SPEC=$(grep -E "^SPEC=" .env 2>/dev/null | cut -d'=' -f2 | tr -d '"' || echo "graflo")
        PROFILE="${SPEC}.${db}"

        echo "  Using profile: $PROFILE"
        docker compose --env-file .env --profile "$PROFILE" up -d
    else
        # For services without .env, try to infer profile
        # Default SPEC to 'graflo' if not set
        PROFILE="graflo.${db}"

        echo "  No .env file found, using profile: $PROFILE"
        echo "  ${YELLOW}Warning: Some services may require .env file for proper configuration${NC}"
        docker compose --profile "$PROFILE" up -d || {
            echo -e "${RED}Failed to start $db${NC}"
            cd ..
            continue
        }
    fi

    cd ..
    echo ""
done

# Block until every container with a healthcheck has left "starting", so tests
# run right after this script do not race service startup.
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300}"
echo "Waiting for health checks (up to ${WAIT_TIMEOUT}s)..."
deadline=$((SECONDS + WAIT_TIMEOUT))
health_list() {
    for db in "${DATABASES[@]}"; do
        docker ps --filter "label=com.docker.compose.project=$db" --filter "health=$1" --format '{{.Names}}'
    done
}
while [ -n "$(health_list starting)" ]; do
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo -e "${YELLOW}Still starting after ${WAIT_TIMEOUT}s:${NC} $(health_list starting | tr '\n' ' ')"
        break
    fi
    sleep 2
done
unhealthy="$(health_list unhealthy)"
if [ -n "$unhealthy" ]; then
    echo -e "${RED}Unhealthy:${NC} $(echo "$unhealthy" | tr '\n' ' ')"
fi
echo ""

echo -e "${GREEN}All services started!${NC}"
echo ""
echo "To check status, run: docker ps"
echo "To stop all services, run: ./stop-all.sh"
