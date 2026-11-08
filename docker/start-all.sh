#!/bin/bash
# Start the GraFlo test services: all of them, or the ones named.
#   ./start-all.sh                 # every service
#   ./start-all.sh neo4j postgres  # just these
set -e
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
select_services "$@"

echo "Starting GraFlo docker services: ${SERVICES[*]}"
echo ""
for service in "${SERVICES[@]}"; do
    require_service "$service"
    echo -e "${GREEN}Starting $service${NC} (profile $(setting "$service" SPEC).$service)"
    compose "$service" up -d
    echo ""
done

# Block until every container with a healthcheck has left "starting", so tests
# run right after this script do not race service startup.
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300}"
echo "Waiting for health checks (up to ${WAIT_TIMEOUT}s)..."
deadline=$((SECONDS + WAIT_TIMEOUT))
health_list() {
    for service in "${SERVICES[@]}"; do
        docker ps --filter "label=com.docker.compose.project=$service" --filter "health=$1" --format '{{.Names}}'
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
echo -e "${GREEN}Started.${NC} Check with: docker ps. Stop with: ./stop-all.sh"
