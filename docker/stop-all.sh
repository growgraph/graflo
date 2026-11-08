#!/bin/bash
# Stop the GraFlo test services: all of them, or the ones named. Containers
# and volumes are kept; ./cleanup-all.sh removes them.
set -e
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
select_services "$@"

echo "Stopping GraFlo docker services: ${SERVICES[*]}"
echo ""
for service in "${SERVICES[@]}"; do
    require_service "$service"
    echo -e "${GREEN}Stopping $service${NC}"
    compose "$service" stop || echo -e "${YELLOW}  No running containers for $service${NC}"
done
echo ""
echo -e "${GREEN}Stopped.${NC} Remove containers and volumes with: ./cleanup-all.sh"
