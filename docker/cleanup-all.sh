#!/bin/bash
# Remove the GraFlo test services' containers and volumes (and, with --images,
# their images): all services, or the ones named.
#   ./cleanup-all.sh                    # every service, containers and volumes
#   ./cleanup-all.sh --images neo4j     # neo4j, with its images
#   ./cleanup-all.sh --no-volumes       # containers only
#   ./cleanup-all.sh --yes              # no confirmation prompt
set -e
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

REMOVE_IMAGES=false
REMOVE_VOLUMES=true
ASSUME_YES=false
NAMES=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --images|-i) REMOVE_IMAGES=true ;;
        --no-volumes|-nv) REMOVE_VOLUMES=false ;;
        --yes|-y) ASSUME_YES=true ;;
        --help|-h)
            sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        -*) echo -e "${RED}Unknown option: $1${NC}"; exit 1 ;;
        *) NAMES+=("$1") ;;
    esac
    shift
done
select_services "${NAMES[@]}"

echo -e "${BLUE}Cleaning up GraFlo docker services: ${SERVICES[*]}${NC}"
[ "$REMOVE_IMAGES" = true ] && echo -e "${YELLOW}Will also remove images${NC}"
[ "$REMOVE_VOLUMES" = false ] && echo -e "${YELLOW}Volumes will be kept${NC}"
if [ "$ASSUME_YES" = false ]; then
    read -p "Proceed? (yes/no): " -r
    [[ $REPLY =~ ^[Yy][Ee][Ss]$ ]] || { echo "Aborted."; exit 0; }
fi

for service in "${SERVICES[@]}"; do
    require_service "$service"
    echo -e "${GREEN}Cleaning up $service${NC}"
    args=(down)
    [ "$REMOVE_VOLUMES" = true ] && args+=(--volumes)
    [ "$REMOVE_IMAGES" = true ] && args+=(--rmi all)
    compose "$service" "${args[@]}" || echo -e "${YELLOW}  Nothing to remove for $service${NC}"
done
echo ""
echo -e "${GREEN}Cleanup complete.${NC}"
