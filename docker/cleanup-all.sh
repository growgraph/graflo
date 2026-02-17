#!/bin/bash
# Script to cleanup all docker compose services for graflo
# Removes containers, volumes, and optionally images

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Database directories
DATABASES=("arango" "neo4j" "postgres" "falkordb" "memgraph" "nebula" "tigergraph" "fuseki")

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse command line arguments
REMOVE_IMAGES=false
REMOVE_VOLUMES=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --images|-i)
            REMOVE_IMAGES=true
            shift
            ;;
        --no-volumes|-nv)
            REMOVE_VOLUMES=false
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --images, -i       Also remove docker images (default: false)"
            echo "  --no-volumes, -nv  Don't remove volumes (default: removes volumes)"
            echo "  --help, -h         Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                 # Remove containers and volumes"
            echo "  $0 --images        # Remove containers, volumes, and images"
            echo "  $0 --no-volumes    # Remove containers only"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}Cleaning up all GraFlo docker compose services...${NC}"
if [ "$REMOVE_IMAGES" = true ]; then
    echo -e "${YELLOW}⚠️  Will also remove docker images${NC}"
fi
if [ "$REMOVE_VOLUMES" = false ]; then
    echo -e "${YELLOW}⚠️  Volumes will be preserved${NC}"
fi
echo ""

# Confirm before proceeding
read -p "Are you sure you want to proceed? (yes/no): " -r
echo
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "Aborted."
    exit 0
fi

for db in "${DATABASES[@]}"; do
    if [ ! -d "$db" ]; then
        echo -e "${YELLOW}Warning: Directory $db not found, skipping...${NC}"
        continue
    fi
    
    echo -e "${GREEN}Cleaning up $db...${NC}"
    cd "$db"
    
    # Check if .env file exists
    if [ -f ".env" ]; then
        # Extract SPEC from .env file (default to 'graflo' if not found)
        SPEC=$(grep -E "^SPEC=" .env 2>/dev/null | cut -d'=' -f2 | tr -d '"' || echo "graflo")
        PROFILE="${SPEC}.${db}"
        
        # Build docker compose command
        COMPOSE_CMD="docker compose --env-file .env --profile $PROFILE"
    else
        # For services without .env, try to infer profile
        PROFILE="graflo.${db}"
        COMPOSE_CMD="docker compose --profile $PROFILE"
    fi
    
    # Remove containers and optionally volumes
    if [ "$REMOVE_VOLUMES" = true ]; then
        echo "  Removing containers and volumes..."
        $COMPOSE_CMD down -v 2>/dev/null || {
            echo -e "${YELLOW}  No containers to remove for $db${NC}"
        }
    else
        echo "  Removing containers (keeping volumes)..."
        $COMPOSE_CMD down 2>/dev/null || {
            echo -e "${YELLOW}  No containers to remove for $db${NC}"
        }
    fi
    
    # Remove images if requested
    if [ "$REMOVE_IMAGES" = true ]; then
        echo "  Removing images..."
        # Try to get image name from docker-compose.yml
        if [ -f ".env" ]; then
            IMAGE_VERSION=$(grep -E "^IMAGE_VERSION=" .env 2>/dev/null | cut -d'=' -f2 | tr -d '"' || echo "")
            if [ -n "$IMAGE_VERSION" ]; then
                docker rmi "$IMAGE_VERSION" 2>/dev/null || {
                    echo -e "${YELLOW}  Image $IMAGE_VERSION not found or in use${NC}"
                }
            fi
        fi
        
        # Special handling for nebula (multiple images)
        if [ "$db" = "nebula" ] && [ -f ".env" ]; then
            NEBULA_VERSION=$(grep -E "^NEBULA_VERSION=" .env 2>/dev/null | cut -d'=' -f2 | tr -d '"' || echo "")
            if [ -n "$NEBULA_VERSION" ]; then
                for img in "vesoft/nebula-metad:${NEBULA_VERSION}" "vesoft/nebula-storaged:${NEBULA_VERSION}" "vesoft/nebula-graphd:${NEBULA_VERSION}" "vesoft/nebula-graph-studio:${NEBULA_VERSION}"; do
                    docker rmi "$img" 2>/dev/null || {
                        echo -e "${YELLOW}  Image $img not found or in use${NC}"
                    }
                done
            fi
        fi
        
        # Special handling for postgres (hardcoded image)
        if [ "$db" = "postgres" ]; then
            docker rmi "postgres:16-alpine" 2>/dev/null || {
                echo -e "${YELLOW}  Image postgres:16-alpine not found or in use${NC}"
            }
        fi
    fi
    
    cd ..
    echo ""
done

echo -e "${GREEN}Cleanup complete!${NC}"
if [ "$REMOVE_IMAGES" = false ]; then
    echo ""
    echo "Note: Images were preserved. Use --images flag to remove them."
fi

