#!/bin/bash
# Script to stop all docker compose services for graflo

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Database directories
DATABASES=("arango" "neo4j" "postgres" "falkordb" "memgraph" "nebula" "tigergraph")

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "Stopping all GraFlo docker compose services..."
echo ""

for db in "${DATABASES[@]}"; do
    if [ ! -d "$db" ]; then
        echo -e "${YELLOW}Warning: Directory $db not found, skipping...${NC}"
        continue
    fi
    
    echo -e "${GREEN}Stopping $db...${NC}"
    cd "$db"
    
    # Check if .env file exists
    if [ -f ".env" ]; then
        # Extract SPEC from .env file (default to 'graflo' if not found)
        SPEC=$(grep -E "^SPEC=" .env 2>/dev/null | cut -d'=' -f2 | tr -d '"' || echo "graflo")
        PROFILE="${SPEC}.${db}"
        
        docker compose --env-file .env --profile "$PROFILE" stop || {
            echo -e "${YELLOW}No running containers for $db${NC}"
        }
    else
        # For services without .env, try to infer profile
        PROFILE="graflo.${db}"
        
        docker compose --profile "$PROFILE" stop || {
            echo -e "${YELLOW}No running containers for $db${NC}"
        }
    fi
    
    cd ..
    echo ""
done

echo -e "${GREEN}All services stopped!${NC}"
echo ""
echo "To remove containers and volumes, run: ./cleanup-all.sh"
echo "To also remove images, run: ./cleanup-all.sh --images"

