#!/bin/bash
# Script to start all docker compose services for graflo

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

echo -e "${GREEN}All services started!${NC}"
echo ""
echo "To check status, run: docker ps"
echo "To stop all services, run: ./stop-all.sh"

