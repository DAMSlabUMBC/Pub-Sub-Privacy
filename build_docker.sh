#!/bin/bash
# Builds all the Docker images we need for testing
# Just run this once before running tests

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "================================================================================"
echo "BUILDING ALL DOCKER IMAGES"
echo "================================================================================"
echo ""

# Build the MQTT-DAP mosquitto broker (this one is used for PM2-4)
echo "${YELLOW}Building MQTT-DAP broker (used for PM2-4)...${NC}"
echo "This will take 5-10 minutes the first time..."
docker compose -f docker-compose-pm2.yml build

echo ""
echo "${GREEN}✓ MQTT-DAP broker built${NC}"
echo ""

# Build the baseline mosquitto and the test runner
echo "${YELLOW}Building baseline and benchmark runner...${NC}"
docker compose -f docker-compose-baseline.yml build

echo ""
echo "${GREEN}✓ Baseline and runner built${NC}"
echo ""

echo "================================================================================"
echo "BUILD COMPLETE"
echo "================================================================================"
echo ""
echo "Next steps:"
echo "  Run all tests:        ./run_all_broker_tests.sh"
echo "  Run PM# tests only:   ./run_pm#_tests.sh"
echo ""
