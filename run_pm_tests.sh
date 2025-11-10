#!/bin/bash
# Runs all tests for one PM method (baseline, pm1, pm2, pm3, or pm4)
# Just pass the PM method as an arg and it figures out the rest

set -e

PM_METHOD="$1"

if [ -z "$PM_METHOD" ]; then
    echo "Usage: $0 <pm_method>"
    echo "Example: $0 baseline"
    echo "Example: $0 pm2"
    exit 1
fi

DOCKER_COMPOSE_FILE="docker-compose-${PM_METHOD}.yml"
BROKER_CONTAINER="benchmark-mosquitto-${PM_METHOD}"
RUNNER_CONTAINER="benchmark-runner-${PM_METHOD}"
RESULTS_DIR="results/${PM_METHOD}"
LOGS_DIR="logs/${PM_METHOD}"

# Some colors to make output easier to read
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PM_NAMES=(
    ["baseline"]="Baseline (No PM)"
    ["pm1"]="PM1 (Registration by Message)"
    ["pm2"]="PM2 (Registration by Subscription)"
    ["pm3"]="PM3 (System-Managed Purposes)"
    ["pm4"]="PM4 (Hybrid Purpose Management)"
)

echo "================================================================================"
echo "${PM_NAMES[$PM_METHOD]:-$PM_METHOD} TESTS"
echo "================================================================================"
echo "Start time: $(date)"
echo ""

# Make sure we have the directories
mkdir -p "$RESULTS_DIR" "$LOGS_DIR"

# Fire up the containers
echo "Starting ${PM_METHOD} broker and runner..."
docker-compose -f "$DOCKER_COMPOSE_FILE" up -d

# Give the broker a sec to get ready
echo "Waiting for broker to initialize..."
sleep 10

# Go find all the test configs for this PM method
if [ "$PM_METHOD" = "baseline" ]; then
    CONFIG_FILES=$(find configs -name "*baseline*.cfg" -o -name "*pm0*.cfg" 2>/dev/null | sort)
else
    CONFIG_FILES=$(find configs -name "*${PM_METHOD}*.cfg" 2>/dev/null | sort)
fi

if [ -z "$CONFIG_FILES" ]; then
    echo "${YELLOW}No ${PM_METHOD} test configs found. Checking for PM method in files...${NC}"
    # Try to find configs by looking inside the files for the PM method number
    PM_NUM=$(echo "$PM_METHOD" | grep -o '[0-9]' | head -1)
    if [ -n "$PM_NUM" ]; then
        CONFIG_FILES=$(grep -l "purpose_management_method: ${PM_NUM}" configs/*.cfg 2>/dev/null | sort)
    fi
fi

if [ -z "$CONFIG_FILES" ]; then
    echo "${RED}ERROR: No ${PM_METHOD} test configs found${NC}"
    docker-compose -f "$DOCKER_COMPOSE_FILE" down
    exit 1
fi

TOTAL_TESTS=$(echo "$CONFIG_FILES" | wc -l | tr -d ' ')
echo "Found $TOTAL_TESTS ${PM_METHOD} test(s)"
echo "================================================================================"
echo ""

TEST_NUM=0
PASSED=0
FAILED=0

# Go through each test
for CONFIG in $CONFIG_FILES; do
    TEST_NUM=$((TEST_NUM + 1))
    CONFIG_NAME=$(basename "$CONFIG" .cfg)

    echo ""
    echo "${YELLOW}[$TEST_NUM/$TOTAL_TESTS]${NC} Running test: $CONFIG_NAME"
    echo "--------------------------------------------------------------------------------"

    # Restart the broker between tests to clear out stored purposes/clients
    if [ $TEST_NUM -gt 1 ]; then
        echo "Restarting broker to clean state..."
        docker restart "$BROKER_CONTAINER"
        sleep 5
    fi

    # Run the test (but don't analyze yet, that happens later in parallel)
    if docker exec "$RUNNER_CONTAINER" ./run_test_no_analyze.sh "$CONFIG" mosquitto 1883; then
        echo "${GREEN}✓ PASSED${NC}: $CONFIG_NAME"
        PASSED=$((PASSED + 1))

        # Grab the logs from the container
        docker cp "$RUNNER_CONTAINER:/app/logs/${CONFIG_NAME}_"* "$LOGS_DIR/" 2>/dev/null || true
    else
        echo "${RED}✗ FAILED${NC}: $CONFIG_NAME"
        FAILED=$((FAILED + 1))
    fi

    sleep 2
done

# Print out the summary
echo ""
echo "================================================================================"
echo "${PM_METHOD} TESTS COMPLETE"
echo "================================================================================"
echo "End time: $(date)"
echo ""
echo "Results:"
echo "  Total tests:   $TOTAL_TESTS"
echo "  Passed:        ${GREEN}$PASSED${NC}"
echo "  Failed:        ${RED}$FAILED${NC}"
echo ""
echo "Logs saved to: $LOGS_DIR/"
echo "================================================================================"
echo ""
echo "Next steps:"
echo "  1. Analyze logs:      ./analyze_logs.sh $LOGS_DIR"
echo "  2. Stop containers:   docker-compose -f $DOCKER_COMPOSE_FILE down"
echo ""

if [ $FAILED -gt 0 ]; then
    exit 1
else
    exit 0
fi
