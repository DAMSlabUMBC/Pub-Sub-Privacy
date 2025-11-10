#!/bin/bash
# Runs tests for all the different broker types
# Tests run one broker at a time, then we analyze all logs in parallel at the end

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "================================================================================"
echo "COMPLETE BENCHMARK SUITE - ALL BROKER TYPES"
echo "================================================================================"
echo "Start time: $(date)"
echo ""
echo "This will run tests for:"
echo "  1. Baseline (no PM)"
echo "  2. PM1 (Registration by Message)"
echo "  3. PM2 (Registration by Subscription)"
echo "  4. PM3 (System-Managed Purposes)"
echo "  5. PM4 (Hybrid Purpose Management)"
echo ""
echo "Tests run one broker at a time, then all logs get analyzed in parallel."
echo "================================================================================"
echo ""

BROKER_RESULTS=()

# Run tests for each broker type
for PM in baseline pm1 pm2 pm3 pm4; do
    echo ""
    echo "${BLUE}>>> Running ${PM} tests...${NC}"

    if ./run_${PM}_tests.sh; then
        BROKER_RESULTS+=("${GREEN}✓ ${PM}${NC}")
    else
        BROKER_RESULTS+=("${RED}✗ ${PM}${NC}")
    fi

    # Stop and clean up before next broker
    docker-compose -f docker-compose-${PM}.yml down
    sleep 5
done

# Now analyze all the logs at once (in parallel)
echo ""
echo "================================================================================"
echo "ALL TESTS COMPLETE - STARTING PARALLEL LOG ANALYSIS"
echo "================================================================================"
echo ""

./analyze_logs.sh logs/baseline 4 &
./analyze_logs.sh logs/pm1 4 &
./analyze_logs.sh logs/pm2 4 &
./analyze_logs.sh logs/pm3 4 &
./analyze_logs.sh logs/pm4 4 &

echo "Waiting for all analyses to complete..."
wait

# Print the final summary
echo ""
echo "================================================================================"
echo "COMPLETE BENCHMARK SUITE - FINISHED"
echo "================================================================================"
echo "End time: $(date)"
echo ""
echo "Broker Test Results:"
for result in "${BROKER_RESULTS[@]}"; do
    echo "  $result"
done
echo ""
echo "Output locations:"
echo "  Logs:     logs/{baseline,pm1,pm2,pm3,pm4}/"
echo "  Results:  results/{baseline,pm1,pm2,pm3,pm4}/"
echo "================================================================================"
