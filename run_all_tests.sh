#!/bin/bash
# Runs all the benchmark tests and calculates metrics for everything

set -e

# Setup
BROKER_ADDRESS="${BROKER_ADDRESS:-localhost}"
BROKER_PORT="${BROKER_PORT:-1883}"
RESULTS_DIR="results"
LOGS_DIR="logs"
SUMMARY_FILE="$RESULTS_DIR/test_summary_$(date +%Y-%m-%d_%H-%M-%S).txt"

# Make sure we have the directories
mkdir -p "$RESULTS_DIR" "$LOGS_DIR"

# Give the broker a bit to get ready
echo "Waiting for broker to be ready..."
sleep 15

# Some colors to make the output easier to read
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Go find all the test configs
echo "================================================================================"
echo "AUTOMATED BENCHMARK TEST SUITE"
echo "================================================================================"
echo "Start time: $(date)"
echo "Broker: $BROKER_ADDRESS:$BROKER_PORT"
echo ""

# Look for config files (but skip templates and examples)
CONFIG_FILES=$(find configs -name "*.cfg" ! -name "*template*" ! -path "*/examples/*" | sort)
TOTAL_TESTS=$(echo "$CONFIG_FILES" | wc -l | tr -d ' ')

if [ "$TOTAL_TESTS" -eq 0 ]; then
    echo "${RED}ERROR: No test config files found in configs/${NC}"
    exit 1
fi

echo "Found $TOTAL_TESTS test configuration(s)"
echo "================================================================================"
echo ""

# Set up the counters
TEST_NUM=0
PASSED=0
FAILED=0
FAILED_TESTS=""

# Start writing the summary file
{
    echo "BENCHMARK TEST SUITE SUMMARY"
    echo "============================"
    echo "Start time: $(date)"
    echo "Broker: $BROKER_ADDRESS:$BROKER_PORT"
    echo "Total tests: $TOTAL_TESTS"
    echo ""
    echo "Test Results:"
    echo "-------------"
} > "$SUMMARY_FILE"

# Go through each test
for CONFIG in $CONFIG_FILES; do
    TEST_NUM=$((TEST_NUM + 1))
    CONFIG_NAME=$(basename "$CONFIG" .cfg)

    echo ""
    echo "${YELLOW}[$TEST_NUM/$TOTAL_TESTS]${NC} Running test: $CONFIG_NAME"
    echo "--------------------------------------------------------------------------------"

    # Run the test and calculate metrics
    if ./run_test_with_metrics.sh "$CONFIG" "$BROKER_ADDRESS" "$BROKER_PORT" 2>&1 | tee "$LOGS_DIR/${CONFIG_NAME}_run.log"; then
        echo "${GREEN}✓ PASSED${NC}: $CONFIG_NAME"
        PASSED=$((PASSED + 1))
        echo "✓ PASSED: $CONFIG_NAME" >> "$SUMMARY_FILE"
    else
        echo "${RED}✗ FAILED${NC}: $CONFIG_NAME"
        FAILED=$((FAILED + 1))
        FAILED_TESTS="$FAILED_TESTS\n  - $CONFIG_NAME"
        echo "✗ FAILED: $CONFIG_NAME" >> "$SUMMARY_FILE"
    fi

    # Give it a little break between tests
    sleep 2
done

# Figure out the success rate
SUCCESS_RATE=$(awk "BEGIN {printf \"%.1f\", ($PASSED / $TOTAL_TESTS) * 100}")

# Final summary
echo ""
echo "================================================================================"
echo "TEST SUITE COMPLETE"
echo "================================================================================"
echo "End time: $(date)"
echo ""
echo "Results:"
echo "  Total tests:   $TOTAL_TESTS"
echo "  Passed:        ${GREEN}$PASSED${NC}"
echo "  Failed:        ${RED}$FAILED${NC}"
echo "  Success rate:  $SUCCESS_RATE%"
echo ""

# Write the summary to the file
{
    echo ""
    echo "Summary:"
    echo "--------"
    echo "End time: $(date)"
    echo "Total tests: $TOTAL_TESTS"
    echo "Passed: $PASSED"
    echo "Failed: $FAILED"
    echo "Success rate: $SUCCESS_RATE%"
} >> "$SUMMARY_FILE"

if [ $FAILED -gt 0 ]; then
    echo "Failed tests:"
    echo -e "$FAILED_TESTS"
    echo "" >> "$SUMMARY_FILE"
    echo "Failed tests:" >> "$SUMMARY_FILE"
    echo -e "$FAILED_TESTS" >> "$SUMMARY_FILE"
fi

echo ""
echo "Output locations:"
echo "  Logs:     $LOGS_DIR/"
echo "  Results:  $RESULTS_DIR/"
echo "  Summary:  $SUMMARY_FILE"
echo "================================================================================"

# Return the right exit code
if [ $FAILED -gt 0 ]; then
    exit 1
else
    exit 0
fi
