#!/bin/bash
# Runs a benchmark test and calculates the metrics automatically

set -e

# Make sure we got the right args
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <config_file> <broker_address> [broker_port]"
    echo "Example: $0 configs/config-new.cfg localhost 1883"
    exit 1
fi

CONFIG_FILE="$1"
BROKER_ADDRESS="$2"
BROKER_PORT="${3:-1883}"

# Start up the venv
source venv/bin/activate

# Pull out the config name so we can use it for naming stuff
CONFIG_NAME=$(basename "$CONFIG_FILE" .cfg)

echo "================================================================================"
echo "Running benchmark test: $CONFIG_NAME"
echo "================================================================================"

# Actually run the test
python3 benchmark/Benchmark.py run "$CONFIG_FILE" "$BROKER_ADDRESS" -p "$BROKER_PORT" -v

# Grab the latest log file for this config
LOG_FILE=$(ls -t logs/${CONFIG_NAME}_*.log 2>/dev/null | head -n 1)

if [ -z "$LOG_FILE" ]; then
    echo "ERROR: No log file found for $CONFIG_NAME"
    exit 1
fi

echo ""
echo "================================================================================"
echo "Test complete! Log file: $LOG_FILE"
echo "================================================================================"
echo ""
echo "Calculating metrics..."
echo ""

# Make the results dir if it doesn't exist yet
mkdir -p results

# Run the metrics calculation
RESULTS_CSV="results/${CONFIG_NAME}_$(date +%Y-%m-%d_%H-%M-%S)_metrics.csv"
python3 benchmark/calculate_metrics.py "$LOG_FILE" \
    --test-name "$CONFIG_NAME" \
    --csv-output "$RESULTS_CSV"

echo ""
echo "================================================================================"
echo "DONE!"
echo "================================================================================"
echo "Log file:     $LOG_FILE"
echo "Metrics CSV:  $RESULTS_CSV"
echo "================================================================================"
