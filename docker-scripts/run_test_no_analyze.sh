#!/bin/bash
# Runs a single test WITHOUT analyzing the logs
# Analysis happens later in parallel so we don't waste time waiting

set -e

# Make sure we got what we need
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <config_file> <broker_address> [broker_port]"
    echo "Example: $0 configs/config-new.cfg localhost 1883"
    exit 1
fi

CONFIG_FILE="$1"
BROKER_ADDRESS="$2"
BROKER_PORT="${3:-1883}"

# Start up venv if we're not in Docker
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

# Grab the config name for the log files
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
echo "Log file saved for later analysis: $LOG_FILE"
echo "================================================================================"
