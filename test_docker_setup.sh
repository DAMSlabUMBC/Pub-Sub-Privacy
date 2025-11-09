#!/bin/bash
# Just a quick test to make sure Docker setup is working

set -e

echo "================================================================================"
echo "TESTING DOCKER SETUP"
echo "================================================================================"

# Make sure Docker is actually installed
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "ERROR: docker-compose is not installed"
    exit 1
fi

echo "✓ Docker and docker-compose are installed"

# Make sure all the files we need are here
REQUIRED_FILES=(
    "docker-compose.yml"
    "Dockerfile.benchmark"
    "run_all_tests.sh"
    "run_test_with_metrics.sh"
    "requirements.txt"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "ERROR: Missing required file: $file"
        exit 1
    fi
done

echo "✓ All required files present"

# Try building the containers
echo ""
echo "Building Docker containers..."
if docker-compose build 2>&1 | tail -20; then
    echo "✓ Containers built successfully"
else
    echo "ERROR: Failed to build containers"
    exit 1
fi

# Fire them up
echo ""
echo "Starting containers..."
if docker-compose up -d; then
    echo "✓ Containers started"
else
    echo "ERROR: Failed to start containers"
    exit 1
fi

# Give the services a bit to get ready
echo ""
echo "Waiting for services to initialize..."
sleep 10

# See what's running
echo ""
echo "Container status:"
docker-compose ps

# Check what's happening in the logs
echo ""
echo "Recent logs from benchmark-runner:"
docker logs benchmark-runner --tail 50

echo ""
echo "================================================================================"
echo "TEST COMPLETE"
echo "================================================================================"
echo ""
echo "Next steps:"
echo "  1. View live logs:    docker logs -f benchmark-runner"
echo "  2. Check results:     ls -lh results/"
echo "  3. Stop containers:   docker-compose down"
echo ""
