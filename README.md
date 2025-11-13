# MQTT-DAP Benchmark

Benchmark framework for evaluating MQTT brokers with purpose-based access control and operational request handling.

Designed to test the [MQTT-DAP-Mosquitto](https://github.com/DAMSlabUMBC/dams-mosquitto/tree/develop) broker implementation.

## Overview

This framework tests MQTT brokers with DAP (Data Access and Processing) extensions:

- **Purpose-Based Access Control (PBAC)**: Verifies messages are only delivered to subscribers with permitted purposes
- **Operational Requests**: Tests GDPR data subject rights (Access, Portability, Erasure, Rectification, Restriction, Objection, Automated Decisions)
- **Realistic IoT Workloads**: Simulates smart city and industrial scenarios with variable device counts, payload sizes, and publishing rates
- **Multiple Purpose Management Methods**: Supports baseline (no PM) and four purpose management approaches (PM1-PM4)

## Features

- Deterministic event scheduling for reproducible tests
- Variable payloads from small sensor data to large LIDAR scans
- Dynamic purpose changes during runtime
- Client disconnection/reconnection patterns
- Comprehensive metrics: system resources, messaging performance, purpose correctness, operational correctness
- Docker integration with automatic broker restart between tests

## Installation

### Prerequisites
- Python 3.8+
- pip

### Install Dependencies

```bash
pip install -r benchmark/requirements.txt
```

Or use a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r benchmark/requirements.txt
```

## Usage

### Run a Test

```bash
python3 benchmark/Benchmark.py run <config_file> <broker_address> [options]
```

**Arguments:**
- `config_file`: Path to YAML test configuration
- `broker_address`: IP address or hostname of MQTT broker
- `-p, --port`: Broker port (default: 1883)
- `-o, --logfile`: Custom log file path
- `-v, --verbose`: Enable verbose logging

**Example:**
```bash
python3 benchmark/Benchmark.py run test-configs/set1_city_static_10p_1subs_pm1.cfg localhost -v
```

### Analyze Results

Process a log file to calculate metrics:

```bash
python3 benchmark/Benchmark.py analyze <log_file> [-o OUTPUT_FILE]
```

**Example:**
```bash
python3 benchmark/Benchmark.py analyze logs/set1_city_static_10p_1subs_pm1_2024-11-13_15-30-00.log
```

### Docker Usage

Build all Docker images:
```bash
./build_all.sh
```

Run all tests across all purpose management methods:
```bash
./run_all_broker_tests.sh
```

This runs tests for baseline, PM1, PM2, PM3, and PM4 brokers with automatic broker restart between tests and parallel log analysis.

Run tests for a specific PM method:
```bash
./run_baseline_tests.sh  # No purpose management
./run_pm1_tests.sh       # Registration by Message
./run_pm2_tests.sh       # Registration by Subscription
./run_pm3_tests.sh       # System-Managed Purposes
./run_pm4_tests.sh       # Hybrid Purpose Management
```

## Test Configuration

Test configurations are YAML files defining:
- Device instances (publishers/subscribers with device types, purposes, publishing rates)
- Purpose definitions and hierarchy
- Scheduled events (purpose changes, disconnections, operational requests)
- Test parameters (duration, log directory, purpose management method)

See `test-configs/` directory for examples.

## Metrics

### Broker Statistics
- CPU and memory usage (min, max, average, variance)

### Messaging Performance
- Latency (min, max, average, variance in milliseconds)
- Throughput (messages per second)
- Header overhead (average MQTT header size in bytes)

### Purpose Correctness
Per subscriber:
- False accept rate (messages received without matching purpose)
- False reject rate (expected messages not received due to purpose mismatch)

### Operational Request Correctness
Per operation type:
- Coverage (percentage of target data successfully retrieved)
- Leakage (unauthorized data included in responses)
- Completion (percentage of requests receiving responses)
- Lost responses (requests with missing/incomplete responses)

### Category Breakdown
- C1_REG: Data registered during test execution
- C2: Historical data from previous connections
- C3: Data from currently connected sessions

All metrics are exported to CSV for comparison across runs.

## Architecture

1. **ConfigParser** (`ConfigParser.py`): Parses YAML test configurations
2. **TestExecutor** (`TestExecutor.py`): Manages test lifecycle, MQTT clients, and event scheduling
3. **LoggingModule** (`LoggingModule.py`): Asynchronously logs events to disk
4. **MetricsCalculator** (`MetricsCalculator.py`): Post-processes logs to compute performance metrics

## Dependencies

See `benchmark/requirements.txt` for the complete list. Key dependencies:
- paho-mqtt 2.1.0
- PyYAML 6.0.2
- rich 14.0.0

## License

GNU General Public License v3.0 - See [LICENSE](./LICENSE)
