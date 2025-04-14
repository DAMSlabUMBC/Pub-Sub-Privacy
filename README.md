# MQTT-DAP-Benchmark

A benchmarking framework for [**MQTT-DAP-Mosquitto-GDPR (MQTT Data Access and Processing)**](https://github.com/DAMSlabUMBC/dams-mosquitto/tree/develop)
, designed to evaluate broker preformance under realistic conditions.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Running the Benchmark](#running-the-benchmark)
- [Client Interface](#client-interface)
- [Metrics](#metrics)
- [Configuration](#configuration)
- [Dependencies](#dependencies)
- [License](#license)

---

## Overview

This benchmark addresses testing MQTT-DAP-Mosquitto-GDPR by:

- Supporting custom MQTT-DAP control packets
- Simulating real-world IoT environments
- Enabling testing of purpose-based access control mechanisms
- Supporting operational requests across distributed clients with variable behaviors

---

## Key Features

- Support for client disconnection/reconnection patterns  
- Variable payload sizes and publishing rates  
- Testing for different purpose management approaches  
- Evaluation of operational request coverage  
- Distributed testing capabilities  

---

## Architecture

The benchmark is composed of five components:

1. **Management Module**  
   Parses configuration files and controls the benchmark node lifecycle.

2. **Synchronization Module**  
   Coordinates test state across distributed benchmark nodes to ensure proper initialization and completion.

3. **Test Execution Module**  
   Runs test logic, manages clients, sends/receives messages, evaluates purposes, and invokes operations.

4. **Result Logging Module**  
   Asynchronously logs results during test execution.

5. **Results Calculation Module**  
   Aggregates data across nodes to compute performance metrics.

---

## Running the Benchmark

```bash
# Run the benchmark with a configuration file and broker IP address
python3 benchmark/Benchmark.py run /configs/<conf_file> <broker_ip>
```
## Client Interface

To integrate a new MQTT-DAP client, implement the following functions as defined in `ClientInterface.py`:

- `connect(client_id, broker_address, broker_port)`  
  Connect the client to the specified MQTT broker.

- `disconnect()`  
  Disconnect the client from the MQTT broker.

- `subscribe(topic, qos, purpose)`  
  Subscribe to a topic with a specified QoS level and purpose metadata.

- `unsubscribe(topic)`  
  Unsubscribe from a given topic.

- `publish(topic, payload, qos, purpose)`  
  Publish a message to a topic with an associated QoS level and purpose.

- `define_purpose(purpose_id, purpose_properties)`  
  Define a purpose with specific properties for access control.

- `invoke_operation(operation, targets, arguments)`  
  Invoke a distributed operation across client targets.

- `receive_callback(callback_function)`  
  Set a callback to handle incoming messages during test execution.

---

## Metrics

The benchmark measures the following performance and correctness indicators:

- **Message Latency**  
  The time elapsed between message publication and receipt.

- **Message Throughput**  
  The number of messages received per second.

- **PBAC Correctness**  
  The accuracy of Purpose-Based Access Control filtering.

- **Operational Coverage**  
  The success rate of completed operational requests.

---

## Dependencies

The MQTT-DAP Benchmark requires the following Python packages:

- `ischedule==1.2.7`  
- `markdown-it-py==3.0.0`  
- `mdurl==0.1.2`  
- `paho-mqtt==2.1.0`  
- `Pygments==2.19.1`  
- `PyYAML==6.0.2`  
- `rich==14.0.0`  
- `sortedcontainers==2.4.0`  

You can install all dependencies using `pip`:

```bash
pip install -r benchmark/requirements.txt
```
## License

This project is licensed under the **GNU General Public License v3.0**.  
See the [LICENSE](./LICENSE) file for details.
