import argparse
import logging
import time
from paho.mqtt.enums import MQTTProtocolVersion

# Local imports
import mqtt_tests as tests
from BenchmarkClient import *

def parse_arguments() -> argparse.Namespace:
    """Reads arguments from the command line
    
        Returns
        -------
        argparse.Namespace
            An object containing the arguments as named variables
    """
    
    parser = argparse.ArgumentParser(prog='MQTT Privacy Benchmark')
    parser.add_argument('-b', '--broker_address', required=True, help="The IP or FQDN of the broker")
    parser.add_argument('-c', '--num_clients', type=int, required=True, help="The number of clients to create")
    parser.add_argument('-p', '--port', type=int, default=1883, help="The port on which to connect (TLS not currently supported)")
    parser.add_argument('-t', '--timeout', type=int, default=10, help="Desired timeout for messages")
    parser.add_argument('-l', '--log_level', default="info", help="The level at which to log", choices=["error", "warning", "info", "debug"])
    
    return parser.parse_args()
    

def initialize_clients(client_count: int, mqtt_version: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv5) -> list[BenchmarkClient]:
    """Initializes a list of clients for user within the benchmark

    Parameters
    ----------
    client_count : int
        The number of brokers to instantiate
    mqtt_version : MQTTProtocolVersion, optional
        The version of MQTT the clients should utilize (default is MQTTv5)

    Returns
    -------
    dict[str, BenchmarkClient]
        A dictionary mapping client IDs to instantiated clients
    """
    
    client_list = list()
    
    # Starting at 1 index for clearer naming
    for i in range(1, client_count + 1):
        client_id = f'Benchmark_Client_{i}'
        client = BenchmarkClient(client_id=client_id, mqtt_version=mqtt_version)
        client_list.append(client)
    
    return client_list



def main():
    
    # Load command line arguments and configure logging
    opts = parse_arguments()
    log_level = logging.getLevelNamesMapping()[opts.log_level.upper()]
    logging.basicConfig(format='%(name)s - %(levelname)s: %(message)s', level=log_level)
    
    # Setup the tests
    print('Creating clients...')
    clients = initialize_clients(client_count=opts.num_clients)
    for client in clients:
        client.connect(broker_address=opts.broker_address, port=opts.port, clean_start=True)

    # Wait for clients to connect or fail
    connect_pending = True
    while(connect_pending):
        clients_waiting = sum(x.connect_waiting for x in clients)
        connect_pending = (clients_waiting != 0)
        time.sleep(1)

    # Check number of connected clients
    connected_count = sum(x.is_connected() for x in clients)
    print(f'{connected_count} of {opts.num_clients} clients connected successfully!')

    # Now start running the tests

    # Test 1, Purpose Management
    tests.test_purpose_management_correctness(clients=clients, method=PurposeManagementMethod.PM_1)
    tests.test_purpose_management_speed(clients=clients, method=PurposeManagementMethod.PM_1)

if __name__ == "__main__":
    main()