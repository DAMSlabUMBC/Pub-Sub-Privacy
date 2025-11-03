import sys
import argparse
from os import path

sys.path.insert(0, path.dirname(path.abspath(__file__)))

import GlobalDefs
from DeterministicConfigParser import DeterministicConfigParser
from DeterministicTestExecutor import DeterministicTestExecutor, DeterministicTestConfiguration
from LoggingModule import ResultLogger
import importlib
import time


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Run deterministic pub/sub privacy benchmark tests'
    )
    parser.add_argument('config', help='Path to configuration file')
    parser.add_argument('broker_address', help='IP or FQDN of the broker')
    parser.add_argument('-p', '--port', type=int, default=1883,
                       help='Broker port (default: 1883)')
    parser.add_argument('-o', '--logfile', help='Log file path (optional)')

    args = parser.parse_args()

    # Validate arguments
    if not path.isfile(args.config):
        print(f"Error: Configuration file not found: {args.config}")
        return 1

    if not 1 <= args.port <= 65535:
        print(f"Error: Port must be in range [1-65535]")
        return 1

    print("=" * 80)
    print("Pub-Sub-Privacy Deterministic Benchmark")
    print("=" * 80)

    # Parse configuration
    print(f"\nLoading configuration from: {args.config}")
    config_parser = DeterministicConfigParser()
    try:
        benchmark_config = config_parser.parse_config(args.config)
    except Exception as e:
        print(f"Error: Failed to parse configuration: {e}")
        return 1

    # Set global configuration values
    GlobalDefs.REG_BY_MSG_REG_TOPIC = benchmark_config.reg_by_msg_reg_topic
    GlobalDefs.REG_BY_TOPIC_PUB_REG_TOPIC = benchmark_config.reg_by_topic_pub_reg_topic
    GlobalDefs.REG_BY_TOPIC_SUB_REG_TOPIC = benchmark_config.reg_by_topic_sub_reg_topic
    GlobalDefs.OR_TOPIC = benchmark_config.or_topic_name
    GlobalDefs.ORS_TOPIC = benchmark_config.ors_topic_name
    GlobalDefs.ON_TOPIC = benchmark_config.on_topic_name
    GlobalDefs.ONP_TOPIC = benchmark_config.onp_topic_name
    GlobalDefs.OSYS_TOPIC = benchmark_config.osys_topic__name
    GlobalDefs.OP_RESPONSE_TOPIC = benchmark_config.op_response_topic
    GlobalDefs.OP_PURPOSE = benchmark_config.op_purpose

    # Load client interface module
    module_name = benchmark_config.client_module_name
    print(f"Loading client module: {module_name}")
    try:
        GlobalDefs.CLIENT_MODULE = _load_client_module(module_name)
    except Exception as e:
        print(f"Error: Failed to load client module: {e}")
        return 1

    # Setup logging
    if args.logfile is None:
        timestring = time.strftime("%Y-%m-%d_%H-%M-%S")
        args.logfile = f"{benchmark_config.log_output_dir}/{benchmark_config.this_node_name}_{timestring}.log"

    print(f"Logging to: {args.logfile}")
    GlobalDefs.LOGGING_MODULE = ResultLogger()
    try:
        GlobalDefs.LOGGING_MODULE.start(args.logfile)
    except Exception as e:
        print(f"Error: Failed to initialize logging: {e}")
        return 1

    GlobalDefs.LOGGING_MODULE.log_pm_method(benchmark_config.method.value)

    # Create test executor
    print(f"\nConnecting to broker: {args.broker_address}:{args.port}")
    print(f"Using purpose management method: {benchmark_config.method.value}")

    executor = DeterministicTestExecutor(
        benchmark_config.this_node_name,
        args.broker_address,
        args.port,
        benchmark_config.method,
        benchmark_config.seed
    )

    # Run each test
    for test_config in benchmark_config.test_list:
        print("\n" + "=" * 80)

        # Check if deterministic mode
        if isinstance(test_config, DeterministicTestConfiguration) and test_config.use_deterministic_scheduling:
            print(f"Running DETERMINISTIC test: {test_config.name}")
            print("=" * 80)
            executor.setup_deterministic_test(test_config)
            executor.perform_deterministic_test(test_config)
        else:
            print(f"Running LEGACY test: {test_config.name}")
            print("=" * 80)
            executor.setup_test(test_config)
            executor.perform_test(test_config)

    # Shutdown
    GlobalDefs.LOGGING_MODULE.shutdown()

    print("\n" + "=" * 80)
    print(f"All tests completed successfully!")
    print(f"Results logged to: {args.logfile}")
    print("=" * 80)

    return 0


def _load_client_module(module_name: str):
    """Load and validate the client interface module"""
    module = importlib.import_module(module_name)
    for function in GlobalDefs.CLIENT_FUNCTIONS:
        if not hasattr(module, function):
            raise AttributeError(f"Could not find required function: {function}")
    return module


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(GlobalDefs.ExitCode.SIGINT_RECEIVED)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(GlobalDefs.ExitCode.UNKNOWN_ERROR)
