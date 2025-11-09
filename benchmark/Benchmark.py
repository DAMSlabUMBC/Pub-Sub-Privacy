import sys
import argparse
from os import path

sys.path.insert(0, path.dirname(path.abspath(__file__)))

import GlobalDefs
from ConfigParser import ConfigParser, TestConfiguration
from DeterministicTestExecutor import DeterministicTestExecutor
from TestExecutor import TestExecutor
from LoggingModule import ResultLogger
import importlib
import time
from LoggingModule import console_log, ConsoleLogLevel


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Run MQTT-DAP benchmark tests'
    )

    subparsers = parser.add_subparsers(title='subcommands',
                                       help="Specify whether to run the benchmark or process benchmark outputs")
    subparsers.required = True
    subparsers.dest = "command"

    run_benchmark_parser = subparsers.add_parser("run")
    run_benchmark_parser.add_argument('config', help='Path to configuration file')
    run_benchmark_parser.add_argument('broker_address', help='IP or FQDN of the broker')
    run_benchmark_parser.add_argument('-p', '--port', type=int, default=1883,
                       help='Broker port (default: 1883)')
    run_benchmark_parser.add_argument('-o', '--logfile', help='Log file path (optional)')
    run_benchmark_parser.add_argument('-v', '--verbose', help='Verbose logging flag (optional)', action='store_true')
    
    analyze_results_parser = subparsers.add_parser("analyze")
    analyze_results_parser.add_argument("logfile", help="The path to log file to analyze")
    analyze_results_parser.add_argument("-o" "--outfile", dest="outfile", help="The file in which to store the results (default: 'BenchmarkResults_YYYY-MM-DD_HH-MM-SS.txt')")
    analyze_results_parser.add_argument('-v', '--verbose', help='Verbose logging flag (optional)', action='store_true')

    args = parser.parse_args()

    # Validate arguments
    if not _validate_arguments(args):
        sys.exit(GlobalDefs.ExitCode.BAD_ARGUMENT)
        
    GlobalDefs.VERBOSE_LOGGING = args.verbose
    
    print("=" * 80)
    print("MQTT-DAP Benchmark")
    print("=" * 80)

    # Perform relevant operations
    if args.command == "run":
        run_tests(args.config, args.logfile, args.broker_address, args.port)
    elif args.command == "analyze":
        analyze_results(args.logfile, args.outfile)
    else:
        # We should never get here as the argument validation should handle 
        # existing on malformed arguments
        sys.exit(GlobalDefs.ExitCode.UNKNOWN_ERROR)

    # If we completed, everything was okay
    sys.exit(GlobalDefs.ExitCode.SUCCESS)   


"""Validates the basic format of the arguments. This function does not validate that files can be opened or that IP addresses can be reached.

Parameters
----------
args : argparse.Namespace
    The parsed arguments

Returns
----------
bool
    True if the arguments are valid, false otherwise
"""
def _validate_arguments(args: argparse.Namespace) -> bool:

    # Check subcommand
    if args.command == "run":
        # Node configuration must exist
        if not path.isfile(args.config):
            console_log(ConsoleLogLevel.ERROR, f"Cannot find configuration file at {args.config}")
            return False
        
        # Since broker address can be a FQDN, we don't validate here
        
        # Port must be valid
        if not 1 <= args.port <= 65535:
            console_log(ConsoleLogLevel.ERROR, f"Port must be in the range [1-66535]")
            return False
        
    elif args.command == "analyze":
                
        # Log directories must exist
        if not path.isfile(args.logfile):
            console_log(ConsoleLogLevel.ERROR, f"Cannot find log file at {args.logfile}")
            return False

        # Outfile will be validated on open
        
    # Invalid subcommand
    else:
        return False
        
    # All passed
    return True
    
def run_tests(config, logfile, broker_address, port):
    # Parse configuration
    console_log(ConsoleLogLevel.INFO, f"Loading configuration from: {config}")
    config_parser = ConfigParser()
    try:
        benchmark_config = config_parser.parse_config(config)
    except Exception as e:
        console_log(ConsoleLogLevel.ERROR, f"Failed to parse configuration: {e}")
        sys.exit(GlobalDefs.ExitCode.MALFORMED_CONFIG)

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
    console_log(ConsoleLogLevel.INFO, f"Loading client module: {module_name}")
    try:
        GlobalDefs.CLIENT_MODULE = _load_client_module(module_name)
    except Exception as e:
        console_log(ConsoleLogLevel.ERROR, f"Error: Failed to load client module: {e}")
        sys.exit(GlobalDefs.ExitCode.BAD_CLIENT_API)

    # Setup logging
    if logfile is None:
        timestring = time.strftime("%Y-%m-%d_%H-%M-%S")
        
        # Use config file name by default
        config_name = path.splitext(path.basename(config))[0]
        logfile = f"{benchmark_config.log_output_dir}/{config_name}_{timestring}.log"

    console_log(ConsoleLogLevel.INFO, f"Logging to: {logfile}")
    GlobalDefs.LOGGING_MODULE = ResultLogger()
    try:
        GlobalDefs.LOGGING_MODULE.start(logfile)
    except Exception as e:
        console_log(ConsoleLogLevel.ERROR, f"Error: Failed to initialize logging: {e}")
        sys.exit(GlobalDefs.ExitCode.FAILED_TO_INIT_LOGGING)

    GlobalDefs.LOGGING_MODULE.log_pm_method(benchmark_config.method.value)

    # Create test executor
    console_log(ConsoleLogLevel.INFO, f"Connecting to broker: {broker_address}:{port}")
    console_log(ConsoleLogLevel.INFO, f"Using purpose management method: {benchmark_config.method.value}")

    # Choose executor based on whether test uses deterministic scheduling
    # If test has scheduled_events, use DeterministicTestExecutor, otherwise use TestExecutor
    use_deterministic = False
    if benchmark_config.test_list:
        first_test = benchmark_config.test_list[0]
        use_deterministic = hasattr(first_test, 'scheduled_events') and len(first_test.scheduled_events) > 0

    if use_deterministic:
        console_log(ConsoleLogLevel.INFO, "Using Deterministic Test Executor")
        executor = DeterministicTestExecutor(
            benchmark_config.this_node_name,
            broker_address,
            port,
            benchmark_config.method,
        )
    else:
        console_log(ConsoleLogLevel.INFO, "Using Randomized Test Executor")
        executor = TestExecutor(
            benchmark_config.this_node_name,
            broker_address,
            port,
            benchmark_config.method,
        )

    # Run each test
    for test_config in benchmark_config.test_list:
        print("\n" + "-" * 80)
        print(f"Running test: {test_config.name}")
        print("-" * 80)
        executor.setup_test(test_config)
        executor.perform_test(test_config)

    # Shutdown
    GlobalDefs.LOGGING_MODULE.shutdown()

    print("\n" + "=" * 80)
    print(f"All tests completed successfully!")
    print(f"Results logged to: {logfile}")
    print("=" * 80)

def _load_client_module(module_name: str):
    """Load and validate the client interface module"""
    module = importlib.import_module(module_name)
    for function in GlobalDefs.CLIENT_FUNCTIONS:
        if not hasattr(module, function):
            raise AttributeError(f"Could not find required function: {function}")
    return module

def analyze_results(logfile, outfile):
    console_log(ConsoleLogLevel.INFO, f"Analyzing results from: {logfile}")
    return 0

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