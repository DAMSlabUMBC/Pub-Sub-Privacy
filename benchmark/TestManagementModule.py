# TestManagementModule.py
import sys
import os
import time
import importlib
from types import ModuleType
import GlobalDefs
from SyncModule import BenchmarkSynchronizer
from ConfigParsingModule import ConfigParser
from LoggingModule import ResultLogger
from TestExecutor import TestExecutor, TestConfiguration

def configure_and_run_tests(config: str, broker_address: str, broker_port: int, log_file: str | None):

    # Read Config
    config_parser = ConfigParser()
    try:
        benchmark_config = config_parser.parse_config(config)
    except Exception as e:
        print(f"Unable to parse config - {e}")
        sys.exit(GlobalDefs.ExitCode.MALFORMED_CONFIG)
        
    # Set globals based on config
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

    # Load Client Interface
    module_name = benchmark_config.client_module_name
    try:
        GlobalDefs.CLIENT_MODULE = _load_client_module(module_name)
    except Exception as e:
        print(f"Unable to load client module {module_name} - {e}")
        sys.exit(GlobalDefs.ExitCode.BAD_CLIENT_API)

    # Create and Start the Synchronization Module
    GlobalDefs.SYNC_MODULE = BenchmarkSynchronizer(benchmark_config.this_node_name, benchmark_config.all_benchmark_names)
    try:
        GlobalDefs.SYNC_MODULE.start(broker_address, broker_port, benchmark_config.method)
    except Exception as e:
        print(f"Unable to initialize the synchronization module - {e}")
        sys.exit(GlobalDefs.ExitCode.FAILED_TO_INIT_SYNC)
    
    # Configure Logging Module
    GlobalDefs.LOGGING_MODULE = ResultLogger()
    if log_file is None:
        timestring = time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file = f"{benchmark_config.log_output_dir}/{benchmark_config.this_node_name}_{timestring}.log"
    try:
        GlobalDefs.LOGGING_MODULE.start(log_file)
    except Exception as e:
        print(f"Unable to initialize the logging module - {e}")
        sys.exit(GlobalDefs.ExitCode.FAILED_TO_INIT_LOGGING)
    
    # Set up test framework and run each test
    # NOTE: Only one test can be run per execution at the moment. Future plans include allowing multiple tests in one configuration file
    executor = TestExecutor(benchmark_config.this_node_name, broker_address, broker_port, benchmark_config.method)
    for test in benchmark_config.test_list:
       
        executor.setup_test(test)
        
        # Wait for other clients to be ready to execute the test
        print(f"{benchmark_config.this_node_name} waiting for all clients to be ready.")
        ready = GlobalDefs.SYNC_MODULE.notify_and_wait_for_ready(benchmark_config.method)
        
        if not ready:
            print(f"{benchmark_config.this_node_name} syncronization client disconnected improperly. Aborting.")
            sys.exit(GlobalDefs.ExitCode.UNEXP_SYNC_CLIENT_DISCONNECT)
        
        executor.perform_test(test)
        
    # Notify Done and Wait
    print(f"{benchmark_config.this_node_name} waiting for all clients to finish.")
    done = GlobalDefs.SYNC_MODULE.notify_and_wait_for_done(benchmark_config.method)

    if not done:
        print(f"{benchmark_config.this_node_name} syncronization client disconnected improperly. Aborting.")
        sys.exit(GlobalDefs.ExitCode.UNEXP_SYNC_CLIENT_DISCONNECT)
    
    GlobalDefs.LOGGING_MODULE.shutdown()
    
    # All Done - Log completion and exit
    print(f"{benchmark_config.this_node_name} completed successfully")
    return

def _load_client_module(module_name: str) -> ModuleType:
    module = importlib.import_module(module_name)
    for function in GlobalDefs.CLIENT_FUNCTIONS:
        if not hasattr(module, function):
            raise AttributeError(f"Could not find required function {function}")
    return module
