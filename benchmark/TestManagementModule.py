# TestManagementModule.py
import sys
import os
import time
import importlib
from types import ModuleType
import GlobalDefs
from SyncModule import BenchmarkSynchronizer
from LoggingModule import ResultLogger
from TestExecutor import TestExecutor, TestConfiguration

def configure_and_run_tests(config: str, broker_address: str, broker_port: int, log_file: str | None):
    # Extract my_id from config file name (e.g., "benchmark-1.cfg" -> "benchmark-1")
    my_id = os.path.basename(config).replace(".cfg", "")
    #existing_benchmarks = ["benchmark-1", "benchmark-2"]  # TODO: Read from config or CLI in the future
    method = GlobalDefs.PurposeManagementMethod.PM_1
    existing_benchmarks = ["benchmark-PM_1-1","benchmark-PM_1-2"]

    # TODO: Read Config (for future dynamic benchmark list)

    # Load Client Interface
    module_name = "ClientInterface"
    try:
        GlobalDefs.CLIENT_MODULE = _load_client_module(module_name)
    except Exception as e:
        print(f"Unable to load client module {module_name} - {e}")
        sys.exit(GlobalDefs.ExitCode.BAD_CLIENT_API)

    # Create and Start the Synchronization Module
    GlobalDefs.SYNC_MODULE = BenchmarkSynchronizer(my_id, existing_benchmarks)
    try:
        GlobalDefs.SYNC_MODULE.start(broker_address, broker_port, method)
    except Exception as e:
        print(f"Unable to initialize the synchronization module - {e}")
        sys.exit(GlobalDefs.ExitCode.FAILED_TO_INIT_SYNC)
    
    # Configure Logging Module
    GlobalDefs.LOGGING_MODULE = ResultLogger()
    if log_file is None:
        timestring = time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file = f"logs/{my_id}_{timestring}.log"
    try:
        GlobalDefs.LOGGING_MODULE.start(log_file)
    except Exception as e:
        print(f"Unable to initialize the logging module - {e}")
        sys.exit(GlobalDefs.ExitCode.FAILED_TO_INIT_LOGGING)
    
    # Notify Ready and Wait
    print(f"{my_id} waiting...")
    GlobalDefs.SYNC_MODULE.notify_and_wait_for_ready(method)
    
    # All Ready - Simulate some test work
    print(f"{my_id} starting benchmark work...")
    
    test_config = TestConfiguration("Test 1", 500, 10)
    #test_config.pct_to_disconnect = 0.4
    #test_config.pct_to_reconnect = 0.5
    #test_config.pct_to_publish_on = 0.5
    #test_config.pct_topics_per_client = 0.5
    #test_config.pct_topics_per_pub = 0.5
    test_config.publish_topic_list = ["topic1", "topic2"]
    test_config.subscribe_topic_list = ["topic1", "topic2"]
    test_config.purpose_list = ["purpose1", "purpose2"]
    executor = TestExecutor(my_id, broker_address, broker_port, method)
    executor.perform_test(test_config)
    
    # Notify Done and Wait
    GlobalDefs.SYNC_MODULE.notify_and_wait_for_done(method)
    
    GlobalDefs.LOGGING_MODULE.shutdown()
    
    # All Done - Log completion and exit
    print(f"{my_id} completed successfully")
    return

def _load_client_module(module_name: str) -> ModuleType:
    module = importlib.import_module(module_name)
    for function in GlobalDefs.CLIENT_FUNCTIONS:
        if not hasattr(module, function):
            raise AttributeError(f"Could not find required function {function}")
    return module
