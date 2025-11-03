# TestManagementModule.py
import sys
import os
import time
import importlib
from types import ModuleType
import GlobalDefs
from ConfigParsingModule import ConfigParser
from LoggingModule import ResultLogger
from TestExecutor import TestExecutor

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
        
    GlobalDefs.LOGGING_MODULE.log_pm_method(benchmark_config.method.value)
    
    # Set up test framework and run each test
    executor = TestExecutor(benchmark_config.this_node_name, broker_address, broker_port, benchmark_config.method, benchmark_config.seed)
    for test in benchmark_config.test_list:
        executor.setup_test(test)
        executor.perform_test(test)

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
