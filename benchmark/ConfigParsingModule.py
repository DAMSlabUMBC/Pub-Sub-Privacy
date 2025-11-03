import yaml
import os
import sys
from typing import List
from GlobalDefs import ExitCode, PurposeManagementMethod
from TestExecutor import TestConfiguration

class BenchmarkConfiguration:
    
    this_node_name: str
    method: PurposeManagementMethod
    client_module_name: str
    log_output_dir: str
    seed: int | None = None
    
    # === Required topics for some PM methods ===
    reg_by_msg_reg_topic: str = ""
    reg_by_topic_pub_reg_topic: str = ""
    reg_by_topic_sub_reg_topic: str = ""
    
    # === Operational Information ===
    or_topic_name: str
    ors_topic_name: str
    on_topic_name: str
    onp_topic_name: str
    osys_topic__name: str
    op_response_topic: str
    op_purpose: str
    
    test_list: List[TestConfiguration] = list()

class ConfigParser:
    
    the_config: BenchmarkConfiguration = BenchmarkConfiguration()

    def parse_config(self, file_path) -> BenchmarkConfiguration:
        
        # Verify the file exists
        if not os.path.exists(file_path):
            print(f"Configuration file not found at {file_path}")
            sys.exit(ExitCode.BAD_ARGUMENT)

        # Attempt to load the file into memory
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
            if data is None:
                print(f"Configuration file could not be parsed")
                sys.exit(ExitCode.MALFORMED_CONFIG)
                
        self._parse_config_yaml(data)
        return self.the_config

    def _parse_config_yaml(self, data):
        
        # Parse the overall config
        if not "node_name" in data:
            raise Exception("node_name not found in config")
        self.the_config.this_node_name = data["node_name"]
        
        if not "client_module_name" in data:
            raise Exception("client_module_name not found in config")
        self.the_config.client_module_name = data["client_module_name"]
        
        if not "output_dir" in data:
            raise Exception("output_dir not found in config")
        self.the_config.log_output_dir = data["output_dir"]
        
        # Optional
        if "seed" in data:
            self.the_config.seed = int(data["seed"])
        
        if not "purpose_management_method" in data:
            raise Exception("purpose_management_method not found in config")
        
        method_int = data["purpose_management_method"]
        if method_int == 0:
            self.the_config.method = PurposeManagementMethod.PM_0
        elif method_int == 1:
            self.the_config.method = PurposeManagementMethod.PM_1
        elif method_int == 2:
            self.the_config.method = PurposeManagementMethod.PM_2
        elif method_int == 3:
            self.the_config.method = PurposeManagementMethod.PM_3
            
            if not "reg_by_msg_reg_topic" in data:
                raise Exception("reg_by_msg_reg_topic not found in config with purpose management method 3")
            self.the_config.reg_by_msg_reg_topic = data["reg_by_msg_reg_topic"]
            
        elif method_int == 4:
            self.the_config.method = PurposeManagementMethod.PM_4
            
            if not "reg_by_topic_pub_reg_topic" in data:
                raise Exception("reg_by_topic_pub_reg_topic not found in config with purpose management method 4")
            self.the_config.reg_by_topic_pub_reg_topic = data["reg_by_topic_pub_reg_topic"]
            
            if not "reg_by_topic_sub_reg_topic" in data:
                raise Exception("reg_by_topic_sub_reg_topic not found in config with purpose management method 4")
            self.the_config.reg_by_topic_sub_reg_topic = data["reg_by_topic_sub_reg_topic"]
            
        else:
            raise Exception("unknown purpose_management_method found in config")

         # Parse the opertional config
        if not "or_topic_name" in data:
            raise Exception("or_topic_name not found in config")
        self.the_config.or_topic_name = data["or_topic_name"]
        
        if not "ors_topic_name" in data:
            raise Exception("ors_topic_name not found in config")
        self.the_config.ors_topic_name = data["ors_topic_name"]
        
        if not "on_topic_name" in data:
            raise Exception("on_topic_name not found in config")
        self.the_config.on_topic_name = data["on_topic_name"]
        
        if not "onp_topic_name" in data:
            raise Exception("onp_topic_name not found in config")
        self.the_config.onp_topic_name = data["onp_topic_name"]
        
        if not "osys_topic_name" in data:
            raise Exception("osys_topic_name not found in config")
        self.the_config.osys_topic__name = data["osys_topic_name"]
        
        if not "operational_response_topic_prefix" in data:
            raise Exception("operational_response_topic_prefix not found in config")
        self.the_config.op_response_topic = data["operational_response_topic_prefix"]
        
        if not "operational_purpose" in data:
            raise Exception("operational_purpose not found in config")
        self.the_config.op_purpose = data["operational_purpose"]
            
        # Parse the test config
        if not "test" in data:
            raise Exception("test not found in config")
        
        # Parse test (currently only supports one)
        test = data["test"]
        
        # Overall information
            
        if not "name" in test:
            raise Exception("name not found for test config")
        name = test["name"]

        if not "duration_ms" in test:
            raise Exception(f"duration_ms not found for test {test_config.name} config")
        test_duration_ms = test["duration_ms"]
        
        if not "client_count" in test:
            raise Exception(f"client_count not found for test {test_config.name} config")
        client_count = test["client_count"]
        
        test_config = TestConfiguration(name, test_duration_ms, client_count)
        
        if not "data_qos" in test:
            raise Exception(f"data_qos not found for test {test_config.name} config")
        test_config.qos = test["data_qos"]
        
        # Client information
        if "perform_connection_test" in test:
            test_config.perform_connection_test = test["perform_connection_test"]

            if test_config.perform_connection_test:
                if not "disconnection_pct" in test:
                    raise Exception(f"disconnection_pct not found with perform_connection_test enabled for test {test_config.name} config")
                test_config.disconnection_pct = test["disconnection_pct"]
        
        if not "topics_subbed_by_client_pct" in test:
            raise Exception(f"topics_subbed_by_client_pct not found for test {test_config.name} config")
        test_config.pct_topics_per_client = test["topics_subbed_by_client_pct"]
        
        if not "pub_period_ms" in test:
            raise Exception(f"pub_period_ms not found for test {test_config.name} config")
        test_config.pub_period_ms = test["pub_period_ms"]
        
        if not "clients_publishing_per_timestep_pct" in test:
            raise Exception(f"clients_publishing_per_timestep_pct not found for test {test_config.name} config")
        test_config.pct_to_publish_on = test["clients_publishing_per_timestep_pct"]
        
        if not "topics_per_pub_pct" in test:
            raise Exception(f"topics_per_pub_pct not found for test {test_config.name} config")
        test_config.pct_topics_per_pub = test["topics_per_pub_pct"]
        
        if not "purpose_shuffle_period_ms" in test:
            raise Exception(f"purpose_shuffle_period_ms not found for test {test_config.name} config")
        test_config.purpose_shuffle_period_ms = test["purpose_shuffle_period_ms"]
        
        if not "purpose_shuffle_pct" in test:
            raise Exception(f"purpose_shuffle_pct not found for test {test_config.name} config")
        test_config.purpose_shuffle_pct = test["purpose_shuffle_pct"]
        
        if not "min_payload_length_bytes" in test:
            raise Exception(f"min_payload_length_bytes not found for test {test_config.name} config")
        test_config.min_payload_length_bytes = test["min_payload_length_bytes"]
        
        if not "max_payload_length_bytes" in test:
            raise Exception(f"max_payload_length_bytes not found for test {test_config.name} config")
        test_config.max_payload_length_bytes = test["max_payload_length_bytes"]
        
        # Operational information
        # Ops not required, may be empty
        if "op_send_chance" in test:
            test_config.op_send_chance = test["op_send_chance"]
            
        if "c1_reg_ops" in test:
            for op in test["c1_reg_ops"]:
                test_config.c1_reg_operations.append(op)
            
        if "c1_ops" in test:
            for op in test["c1_ops"]:
                test_config.possible_operations[op] = "C1"
                
        if "c2_ops" in test:
            for op in test["c2_ops"]:
                test_config.possible_operations[op] = "C2"
                
        if "c3_ops" in test:
            for op in test["c3_ops"]:
                test_config.possible_operations[op] = "C3"
        
        if not "topics" in test:
            raise Exception(f"topics not found for test {test_config.name} config")
        
        for topic in test["topics"]:
            test_config.publish_topic_list.append(topic)
        
        if not "topic_filters" in test:
            raise Exception(f"topic_filters not found for test {test_config.name} config")
        
        for filter in test["topic_filters"]:
            test_config.subscribe_topic_list.append(filter)
                
        if not "purposes" in test:
            raise Exception(f"purposes not found for test {test_config.name} config")
        
        for purpose in test["purposes"]:
            test_config.publish_purpose_list.append(purpose)
        
        if not "purpose_filters" in test:
            raise Exception(f"purpose_filters not found for test {test_config.name} config")
        
        for filter in test["purpose_filters"]:
            test_config.subscribe_purpose_list.append(filter)
    
        self.the_config.test_list.append(test_config)