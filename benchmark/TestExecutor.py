import paho.mqtt.client as mqtt
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.properties import Properties
import random
import time
import threading
import ischedule
import sched
import GlobalDefs
from typing import List, Dict, Tuple, Any
import datetime
from dataclasses import dataclass

class TestConfiguration:
    
    # === Overall information
    name: str
    test_duration_ms: int
    
    # === Client information ===
    client_count: int
    qos: int = 0
    pct_connected_clients_on_init: float = 1.0
    
    # Used to randomly disconnect/reconnect clients
    pct_to_disconnect: float = 0.0
    disconnect_period_ms: int = 0
    pct_to_reconnect: float = 0.0
    reconnect_period_ms: int = 0
    
    # === Topic information ===   
    pct_topics_per_client: float = 1.0
    publish_topic_list: List[str] = list()
    subscribe_topic_list: List[str] = list()
    
    # Used to randomly generate topics
    generate_topics: bool = True
    topic_count: int = 0
    topic_filter_count: int = 0
    
    # === Purpose information ===
    purpose_shuffle_period_ms: int = 0
    purpose_shuffle_chance: float = 0.0
    publish_purpose_list: List[str] = list()
    subscribe_purpose_list: List[str] = list()
    
    # Used to randomly generate purposes
    generate_purposes: bool = True
    purpose_count: int = 0
    purpose_filter_count: int = 0
    
    # === Publication information ===   
    pct_to_publish_on: float = 1.0
    pct_topics_per_pub: float = 1.0
    pub_period_ms: int = 500
    min_payload_length_bytes: int = 1
    max_payload_length_bytes: int = 1024
    
    # === Operation information ===
    op_send_chance: float = 0.0
    c1_reg_operations: List[str] = list()
    possible_operations: List[str] = list()
    
    def __init__(self, name:str, test_duration_ms: int, client_count: int):
        self.name = name
        self.test_duration_ms = test_duration_ms
        self.client_count = client_count
    

class TestExecutor:
    
    class TestClient:
        client: mqtt.Client
        name: str
        subscribed_topics: Dict[str, str] = dict() # Maps topic filter to purpose filter
        publish_topics: Dict[str, str] = dict() # Maps topic to purpose
        is_connected: bool = False
        has_set_c1_ops: bool = False
        msg_send_counter: int = 0
        message_id_to_send_counter: Dict[int, int] = dict()
        
        def __init__(self, client: mqtt.Client, name: str):
            self.client = client
            self.name = name
            
        def get_send_counter(self) -> int:
            ret_val = self.msg_send_counter
            self.msg_send_counter = self.msg_send_counter + 1
            return ret_val
    
    my_id: str
    broker_address: str
    broker_port: int
    method: GlobalDefs.PurposeManagementMethod
    current_config: TestConfiguration
    stop_event: threading.Event = threading.Event()
    duration_scheduler: sched.scheduler
    
    all_clients: List[TestClient] = list()
    pending_publishes: Dict[str, Dict[int, Tuple[str, str, str]]] = dict() # client name => [message id => (topic, purpose, message_type)]
    
    def __init__(self, executor_id: str, broker_address: str, broker_port: int, method: GlobalDefs.PurposeManagementMethod, seed: int | None = None):
        
        self.my_id = executor_id
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.method = method
        
        # If not supplied, set random seed based on time
        if seed is None:
            seed = int(time.time())
            
        #  Seed the random number generator and write seed to log file
        random.seed(seed)
        GlobalDefs.LOGGING_MODULE.log_seed(seed)
        
        # Configure scheduler
        self.duration_scheduler = sched.scheduler()
        
    
    def setup_test(self, test_config: TestConfiguration):
        
        #Create a clean environment for the test
        self._clear_previous_test_data()
        
        # Need to save this for the test
        self.current_config = test_config
        
        print(f"Configuring test {test_config.name}")
        
        # Create clients
        print(f"\tSetting up test...")
        self._create_clients()
        
        # Determine which topic/purposes each client will publish to
        self._assign_publication_topics_and_purposes()
        
        # Connect clients
        print(f"\tConnecting and configuring initial clients...")
        self._connect_initial_clients()
        
        # Give a moment to allow messages to travel
        time.sleep(1)
        
        print(f"Test configured!")
        
    
    def perform_test(self, test_config: TestConfiguration):
        
        print(f"Running test {test_config.name}")
        
        # == Setup scheduler tasks, timing is in seconds and needs to be divided by 1000 ==
        # Publish task
        ischedule.schedule(self._publish_from_clients, interval=(test_config.pub_period_ms / 1000.0))
        
        # Connect task
        if test_config.reconnect_period_ms > 0:
            ischedule.schedule(self._reconnect_clients, interval=(test_config.reconnect_period_ms / 1000.0))
        
        # Disconnect task
        if test_config.disconnect_period_ms > 0:
            ischedule.schedule(self._disconnect_clients, interval=(test_config.disconnect_period_ms / 1000.0))
            
        # Shuffle purpose task
        if test_config.purpose_shuffle_period_ms > 0:
            ischedule.schedule(self._shuffle_publication_purposes, interval=(test_config.purpose_shuffle_period_ms / 1000.0))
        
        # == Main test loop ==
        # Calculate end time
        end_time = time.monotonic() + (test_config.test_duration_ms / 1000.0) # Convert to seconds
                
        # Start scheduler processing
        self.stop_event.clear()
        
        # Set event to be triggered at the test duration end
        self.duration_scheduler.enterabs(end_time, 1, self._end_test)
        stop_thread = threading.Thread(target=self.duration_scheduler.run)
        print(f"\tTest will run for {test_config.test_duration_ms / 1000.0} second(s)")
        stop_thread.start()
        
        # Start the threads
        # NOTE this must be done AFTER the scheduler above, or it won't terminate
        print(f"\tTest started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}!")
        ischedule.run_loop(stop_event=self.stop_event)
        
        print(f"\tTest complete! Cleaning up...")
        # Give few seconds to finish publications and recieves
        
        time.sleep(2)
        
        # Disconnect all clients and clean data
        self._disconnect_all_clients()
        self._clear_previous_test_data()

        print(f"Cleanup complete!")
        
        
    def _clear_previous_test_data(self):
        self.current_config = None
        self.all_clients = list()
        self.pending_publishes = dict()
        ischedule.reset()

            
    def _end_test(self):
        # Ending just means triggering the stop event
        self.stop_event.set()
        
        
    """1. Create specified number of clients with random topic subscriptions""" 
    def _create_clients(self) -> None:
        
        # Create the specified number of clients
        for i in range(self.current_config.client_count):    
            
            name = f"{self.my_id}__{self.current_config.name}__C{i}"
            client = GlobalDefs.CLIENT_MODULE.create_v5_client(name)
            
            # Assign callback
            client.on_publish = self._on_publish
            
            # Add to all clients
            test_client = self.TestClient(client, name)
            client.user_data_set(test_client) # Set reference to client data
            self.all_clients.append(test_client)
            
            
    def _connect_initial_clients(self) -> None:
        
        # Connect the specified number of clients
        init_connected_client_count = (int)(self.current_config.client_count * self.current_config.pct_connected_clients_on_init)
        clients_to_connect = random.sample(self.all_clients, init_connected_client_count)
        
        for test_client in clients_to_connect:
            result_code = GlobalDefs.CLIENT_MODULE.connect_client(test_client.client, self.broker_address, self.broker_port, 
                                                                  success_callback=self._on_connect)
    
            if result_code == mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
                test_client.client.loop_start()
            else:
                raise RuntimeError(f"Failed to connect client {test_client.name}")
   
    def _subscribe_client_for_operations(self, test_client: TestClient):
        
        client_id = test_client.name
        operation_response_topic = f"{GlobalDefs.OP_RESPONSE_TOPIC}/{client_id}"
        # Subscribe to all operational topics and the default response topic
        topics = [GlobalDefs.ON_TOPIC, GlobalDefs.ONP_TOPIC + "/" + client_id, GlobalDefs.OR_TOPIC, GlobalDefs.ORS_TOPIC + "/" + client_id, operation_response_topic]
        purpose_for_subscription = GlobalDefs.OP_PURPOSE
        
        # Subscribe to each of the non-broker operational topics
        for topic in topics:
            result_code, mid = GlobalDefs.CLIENT_MODULE.subscribe_for_operations(test_client.client, self.method, self._on_message_recv, topic)
            if result_code == mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
                # Log and store
                GlobalDefs.LOGGING_MODULE.log_subscribe(time.time(), self.my_id, test_client.name, topic, purpose_for_subscription)
            else:
                raise RuntimeError(f"Failed to subscribe on {topic} with purpose {purpose_for_subscription}")
        
                
    def _subscribe_client_for_data(self, test_client: TestClient):

        # Check if topics already exist, if so resubscribe to these
        if len(test_client.subscribed_topics) > 0:
            
            for topic in test_client.subscribed_topics:
                purpose_for_subscription = test_client.subscribed_topics[topic]
                
                result_code, mid = GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(test_client.client, self.method, self._on_message_recv, 
                                                                                          topic, purpose_for_subscription, self.current_config.qos)
                if result_code == mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
                    # Log and store
                    GlobalDefs.LOGGING_MODULE.log_subscribe(time.time(), self.my_id, test_client.name, topic, purpose_for_subscription)
                else:
                    raise RuntimeError(f"Failed to subscribe on {topic} with purpose {purpose_for_subscription}")
        
        # If not, determine new topics and purpose to subscribe on
        else:
            # Determine the specified number of topics to connect to 
            topics_for_subscription_count = (int)(len(self.current_config.subscribe_topic_list) * self.current_config.pct_topics_per_client)
            topics_for_subscription = random.sample(self.current_config.subscribe_topic_list, topics_for_subscription_count)
            
            for topic in topics_for_subscription:
            
                # Select a purpose filter for this topic
                purpose_for_subscription = random.choice(self.current_config.subscribe_purpose_list)
                
                result_code, mid = GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(test_client.client, self.method, self._on_message_recv, 
                                                                                          topic, purpose_for_subscription)
                
                if result_code == mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
                    # Log and store
                    GlobalDefs.LOGGING_MODULE.log_subscribe(time.time(), self.my_id, test_client.name, topic, purpose_for_subscription)
                    test_client.subscribed_topics[topic] = purpose_for_subscription
                else:
                    raise RuntimeError(f"Failed to subscribe on {topic} with purpose {purpose_for_subscription}")
                
                
    def _assign_publication_topics_and_purposes(self) -> None:
        
        # Do this for all clients
        for test_client in self.all_clients:
            
            # Find the specified number of topics
            topics_for_publication_count = (int)(len(self.current_config.publish_topic_list) * self.current_config.pct_topics_per_client)
            topics_for_publication = random.sample(self.current_config.publish_topic_list, topics_for_publication_count)
            
            for topic in topics_for_publication:
                # Assign a purpose
                purpose_for_publication = random.choice(self.current_config.publish_purpose_list)
                test_client.publish_topics[topic] = purpose_for_publication
                   
                
    def _register_initial_publication_purposes_for_client(self, test_client: TestClient) -> None:

        # All clients have already had this done
        for topic in test_client.publish_topics:
            # Prep publications (if the method needs it)
            GlobalDefs.CLIENT_MODULE.register_publish_purpose_for_topic(test_client.client, self.method, topic, test_client.publish_topics[topic])
            
            
    def _register_data_for_c1_operations_for_client(self, test_client: TestClient) -> None:
        
        # Nothing else to do
        if test_client.has_set_c1_ops:
            return
        
        # Not used if not broker-modifying
        if self.method is not GlobalDefs.PurposeManagementMethod.PM_0 and self.method is not GlobalDefs.PurposeManagementMethod.PM_1:
            for c1_op in self.current_config.c1_reg_operations:
                GlobalDefs.CLIENT_MODULE.publish_operation_request(test_client.client, self.method, c1_op, test_client.get_send_counter())
                test_client.has_set_c1_ops = True
                
            
    def _disconnect_clients(self) -> None:
        
        # Find connected clients
        connected_clients = [client for client in self.all_clients if client.is_connected]
                
        # Disconnect the specified number of clients
        clients_to_disconnect_count = (int)(len(connected_clients) * self.current_config.pct_to_disconnect)
        clients_to_disconnect = random.sample(connected_clients, clients_to_disconnect_count)
                
        for test_client in clients_to_disconnect:
            result_code = GlobalDefs.CLIENT_MODULE.disconnect_client(test_client.client, callback=self._on_disconnect)
    
            if result_code != mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"Failed to disconnect client {test_client.name}")
            
            
    def _disconnect_all_clients(self) -> None:
        # Find connected clients
        connected_clients = [client for client in self.all_clients if client.is_connected]
            
        for test_client in connected_clients:
            GlobalDefs.CLIENT_MODULE.disconnect_client(test_client.client, callback=self._on_disconnect)
    
    
    def _reconnect_clients(self) -> None:
        # Find disconnected clients
        disconnected_clients = [client for client in self.all_clients if not client.is_connected]
                
        # Connect the specified number of clients
        clients_to_reconnect_count = (int)(len(disconnected_clients) * self.current_config.pct_to_reconnect)
        clients_to_reconnect = random.sample(disconnected_clients, clients_to_reconnect_count)
                
        for test_client in clients_to_reconnect:
            result_code = GlobalDefs.CLIENT_MODULE.connect_client(test_client.client, self.broker_address, self.broker_port, 
                                                                  success_callback=self._on_connect, clean_start=False)
    
            if result_code == mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
                test_client.client.loop_start()
            else:
                raise RuntimeError(f"Failed to connect client {test_client.name}")
            
    def _shuffle_publication_purposes(self) -> None:
        
        # Shuffle the purposes for all publications
        for test_client in self.all_clients:
            
            # Check if we should shuffle purposes
            if(random.random() < self.current_config.purpose_shuffle_chance):      
                    
                # Select a new purpose for each topic
                for topic in test_client.publish_topics:             
                    purpose_for_publication = random.choice(self.current_config.publish_purpose_list)
                    test_client.publish_topics[topic] = purpose_for_publication
            
    
    def _publish_from_clients(self):
        # Find connected clients
        connected_clients = [client for client in self.all_clients if client.is_connected]
        
        # Publish on specified number of clients
        clients_to_publish_count = (int)(len(connected_clients) * self.current_config.pct_to_publish_on)
        clients_to_publish = random.sample(connected_clients, clients_to_publish_count)
        
        for test_client in clients_to_publish:
            
            # Check if we should also do an operational publish
            if(random.random() < self.current_config.op_send_chance):
                self._publish_operation(test_client)
            
            self._publish_data(test_client)
             
                    
    def _publish_operation(self, test_client: TestClient):
        
        message_counter = test_client.get_send_counter()
        
        # Select an operation for this topic
        operation = random.choice(self.current_config.possible_operations)
        results = GlobalDefs.CLIENT_MODULE.publish_operation_request(test_client.client, self.method, operation, message_counter)
        
        for message_info, topic in results:
            if not test_client.name in self.pending_publishes:
                self.pending_publishes[test_client.name] = dict()
            self.pending_publishes[test_client.name][message_info.mid] = (topic, GlobalDefs.OP_PURPOSE, operation)
            
            # Save message counter for correlations
            test_client.message_id_to_send_counter[message_info.mid] = message_counter
            
    
    def _publish_data(self, test_client: TestClient):
        # Publish on specified number of topics
        topics_to_publish_on_count = (int)(len(test_client.publish_topics) * self.current_config.pct_topics_per_pub)
        topics_to_publish_on = random.sample(sorted(test_client.publish_topics.keys()), topics_to_publish_on_count)
        
        for topic in topics_to_publish_on:
            
            # Generate a payload to simulate variable data (if requested)
            payload = None
            num_bytes = random.randrange(self.current_config.min_payload_length_bytes, self.current_config.max_payload_length_bytes)         
            if num_bytes > 0:
                payload = random.randbytes(num_bytes)
                
            message_counter = test_client.get_send_counter()
            
            results = GlobalDefs.CLIENT_MODULE.publish_with_purpose(test_client.client, self.method, topic, 
                                                                    test_client.publish_topics[topic], qos=self.current_config.qos, payload=payload, correlation_data=message_counter)
            
            for message_info, topic in results:
                if not test_client.name in self.pending_publishes:
                    self.pending_publishes[test_client.name] = dict()
                    
                # For purpose management method 1, we need to extract the topic properly
                if self.method == GlobalDefs.PurposeManagementMethod.PM_1:
                    purpose_start_index = topic.rfind('[')
                    topic = topic[:purpose_start_index - 1]
                    
                self.pending_publishes[test_client.name][message_info.mid] = (topic, test_client.publish_topics[topic], "DATA")
                
                # Save message counter for correlations
                test_client.message_id_to_send_counter[message_info.mid] = message_counter
        
        
    ###################################     
    #   CALLBACK FUNCTIONS
    ###################################
    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: mqtt.ConnectFlags, reason_code: ReasonCode, properties: Properties):   
        if reason_code == 0: # Success
            # Userdata is a TestClient object
            userdata.is_connected = True
            
            # Log
            GlobalDefs.LOGGING_MODULE.log_connect(time.time(), self.my_id, userdata.name)
            
            # Resubscribe clients
            self._subscribe_client_for_operations(userdata)
            self._subscribe_client_for_data(userdata)
        
            # Reestablish publication purpose and c1 operational data
            self._register_initial_publication_purposes_for_client(userdata)
            self._register_data_for_c1_operations_for_client(userdata)
            
            
    def _on_disconnect(self, client: mqtt.Client, userdata: Any, flags: mqtt.DisconnectFlags, reason_code: ReasonCode, properties: Properties):
        if reason_code == 0: # Success
            # Userdata is a TestClient object
            userdata.is_connected = False
            
            # Log
            GlobalDefs.LOGGING_MODULE.log_disconnect(time.time(), self.my_id, userdata.name)
    
    def _on_publish(self, client: mqtt.Client, userdata: TestClient, mid:int, reason_code: ReasonCode, properties: Properties):    
        
        # Check if message exists and was successful
        if userdata.name in self.pending_publishes:
            if mid in self.pending_publishes[userdata.name]:    
                
                corr_data = userdata.message_id_to_send_counter[mid]
                    
                # If successful
                if reason_code == 0:
                    topic, purpose, message_type = self.pending_publishes[userdata.name][mid]
                    
                    # Do not log registrations
                    if not topic == GlobalDefs.OSYS_TOPIC:
                    
                        # Check if operational or data
                        if message_type == "DATA":
                            # Log message
                            GlobalDefs.LOGGING_MODULE.log_publish(time.time(), self.my_id, userdata.name, corr_data, topic, purpose, message_type)
                        else:
                            # Log message
                            GlobalDefs.LOGGING_MODULE.log_operation_publish(time.time(), self.my_id, userdata.name, corr_data, topic, purpose, message_type)
                
                
    def _on_message_recv(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage):
        
        # Check if this is an operational message
        if hasattr(message.properties, "UserProperty"):
            for name, value in message.properties.UserProperty:
                if name == GlobalDefs.PROPERTY_OPERATION:
                    self._on_operation_message_recv(userdata, message, value)
                    return
        
        # Default to data message
        self._data_message_recv(userdata, message)    
        
    
    def _data_message_recv(self, userdata: Any, message: mqtt.MQTTMessage):      
        
        # Check for correlation data
        corr_data = -1
        if hasattr(message.properties, "CorrelationData"):
            corr_data = int.from_bytes(message.properties.CorrelationData, byteorder='big', signed=True)
            
        # Log message
        GlobalDefs.LOGGING_MODULE.log_recv(time.time(), self.my_id, userdata.name, corr_data, message.topic, "DATA")
             
    def _on_operation_message_recv(self, userdata: Any, message: mqtt.MQTTMessage, operation: str):    
        
        # Check for correlation data
        corr_data = -1
        if hasattr(message.properties, "CorrelationData"):
            corr_data = int.from_bytes(message.properties.CorrelationData, byteorder='big', signed=True)
            
        # Log message
        GlobalDefs.LOGGING_MODULE.log_operation_recv(time.time(), self.my_id, userdata.name, corr_data, message.topic, operation)