import paho.mqtt.client as mqtt
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.properties import Properties
import random
import signal
import time
import os
from math import ceil
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
    
    # Used to randomly disconnect/reconnect clients
    perform_connection_test: bool = False
    
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
    possible_operations: Dict[str, str] = dict()
    
    def __init__(self, name:str, test_duration_ms: int, client_count: int):
        self.name = name
        self.test_duration_ms = test_duration_ms
        self.client_count = client_count
    

class TestExecutor:
    
    class TestClient:
        client: mqtt.Client
        name: str
        subscribed_topics: Dict[str, str] # Maps topic filter to purpose filter
        publish_topics: Dict[str, str] # Maps topic to purpose
        is_connected: bool
        has_set_c1_ops: bool
        msg_send_counter: int
        message_id_to_send_counter: Dict[int, int]
        
        def __init__(self, client: mqtt.Client, name: str):
            self.client = client
            self.name = name
            self.subscribed_topics = dict()
            self.publish_topics = dict()
            self.is_connected = False
            self.has_set_c1_ops = False
            self.msg_send_counter = 0
            self.message_id_to_send_counter = dict()
            
        def get_send_counter(self) -> int:
            ret_val = self.msg_send_counter
            self.msg_send_counter = self.msg_send_counter + 1
            return ret_val
    
    my_id: str
    broker_address: str
    broker_port: int
    method: GlobalDefs.PurposeManagementMethod
    current_config: TestConfiguration
    stop_event: threading.Event
    duration_scheduler: sched.scheduler
    
    all_clients: List[TestClient]
    pending_publishes: Dict[str, Dict[int, Tuple[str, str, str, float]]] # client name => [message id => (topic, purpose, message_type, timestamp)]
    pending_subscribes: Dict[str, Dict[int, Tuple[str, str, int, float]]] # client name => [message id => (topic_filter, purpose_filter, sub_id, timestamp)]
    publish_lock: threading.Lock
    subscribe_lock: threading.Lock
    
    def __init__(self, executor_id: str, broker_address: str, broker_port: int, method: GlobalDefs.PurposeManagementMethod, seed: int | None = None):
        
        self.my_id = executor_id
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.method = method
        self.pending_publishes = dict()
        self.pending_subscribes = dict()
        self.stop_event = threading.Event()
        self.all_clients = list()
        self.publish_lock = threading.Lock()
        self.subscribe_lock = threading.Lock()
        
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
        self._connect_all_clients()
        
        # Give a moment to allow messages to travel
        time.sleep(1)
        
        print(f"Test configured!")
        
    
    def perform_test(self, test_config: TestConfiguration):
        
        print(f"Running test {test_config.name}")
        
        # == Setup scheduler tasks, timing is in seconds and needs to be divided by 1000 ==
        # Publish task
        ischedule.schedule(self._publish_from_clients, interval=(test_config.pub_period_ms / 1000.0))

        # Shuffle purpose task
        if test_config.purpose_shuffle_period_ms > 0:
            ischedule.schedule(self._shuffle_publication_purposes, interval=(test_config.purpose_shuffle_period_ms / 1000.0))
        
        # == Main test loop ==
        # Calculate end time
        start_time = time.monotonic()
        test_dur_secs = test_config.test_duration_ms / 1000.0
        end_time = start_time + test_dur_secs

        # Disconnect testing tasks if enabled
        if test_config.perform_connection_test:
            disconnect_time = start_time + (test_dur_secs * (1/3))
            reconnect_time = start_time + (test_dur_secs * (2/3))
            self.duration_scheduler.enterabs(disconnect_time, 1, self._disconnect_clients)
            self.duration_scheduler.enterabs(reconnect_time, 1, self._connect_all_clients)
                
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
        
        try:
            ischedule.run_loop(stop_event=self.stop_event)
        except:
            self.stop_event.clear()
            raise
        
        print(f"\tTest complete! Cleaning up...")
        
        # Give few seconds to finish publications and receives
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
            
            # Assign callbacks
            client.on_connect = self._on_connect
            client.on_disconnect = self._on_disconnect
            client.on_subscribe = self._on_subscribe
            client.on_publish = self._on_publish
            client.on_message = self._on_message_recv
            
            # Add to all clients
            test_client = self.TestClient(client, name)
            client.user_data_set(test_client) # Set reference to client data
            self.all_clients.append(test_client)
            
            
    def _connect_all_clients(self) -> None:
        # Connect any non-connected client specified number of clients
        clients_to_connect = [x for x in self.all_clients if not x.is_connected]
        
        for test_client in clients_to_connect:
            result_code = GlobalDefs.CLIENT_MODULE.connect_client(test_client.client, self.broker_address, self.broker_port)
    
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
            
            # Lock this section to prevent a race condition with _on_subscribe
            self.subscribe_lock.acquire()
            
            results = GlobalDefs.CLIENT_MODULE.subscribe_for_operations(test_client.client, self.method, topic)
            
            # Save time
            now = time.time()
            
            # Store results to correlate with subscription ids
            for result_code, mid, sub_id in results:
                if result_code != mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
                    raise RuntimeError(f"Failed to subscribe on {topic} with purpose {purpose_for_subscription}")
                else:
                    if not test_client.name in self.pending_subscribes:
                        self.pending_subscribes[test_client.name] = dict()
                    self.pending_subscribes[test_client.name][mid] = (topic, purpose_for_subscription, sub_id, now)
                
            # Release lock
            self.subscribe_lock.release()
            
                
    def _subscribe_client_for_data(self, test_client: TestClient):

        # Check if topics already exist, if so resubscribe to these
        if len(test_client.subscribed_topics) > 0:
            
            for topic in test_client.subscribed_topics:
                purpose_for_subscription = test_client.subscribed_topics[topic]
                
                # Lock this section to prevent a race condition with _on_subscribe
                self.subscribe_lock.acquire()
                
                results = GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(test_client.client, self.method, 
                                                                                          topic, purpose_for_subscription, self.current_config.qos)
                # Save time
                now = time.time()
                
                # Store results to correlate with subscription ids
                for result_code, mid, sub_id in results:
                    if result_code != mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
                        raise RuntimeError(f"Failed to subscribe on {topic} with purpose {purpose_for_subscription}")
                    else:
                        if not test_client.name in self.pending_subscribes:
                            self.pending_subscribes[test_client.name] = dict()
                        self.pending_subscribes[test_client.name][mid] = (topic, purpose_for_subscription, sub_id, now)
                
                # Release lock
                self.subscribe_lock.release()
        
        # If not, determine new topics and purpose to subscribe on
        else:
            # Determine the specified number of topics to connect to 
            topics_for_subscription_count = (int)(len(self.current_config.subscribe_topic_list) * self.current_config.pct_topics_per_client)
            topics_for_subscription = random.sample(self.current_config.subscribe_topic_list, topics_for_subscription_count)
            
            for topic in topics_for_subscription:
            
                # Select a purpose filter for this topic
                purpose_for_subscription = random.choice(self.current_config.subscribe_purpose_list)
                
                # Lock this section to prevent a race condition with _on_subscribe
                self.subscribe_lock.acquire()
                
                results = GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(test_client.client, self.method, 
                                                                                          topic, purpose_for_subscription)
                
                # Save time
                now = time.time()
                
                # Store results to correlate with subscription ids
                for result_code, mid, sub_id in results:
                    if result_code != mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
                        raise RuntimeError(f"Failed to subscribe on {topic} with purpose {purpose_for_subscription}")
                    else:
                        if not test_client.name in self.pending_subscribes:
                            self.pending_subscribes[test_client.name] = dict()
                        self.pending_subscribes[test_client.name][mid] = (topic, purpose_for_subscription, sub_id, now)
                        test_client.subscribed_topics[topic] = purpose_for_subscription
                
                # Release lock
                self.subscribe_lock.release()
                
                
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
        clients_to_disconnect_count = (int)(len(connected_clients) * 0.25)
        clients_to_disconnect = random.sample(connected_clients, clients_to_disconnect_count)
                
        for test_client in clients_to_disconnect:
            result_code = GlobalDefs.CLIENT_MODULE.disconnect_client(test_client.client)
    
            if result_code == mqtt.MQTT_ERR_NO_CONN:
                # Already disconnected
                test_client.is_connected = False
                
            elif result_code != mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"Failed to disconnect client {test_client.name}")
            
            
    def _disconnect_all_clients(self) -> None:
        # Find connected clients
        connected_clients = [client for client in self.all_clients if client.is_connected]
            
        for test_client in connected_clients:
            GlobalDefs.CLIENT_MODULE.disconnect_client(test_client.client)
            
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
            else:
                self._publish_data(test_client)
             
                    
    def _publish_operation(self, test_client: TestClient):
        
        message_counter = test_client.get_send_counter()
        
        # Select an operation for this topic
        operation = random.choice(list(self.current_config.possible_operations.keys()))
        
        # Lock this section to prevent a race condition with _on_publish
        self.publish_lock.acquire()
        
        results = GlobalDefs.CLIENT_MODULE.publish_operation_request(test_client.client, self.method, operation, message_counter)
        
        # Save time
        now = time.time()
        
        for message_info, topic in results:
            if not test_client.name in self.pending_publishes:
                self.pending_publishes[test_client.name] = dict()
                
            # For purpose management method 1, we need to extract the topic properly
            if self.method == GlobalDefs.PurposeManagementMethod.PM_1:
                purpose_start_index = topic.rfind('[')
                topic = topic[:purpose_start_index - 1]
                
            self.pending_publishes[test_client.name][message_info.mid] = (topic, GlobalDefs.OP_PURPOSE, operation, now)
            
            # Save message counter for correlations
            test_client.message_id_to_send_counter[message_info.mid] = message_counter
            
        # Release lock
        self.publish_lock.release()
            
    
    def _publish_data(self, test_client: TestClient):
        # Publish on specified number of topics
        topics_to_publish_on_count = ceil((len(test_client.publish_topics) * self.current_config.pct_topics_per_pub))
        topics_to_publish_on = random.sample(sorted(test_client.publish_topics.keys()), topics_to_publish_on_count)
        
        for topic in topics_to_publish_on:
            
            # Generate a payload to simulate variable data (if requested)
            payload = None
            num_bytes = random.randrange(self.current_config.min_payload_length_bytes, self.current_config.max_payload_length_bytes)         
            if num_bytes > 0:
                payload = random.randbytes(num_bytes)
                
            # Lock this section to prevent a race condition with _on_publish
            self.publish_lock.acquire()
                
            message_counter = test_client.get_send_counter()
            
            results = GlobalDefs.CLIENT_MODULE.publish_with_purpose(test_client.client, self.method, topic, 
                                                                    test_client.publish_topics[topic], qos=self.current_config.qos, payload=payload, correlation_data=message_counter)
            
            # Save time
            now = time.time()
            
            for message_info, topic in results:
                if not test_client.name in self.pending_publishes:
                    self.pending_publishes[test_client.name] = dict()
                    
                # For purpose management method 1, we need to extract the topic properly
                if self.method == GlobalDefs.PurposeManagementMethod.PM_1:
                    purpose_start_index = topic.rfind('[')
                    topic = topic[:purpose_start_index - 1]
                    
                self.pending_publishes[test_client.name][message_info.mid] = (topic, test_client.publish_topics[topic], "DATA", now)
                
                # Save message counter for correlations
                test_client.message_id_to_send_counter[message_info.mid] = message_counter
                
            # Release lock
            self.publish_lock.release()
        
        
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
            
    def _on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        
        # Make sure we're not in a subscribeh
        self.subscribe_lock.acquire()
        
        # We can release immediately since this is a callback
        # We only had to block until the publications which will 
        # trigger this function had their information set
        self.subscribe_lock.release()
        
        # Check if message exists and was successful
        if userdata.name in self.pending_subscribes:
            if mid in self.pending_subscribes[userdata.name]: 
                    
                # We only do one subscribe per packet so this is always len one and is success for QoS 0/1/2
                if reason_code_list[0] == 0 or reason_code_list[0] == 1 or reason_code_list[0] == 2:
                    topic_filter, purpose_filter, sub_id, time = self.pending_subscribes[userdata.name][mid]
                    GlobalDefs.LOGGING_MODULE.log_subscribe(time, self.my_id, userdata.name, topic_filter, purpose_filter, sub_id)
        
    
    def _on_publish(self, client: mqtt.Client, userdata: TestClient, mid:int, reason_code: ReasonCode, properties: Properties):    
        
        # Make sure we're not in a publish
        self.publish_lock.acquire()
        
        # We can release immediately since this is a callback
        # We only had to block until the publications which will 
        # trigger this function had their information set
        self.publish_lock.release()
        
        # Check if message exists and was successful
        if userdata.name in self.pending_publishes:
            if mid in self.pending_publishes[userdata.name]: 
                
                corr_data = userdata.message_id_to_send_counter[mid]
                    
                # If successful
                if reason_code == 0:
                    topic, purpose, op_type, time = self.pending_publishes[userdata.name][mid]
                    
                    # Do not log communications to the broker
                    # if not topic[0] == '$':
                    
                    # Check if operational or data
                    if op_type == "DATA":
                        # Log message
                        GlobalDefs.LOGGING_MODULE.log_publish(time, self.my_id, userdata.name, corr_data, topic, purpose, op_type)
                    else:
                        # Log message
                        GlobalDefs.LOGGING_MODULE.log_operation_publish(time, self.my_id, userdata.name, corr_data, topic, purpose, op_type, self.current_config.possible_operations[op_type])
                
                
    def _on_message_recv(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage):
        
        operational_message = False
        operation_type = ""
        sending_client = "UNKNOWN"
        op_message_type = "OP"
        correlation_data = -1
        sub_id: List[int] = list()
        
        # Check properties
        if message.properties is not None:
            if hasattr(message.properties, "UserProperty"):
                for name, value in message.properties.UserProperty:
                    if name == GlobalDefs.PROPERTY_OPERATION:
                        operational_message = True
                        operation_type = value
                    elif name == GlobalDefs.PROPERTY_ID:
                        sending_client = value
                    elif name == GlobalDefs.PROPERTY_OP_STATUS:
                        op_message_type = value
                        
            if hasattr(message.properties, "CorrelationData"):
                correlation_data = int.from_bytes(message.properties.CorrelationData, byteorder='big', signed=False)
                
            if hasattr(message.properties, "SubscriptionIdentifier"):
                sub_id = message.properties.SubscriptionIdentifier
            else:
                sub_id.append(-1)

        # Log messages
        if operational_message:
            GlobalDefs.LOGGING_MODULE.log_operation_recv(time.time(), self.my_id, userdata.name, sending_client, correlation_data, message.topic, operation_type, self.current_config.possible_operations[operation_type], op_message_type, sub_id[0])
        else:
            GlobalDefs.LOGGING_MODULE.log_recv(time.time(), self.my_id, userdata.name, sending_client, correlation_data, message.topic, "DATA", sub_id[0])