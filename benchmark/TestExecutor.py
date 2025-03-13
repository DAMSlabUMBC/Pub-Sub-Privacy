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
    
    # === Purpose information ===   
    purpose_list: List[str] = list()
    
    # Used to randomly generate purposes
    generate_purposes: bool = True
    purpose_count: int = 0
    
    # === Publication information ===   
    pct_to_publish_on: float = 1.0
    pct_topics_per_pub: float = 1.0
    pub_period_ms: int = 500
    min_payload_length_bytes: int = 1
    max_payload_length_bytes: int = 1024
    
    def __init__(self, name:str, test_duration_ms: int, client_count: int):
        self.name = name
        self.test_duration_ms = test_duration_ms
        self.client_count = client_count
    

class TestExecutor:
    
    class TestClient:
        client: mqtt.Client
        name: str
        subscribed_topics: Dict[str, str] = dict() # Maps topic filter to purpose filter
        publish_topics: Dict[str, str] = dict() # Maps topic to purpose filter
        is_connected: bool = False
        
        def __init__(self, client: mqtt.Client, name: str):
            self.client = client
            self.name = name
    
    my_id: str
    broker_address: str
    broker_port: int
    method: GlobalDefs.PurposeManagementMethod
    current_config: TestConfiguration
    stop_event: threading.Event = threading.Event()
    duration_scheduler: sched.scheduler
    
    all_clients: List[TestClient] = list()
    pending_publishes: Dict[int, Tuple[str, str, str]] = dict() # message id => (topic, purpose, message_type)
    
    def __init__(self, executor_id: str, broker_address: str, broker_port: int, method: GlobalDefs.PurposeManagementMethod, seed: int | None = None):
        
        self.my_id = executor_id
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.method = method
        
        # If not supplied, set random seed based on time
        if seed is None:
            seed = time.time()
            
        #  Seed the random number generator and write seed to log file
        random.seed(seed)
        GlobalDefs.LOGGING_MODULE.log_seed(seed)
        
        # Configure scheduler
        self.duration_scheduler = sched.scheduler()
        
    
    def perform_test(self, test_config: TestConfiguration):
        
        # Need to save this for scheduled functions
        # but we'll also use in other functions
        self.current_config = test_config
        
        # == Setup ==
        # Create clients
        self._create_clients()
        
        # TODO Generate subscriptions and purposes if needed
        
        # Determine which topic/purposes each client will publish to
        self._assign_publication_topics_and_purposes()
        
        # Connect clients
        self._connect_initial_clients()
        
        # == Setup scheduler tasks, timing is in seconds and needs to be divided by 1000 ==
        # Publish task
        ischedule.schedule(self._publish_from_clients, interval=0.5)
        
        # Connect task
        if test_config.reconnect_period_ms > 0:
            ischedule.schedule(self._reconnect_clients, interval=(test_config.reconnect_period_ms / 1000.0))
        
        # Disconnect task
        if test_config.disconnect_period_ms > 0:
            ischedule.schedule(self._disconnect_clients, interval=(test_config.disconnect_period_ms / 1000.0))
            
        # TODO: Add purpose reshuffle
        
        # == Main test loop ==
        # Calculate end time
        end_time = time.monotonic() + (test_config.test_duration_ms / 1000.0) # Convert to seconds
                
        # Start scheduler processing
        self.stop_event.clear()
        
        # Set event to be triggered at the test duration end
        self.duration_scheduler.enterabs(end_time, 1, self._end_test)
        stop_thread = threading.Thread(target=self.duration_scheduler.run)
        stop_thread.start()
        
        # Start the threads
        # NOTE this must be done AFTER the scheduler above, or it won't terminate
        ischedule.run_loop(stop_event=self.stop_event)

        # Clear scheduled tasks for next loop start
        ischedule.reset()
        
        
        #self.clients: List[Client] = []
        #self.topics = {"topic1", "topic2", "topic3"}  # Example topics
        #self.purposes = {"chat", "alert", "update"}  # Example purposes
        
        # Randomly select subset of topics for each client
        #num_topics = random.randint(1, len(self.topics))
        #subscribed_topics = set(random.sample(list(self.topics), num_topics))
        #self.clients.append(Client(id=i, subscribed_topics=subscribed_topics))
            
            
    def _end_test(self):
        # Ending just means triggering the stop event
        self.stop_event.set()
        
        
    """1. Create specified number of clients with random topic subscriptions""" 
    def _create_clients(self) -> None:
        
        # Create the specified number of clients
        for i in range(self.current_config.client_count):    
            
            name = f"{self.my_id}__{self.current_config.name}__{i}"
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
            
            
    def _subscribe_initial_clients(self) -> None:
        
        # All connected clients should have a subscription
        connected_clients = [client for client in self.all_clients if client.is_connected]
        
        for test_client in connected_clients:
            self._subscribe_client(test_client)
            
                
    def _subscribe_client(self, test_client: TestClient):

        # Check if topics already exist, if so resubscribe to these
        if len(test_client.subscribed_topics) > 0:
            
            for topic in test_client.subscribed_topics:
                purpose_for_subscription = test_client.subscribed_topics[topic]
                
                result_code, mid = GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(test_client.client, self.method, self._on_data_message_recv, 
                                                                                          topic, purpose_for_subscription)
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
                purpose_for_subscription = random.choice(self.current_config.purpose_list)
                
                result_code, mid = GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(test_client.client, self.method, self._on_data_message_recv, 
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
            topics_for_subscription_count = (int)(len(self.current_config.publish_topic_list) * self.current_config.pct_topics_per_client)
            topics_for_subscription = random.sample(self.current_config.publish_topic_list, topics_for_subscription_count)
            
            for topic in topics_for_subscription:
                # Assign a purpose
                purpose_for_publication = random.choice(self.current_config.purpose_list)
                test_client.publish_topics[topic] = purpose_for_publication
                
                
    def _register_initial_publication_purposes(self) -> None:
        
        # All connected clients should have a publication purpose sent
        connected_clients = [client for client in self.all_clients if client.is_connected]
        
        for test_client in connected_clients:
            self._register_initial_publication_purposes_for_client(test_client)
            
                
    def _register_initial_publication_purposes_for_client(self, test_client: TestClient) -> None:

        # All clients have already had this done
        for topic in test_client.publish_topics:
            # Prep publications (if the method needs it)
            GlobalDefs.CLIENT_MODULE.register_publish_purpose_for_topic(test_client.client, self.method, topic, test_client.publish_topics[topic])
            
            
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
            
    
    def _publish_from_clients(self):
                
        # Find connected clients
        connected_clients = [client for client in self.all_clients if client.is_connected]
        
        # Publish on specified number of clients
        clients_to_publish_count = (int)(len(connected_clients) * self.current_config.pct_to_publish_on)
        clients_to_publish = random.sample(connected_clients, clients_to_publish_count)
        
        for test_client in clients_to_publish:
            
            # Publish on specified number of topics
            topics_to_publish_on_count = (int)(len(test_client.publish_topics) * self.current_config.pct_topics_per_pub)
            topics_to_publish_on = random.sample(sorted(test_client.publish_topics.keys()), topics_to_publish_on_count)
            
            for topic in topics_to_publish_on:
                
                # Generate a payload to simulate variable data (if requested)
                payload = None
                num_bytes = random.randrange(self.current_config.min_payload_length_bytes, self.current_config.max_payload_length_bytes)         
                if num_bytes > 0:
                    payload = random.randbytes(num_bytes)
                
                results = GlobalDefs.CLIENT_MODULE.publish_with_purpose(test_client.client, self.method, topic, 
                                                                        test_client.publish_topics[topic], qos=self.current_config.qos, payload=payload)
                
                for message_info, topic in results:
                    self.pending_publishes[message_info.mid] = (topic, test_client.publish_topics[topic], "DATA")
                    
            
    # On action functions
    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: mqtt.ConnectFlags, reason_code: ReasonCode, properties: Properties):   
        if reason_code == 0: # Success
            # Userdata is a TestClient object
            userdata.is_connected = True
            
            # Log
            GlobalDefs.LOGGING_MODULE.log_connect(time.time(), self.my_id, userdata.name)
            
            # Resubscribe clients
            self._subscribe_client(userdata)
        
            # Reestablish publication purpose
            self._register_initial_publication_purposes_for_client(userdata)
            
            
    def _on_disconnect(self, client: mqtt.Client, userdata: Any, flags: mqtt.DisconnectFlags, reason_code: ReasonCode, properties: Properties):
        if reason_code == 0: # Success
            # Userdata is a TestClient object
            userdata.is_connected = False
            
            # Log
            GlobalDefs.LOGGING_MODULE.log_disconnect(time.time(), self.my_id, userdata.name)
    
    def _on_publish(self, client: mqtt.Client, userdata: TestClient, mid:int, reason_code: ReasonCode, properties: Properties):    
        
        # Check if message exists and was successful
        if mid in self.pending_publishes:    
                   
            if reason_code == 0:
                topic, purpose, message_type = self.pending_publishes[mid]
                
                # Log message
                GlobalDefs.LOGGING_MODULE.log_publish(time.time(), self.my_id, userdata.name, mid, topic, purpose, message_type)
                
            
    def _on_data_message_recv(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage):    
        # Log message
        GlobalDefs.LOGGING_MODULE.log_recv(time.time(), self.my_id, userdata.name, message.mid, message.topic, "DATA")