import paho.mqtt.client as mqtt
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.properties import Properties
import random
import time
import threading
import ischedule
import sched
import GlobalDefs
from typing import List, Set, Tuple, Any
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
    topic_list: List[str] = list()
    
    # Used to randomly generate topics
    generate_topics: bool = True
    topic_count: int = 0
    
    # === Purpose information ===   
    pct_purpose_per_client: float = 1.0
    purpose_list: List[str] = list()
    
    # Used to randomly generate purposes
    generate_purposes: bool = True
    purpose_count: int = 0
    
    # === Publication information ===   
    pct_to_publish_on: float = 1.0
    pct_topics_per_pub: float = 1.0
    pub_period_ms: int = 1000
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
        subscribed_topics: List[Tuple[str, str]] = list() # Maps topic filter to purpose filter
        publish_topics: List[Tuple[str, str]] = list() # Maps topic to purpose filter
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
    
    def __init__(self, executor_id: str, broker_address: str, broker_port: int, method: GlobalDefs.PurposeManagementMethod, seed: int | None = None):
        
        self.my_id = executor_id
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.method = method
        
        # If not supplied, set random seed based on time
        if seed is None:
            seed = (int)(datetime.datetime.now().timestamp())
            
        #  Seed the random number generator and write seed to log file
        random.seed(seed)
        GlobalDefs.LOGGING_MODULE.log_seed(seed)
        
        # Configure scheduler
        self.duration_scheduler = sched.scheduler(time.monotonic, time.sleep)
        
    
    def perform_test(self, test_config: TestConfiguration):
        
        # Need to save this for scheduled functions
        # but we'll also use in other functions
        self.current_config = test_config
        
        # == Setup ==
        # Create clients
        self._create_clients()
        
        # Connect clients
        self._connect_initial_clients()
        
        # TODO: Subscribe clients
        
        # TODO: Register publication purposes (if needed by method)
        
        # == Setup scheduler tasks, timing is in seconds and needs to be divided by 1000 ==
        # Publish task
        ischedule.schedule(self._publish_from_clients, interval=(test_config.pub_period_ms / 1000))
        
        # Connect task
        if test_config.reconnect_period_ms > 0:
            ischedule.schedule(self._reconnect_clients, interval=(test_config.reconnect_period_ms / 1000))
        
        # Disconnect task - Lowest priority of 3
        if test_config.disconnect_period_ms > 0:
            ischedule.schedule(self._disconnect_clients, interval=(test_config.disconnect_period_ms / 1000))
        
        # == Main test loop ==
        # Calculate end time
        end_time = time.monotonic() + (test_config.test_duration_ms / 1000) # Convert to seconds
        
        # Start scheduler processing
        self.stop_event.clear()
        ischedule.run_loop(stop_event=self.stop_event)
        
        # Set event to be triggered at the test duration end
        self.duration_scheduler.enterabs(end_time, 1, self._end_test)
        
        # Block until stop
        self.stop_event.wait()
        
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
        print("STOPPING")
        self.stop_event.set()
        
        
    """1. Create specified number of clients with random topic subscriptions""" 
    def _create_clients(self) -> None:
        
        # Create the specified number of clients
        for i in range(self.current_config.client_count):    
            
            name = f"{self.my_id}__{self.current_config.name}__{i}"
            client = GlobalDefs.CLIENT_MODULE.create_v5_client(name)
            
            # Add to all clients
            test_client = self.TestClient(client, name)
            client.user_data_set(test_client) # Set reference to client data
            self.all_clients.append(test_client)
            
            
    def _connect_initial_clients(self,) -> None:
        
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
            topics_to_publish_on = random.sample(test_client.publish_topics, topics_to_publish_on_count)
            
            for topic, purpose in topics_to_publish_on:
                
                # Generate a payload to simulate variable data (if requested)
                payload = None
                num_bytes = random.randrange(self.current_config.min_payload_length_bytes, self.current_config.max_payload_length_bytes)         
                if num_bytes > 0:
                    payload = random.randbytes(num_bytes)
                
                results = GlobalDefs.CLIENT_MODULE.publish_with_purpose(test_client.client, self.method, topic, 
                                                                        purpose, qos=self.current_config.qos, payload=payload)
            
            
    # On action functions
    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: mqtt.ConnectFlags, reason_code: ReasonCode, properties: Properties):   
        if reason_code == 0: # Success
            # Userdata is a TestClient object
            userdata.is_connected = True
            
    def _on_disconnect(self, client: mqtt.Client, userdata: Any, flags: mqtt.DisconnectFlags, reason_code: ReasonCode, properties: Properties):
        if reason_code == 0: # Success
            # Userdata is a TestClient object
            userdata.is_connected = False