import threading
import paho.mqtt.client as mqtt
import ClientInterface
from Benchmark import PurposeManagementMethod
from typing import Any

class SyncModule:
       
    my_id: str
    client: mqtt.Client  
    benchmark_ready_states: dict[str, bool]
    benchmark_done_states: dict[str, bool]
    THIS_NODE_READY_TOPIC: str
    THIS_NODE_DONE_TOPIC: str
    
    READY_TOPIC_PREFIX = "benchmark/ready_state/"
    DONE_TOPIC_PREFIX = "benchmark/done_state/"
    SYNC_PURPOSE = "lifecycle"
    
    def __init__(self, my_id: str, expected_benchmarks: list[str], broker_address: str, broker_port: int, method: PurposeManagementMethod):
        
        # Initialize class members
        self.benchmark_ready_states = {benchmark: False for benchmark in expected_benchmarks}
        self.benchmark_done_states = {benchmark: False for benchmark in expected_benchmarks}
        self.my_id = my_id
        self.THIS_NODE_READY_TOPIC = SyncModule.READY_TOPIC_PREFIX + my_id
        self.THIS_NODE_DONE_TOPIC = SyncModule.DONE_TOPIC_PREFIX + my_id
        
        # Create and connect client
        client = ClientInterface.create_v5_client("syncronization_client")
        result_code = ClientInterface.connect_client(client, broker_address, broker_port)
    
        if result_code == mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
            self.client = client
        else:
            raise RuntimeError("Failed to create Syncronization Client")
        
        # Attach message handler
        self.client.on_message = self._on_message_recv
    
        # Start the client loop
        self.client.loop_start()
        
        # Subscribe to ready and done topics for each benchmark
        for id in expected_benchmarks:
            
            # Don't subscribe to this node's topic
            if id == my_id:
                continue
            
            # Register subscriptions
            ClientInterface.subscribe_with_purpose_filter(self.client, method, self._on_message_recv, 
                                                            self.THIS_NODE_READY_TOPIC, SyncModule.SYNC_PURPOSE)
            ClientInterface.subscribe_with_purpose_filter(self.client, method, self._on_message_recv, 
                                                           self.THIS_NODE_DONE_TOPIC, SyncModule.SYNC_PURPOSE)
    
            # Prep publications (if the method needs it)
            ClientInterface.register_publish_purpose_for_topic(self.client, method, self.THIS_NODE_READY_TOPIC, SyncModule.SYNC_PURPOSE)
            ClientInterface.register_publish_purpose_for_topic(self.client, method, self.THIS_NODE_DONE_TOPIC, SyncModule.SYNC_PURPOSE)
    
    
    def _on_message_recv(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage):    
     
        # Parse message properties
        topic = message.topic
        fields = message.payload.decode().split(':')
        id = fields[0]
        status = fields[1]
        
        # Set fields as needed for topic
        if topic.startswith(self.READY_TOPIC_PREFIX):
            if id in self.benchmark_ready_states and status == "READY":
                self.benchmark_ready_states[id] = True
            
        elif topic.startswith(self.DONE_TOPIC_PREFIX):
            if id in self.benchmark_done_states and status == "DONE":
                self.benchmark_done_states[id] = True   
        
    
    def notify_ready(self, method: PurposeManagementMethod):
        ClientInterface.publish_with_purpose(self.client, method, self.THIS_NODE_READY_TOPIC, SyncModule.SYNC_PURPOSE, payload=f"{self.my_id}:READY")
        if self.my_id in self.benchmark_ready_states:
            self.benchmark_ready_states[self.my_id] = True
            
   
    def notify_done(self, method: PurposeManagementMethod):
        ClientInterface.publish_with_purpose(self.client, method, self.THIS_NODE_DONE_TOPIC, SyncModule.SYNC_PURPOSE, payload=f"{self.my_id}:DONE")
        if self.my_id in self.benchmark_done_states:
            self.benchmark_done_states[self.my_id] = True