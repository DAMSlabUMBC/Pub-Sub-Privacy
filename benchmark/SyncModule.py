import paho.mqtt.client as mqtt
import GlobalDefs
from typing import Any
from time import sleep

class BenchmarkSynchronizer:
       
    my_id: str
    client: mqtt.Client  
    benchmark_ready_states: dict[str, bool]
    benchmark_done_states: dict[str, bool]
    THIS_NODE_READY_TOPIC: str
    THIS_NODE_DONE_TOPIC: str
    
    READY_TOPIC_PREFIX = "benchmark/ready_state/"
    READY_TOPIC_FILTER = "benchmark/ready_state/#"
    DONE_TOPIC_PREFIX = "benchmark/done_state/"
    DONE_TOPIC_FILTER = "benchmark/done_state/#"
    SYNC_PURPOSE = "lifecycle"
    
    def __init__(self, my_id: str, expected_benchmarks: list[str]):
        
        # Initialize class members
        self.benchmark_ready_states = {benchmark: False for benchmark in expected_benchmarks}
        self.benchmark_done_states = {benchmark: False for benchmark in expected_benchmarks}
        self.my_id = my_id
        self.THIS_NODE_READY_TOPIC = BenchmarkSynchronizer.READY_TOPIC_PREFIX + my_id
        self.THIS_NODE_DONE_TOPIC = BenchmarkSynchronizer.DONE_TOPIC_PREFIX + my_id


    def start(self, broker_address: str, broker_port: int, method: GlobalDefs.PurposeManagementMethod):
        # Create and connect client
        client = GlobalDefs.CLIENT_MODULE.create_v5_client(f"{self.my_id} sync client")
        result_code = GlobalDefs.CLIENT_MODULE.connect_client(client, broker_address, broker_port)
    
        if result_code == mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
            self.client = client
        else:
            raise RuntimeError("Failed to create Syncronization Client")

        # Attach message handler
        self.client.on_message = self._on_message_recv

        # Start the client loop
        self.client.loop_start()
        
        # Subscribe to ready and done topics for each benchmark
        for id in self.benchmark_ready_states.keys():
            
            # Don't subscribe to this node's topic
            if id == self.my_id:
                continue
            
            # Register subscriptions
            # For broker-unaware purpose filtering, no wildcards can be used
            if method == GlobalDefs.PurposeManagementMethod.PM_0:
                for benchmark in self.benchmark_ready_states.keys():
                    GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(self.client, method, self._on_message_recv, 
                                                            self.READY_TOPIC_PREFIX + benchmark, BenchmarkSynchronizer.SYNC_PURPOSE, qos=1)
                    GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(self.client, method, self._on_message_recv, 
                                                            self.DONE_TOPIC_PREFIX + benchmark, BenchmarkSynchronizer.SYNC_PURPOSE, qos=1)
                   
            # Can use wildcards for the rest 
            else:
                GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(self.client, method, self._on_message_recv, 
                                                                self.READY_TOPIC_FILTER, BenchmarkSynchronizer.SYNC_PURPOSE, qos=1)
                GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(self.client, method, self._on_message_recv, 
                                                            self.DONE_TOPIC_FILTER, BenchmarkSynchronizer.SYNC_PURPOSE, qos=1)
        
                # Prep publications (if the method needs it)
                GlobalDefs.CLIENT_MODULE.register_publish_purpose_for_topic(self.client, method, self.THIS_NODE_READY_TOPIC, BenchmarkSynchronizer.SYNC_PURPOSE)
                GlobalDefs.CLIENT_MODULE.register_publish_purpose_for_topic(self.client, method, self.THIS_NODE_DONE_TOPIC, BenchmarkSynchronizer.SYNC_PURPOSE)
        
    def _on_message_recv(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage):    
     
        # Parse message properties
        topic = message.topic
        fields = message.payload.decode().split(':')
        id = fields[0]
        status = fields[1]
        
        # Set fields as needed for topic
        if topic.startswith(self.READY_TOPIC_PREFIX):
            if id in self.benchmark_ready_states and status == "READY" and not self.benchmark_ready_states[id]:
                self.benchmark_ready_states[id] = True
                
                # Log the status of benchmarks ready, including the total number
                benchmark_total_count = len(self.benchmark_ready_states)
                benchmark_ready_count = sum(1 for x in self.benchmark_ready_states.values() if x == True)
                print(f"{id} READY ({benchmark_ready_count}/{benchmark_total_count})")
            
        elif topic.startswith(self.DONE_TOPIC_PREFIX):
            if id in self.benchmark_done_states and status == "DONE" and not self.benchmark_done_states[id]:
                self.benchmark_done_states[id] = True   
                
                # Log the status of benchmarks ready, including the total number
                benchmark_total_count = len(self.benchmark_done_states)
                benchmark_done_count = sum(1 for x in self.benchmark_done_states.values() if x)
                print(f"{id} DONE ({benchmark_done_count}/{benchmark_total_count})")
        
    
    def _notify_ready(self, method: GlobalDefs.PurposeManagementMethod):
        GlobalDefs.CLIENT_MODULE.publish_with_purpose(self.client, method, self.THIS_NODE_READY_TOPIC, BenchmarkSynchronizer.SYNC_PURPOSE, qos=1, payload=f"{self.my_id}:READY")
            
   
    def _notify_done(self, method: GlobalDefs.PurposeManagementMethod):
        GlobalDefs.CLIENT_MODULE.publish_with_purpose(self.client, method, self.THIS_NODE_DONE_TOPIC, BenchmarkSynchronizer.SYNC_PURPOSE, qos=1, payload=f"{self.my_id}:DONE")


    def notify_and_wait_for_ready(self, method: GlobalDefs.PurposeManagementMethod) -> bool:
        
        while True:
            # We need to periodically send out that we're ready, otherwise the other benchmarks won't start
            self._notify_ready(method)

            # Wait until all benchmarks are ready and return True.
            if all(self.benchmark_ready_states.values()):
                 return True

            # Sleep to prevent busy-waiting
            sleep(1)            
            

    def notify_and_wait_for_done(self, method: GlobalDefs.PurposeManagementMethod) -> bool:
        
        while True:
            # We need to periodically send out that we're done, otherwise the other benchmarks won't finish
            self._notify_done(method)

            # Wait until all benchmarks are done and return True.
            if all(self.benchmark_done_states.values()):
                 return True

            # Sleep to prevent busy-waiting
            sleep(1) 