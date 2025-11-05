import paho.mqtt.client as mqtt
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.properties import Properties
import time
import random
import ischedule
import threading
import sched
from typing import Dict, List, Optional, Tuple, Any
import GlobalDefs
from ConfigParser import TestConfiguration
from EventScheduler import EventScheduler
from DeviceDefinitions import (
    DeviceManager, DeviceInstance, PublisherDefinition,
    SubscriberDefinition, PurposeDefinition, DeviceDefinition
)
from BrokerMonitor import BrokerMonitor
from LoggingModule import console_log, ConsoleLogLevel

class DeterministicTestExecutor():
    """Test executor with deterministic event scheduling and per-device publication rates"""

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

    def __init__(self, executor_id: str, broker_address: str, broker_port: int,
                 method: GlobalDefs.PurposeManagementMethod):
                
        self.my_id = executor_id
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.method = method
        self.pending_publishes = dict()
        self.pending_subscribes = dict()
        self.stop_event = threading.Event()
        self.publish_lock = threading.Lock()
        self.subscribe_lock = threading.Lock()
          
        #  Seed the random number generator and write seed to log file
        seed = int(time.time())
        random.seed(time.time())
        GlobalDefs.LOGGING_MODULE.log_seed(seed)
        
        # Configure scheduler
        self.duration_scheduler = sched.scheduler()

        # Scheduling and device management
        self.event_scheduler = EventScheduler()
        self.device_manager = DeviceManager()
        self.broker_monitor: Optional[BrokerMonitor] = None

    def setup_test(self, test_config: TestConfiguration):
        # """Setup test with deterministic scheduling"""
        console_log(ConsoleLogLevel.DEBUG, f"Configuring test {test_config.name}", __name__)

        # Clear previous test data
        self._clear_previous_test_data()
        self.current_config = test_config

        # Setup purpose definitions
        self._setup_purpose_definitions(test_config)

        # Setup device definitions
        self._setup_device_definitions(test_config)

        # Create device instances
        self._create_device_instances(test_config)

        # Setup event scheduler
        self._setup_event_scheduler(test_config)
        self.event_scheduler.print_schedule()

        # Setup broker monitoring if enabled
        if test_config.monitor_broker:
            self._setup_broker_monitoring(test_config)

        console_log(ConsoleLogLevel.DEBUG, f"Test configured!",  __name__)
        
    def _clear_previous_test_data(self):
        self.device_manager.clear()
        self.event_scheduler.clear()
        self.pending_publishes = dict()
        if self.broker_monitor:
            self.broker_monitor.clear_samples()
        
        self.current_config = None
        self.pending_publishes = dict()
        ischedule.reset()

    def perform_test(self, test_config: TestConfiguration):
        """Run a test with deterministic scheduling"""

        console_log(ConsoleLogLevel.INFO, f"Starting test {test_config.name} at {time.strftime('%Y-%m-%d %H:%M:%S')}", __name__)
        console_log(ConsoleLogLevel.INFO, f"Test will run for {test_config.test_duration_ms / 1000.0} second(s)")

        # Start scheduler and timer
        test_start_time_ms = time.monotonic() * 1000.0
        self.event_scheduler.start()

        if self.broker_monitor:
            self.broker_monitor.start_monitoring()

        # Main test loop
        test_end_time_ms = test_start_time_ms + test_config.test_duration_ms

        try:
            while time.monotonic() * 1000.0 < test_end_time_ms:
                current_time_ms = time.monotonic() * 1000.0
                elapsed_ms = current_time_ms - test_start_time_ms

                # Process scheduled events
                self.event_scheduler.process_due_events()

                # Publish from devices that are ready
                self._publish_from_ready_devices(elapsed_ms)

                # Collect broker metrics if needed
                if self.broker_monitor and self.broker_monitor.should_collect_sample(test_config.monitor_interval_ms):
                    self.broker_monitor.collect_sample()

                # Sleep briefly to avoid busy waiting
                # Calculate sleep time based on next event
                sleep_time = self._calculate_optimal_sleep_time(test_config)
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("\n\tTest interrupted by user")
        except Exception as e:
            print(f"\n\tTest failed with error: {e}")
            raise

        print(f"\tTest complete! Cleaning up...")

        # Stop monitoring
        if self.broker_monitor:
            self.broker_monitor.stop_monitoring()
            self.broker_monitor.print_summary()

        # Give a moment to finish pending operations
        time.sleep(2)

        # Cleanup
        self._disconnect_all_devices()
        self._clear_previous_test_data()

        print(f"Cleanup complete!")

    def _setup_purpose_definitions(self, test_config: TestConfiguration):
        """Load purpose definitions into the device manager"""
        for purpose_id, purpose_info in test_config.purpose_definitions.items():
            purpose_def = PurposeDefinition(
                id=purpose_id,
                description=purpose_info.get('description', '')
            )
            self.device_manager.register_purpose_definition(purpose_def)

    def _setup_device_definitions(self, test_config: TestConfiguration):
        """Load device definitions into the device manager"""
        for dev_id, dev_config in test_config.device_definitions.items():
            dev_type = dev_config['type']

            device_def: DeviceDefinition
            if dev_type == 'publisher':
                device_def = PublisherDefinition(
                    id=dev_id,
                    topic=dev_config['topic'],
                    initial_purpose=dev_config['initial_purpose'],
                    pub_period_ms=dev_config['pub_period_ms'],
                    min_payload_bytes=dev_config['min_payload_bytes'],
                    max_payload_bytes=dev_config['max_payload_bytes']
                )
            elif dev_type == 'subscriber':
                device_def = SubscriberDefinition(
                    id=dev_id,
                    topic_filter=dev_config['topic_filter'],
                    purpose_filter=dev_config['purpose_filter']
                )
            else:
                raise ValueError(f"Unknown device type: {dev_type}")

            self.device_manager.register_device_definition(device_def)

    def _create_device_instances(self, test_config: TestConfiguration):
        """Create device instances from definitions"""
        for instance_config in test_config.device_instances_config:
            device_def_id = instance_config['device_def_id']
            instance_id = instance_config['instance_id']
            count = instance_config.get('count', 1)

            # Create multiple instances if count > 1
            for i in range(count):
                if count > 1:
                    full_instance_id = f"{instance_id}_{i}"
                else:
                    full_instance_id = instance_id

                # Create MQTT client
                client_name = f"{test_config.name}__{full_instance_id}"
                mqtt_client = GlobalDefs.CLIENT_MODULE.create_v5_client(client_name)

                # Set callbacks
                mqtt_client.on_connect = self._on_connect
                mqtt_client.on_disconnect = self._on_disconnect
                mqtt_client.on_subscribe = self._on_subscribe
                mqtt_client.on_publish = self._on_publish
                mqtt_client.on_message = self._on_message_recv

                # Create device instance
                device_instance = self.device_manager.create_device_instance(
                    device_def_id, full_instance_id, mqtt_client, client_name
                )

                # Set user data for callbacks
                mqtt_client.user_data_set(device_instance)

    def _setup_event_scheduler(self, test_config: TestConfiguration):
        """Setup scheduled events"""
        # Register event handlers
        self.event_scheduler.register_handler("connect_all", self._handle_connect_all)
        self.event_scheduler.register_handler("disconnect_all", self._handle_disconnect_all)
        self.event_scheduler.register_handler("connect", self._handle_connect_devices)
        self.event_scheduler.register_handler("disconnect", self._handle_disconnect_devices)
        self.event_scheduler.register_handler("reconnect", self._handle_reconnect_devices)
        self.event_scheduler.register_handler("start_publishing", self._handle_start_publishing)
        self.event_scheduler.register_handler("stop_publishing", self._handle_stop_publishing)
        self.event_scheduler.register_handler("change_purpose", self._handle_change_purpose)

        # Schedule all events
        for event_config in test_config.scheduled_events:
            self.event_scheduler.schedule_event(
                time_ms=event_config['time_ms'],
                event_type=event_config['type'],
                params=event_config,
                description=event_config.get('description', '')
            )

    def _setup_broker_monitoring(self, test_config: TestConfiguration):
        """Setup broker monitoring"""
        self.broker_monitor = BrokerMonitor(test_config.node_exporter_url)

    def _publish_from_ready_devices(self, elapsed_ms: float):
        """Publish from all devices that are ready based on their individual publication rates"""
        ready_publishers = self.device_manager.get_publishers_ready_to_publish(elapsed_ms)

        for device_instance in ready_publishers:
            self._publish_from_device(device_instance, elapsed_ms)

    def _publish_from_device(self, device_instance: DeviceInstance, elapsed_ms: float):
        """Publish a message from a specific device"""
        device_def = device_instance.device_definition
        if not isinstance(device_def, PublisherDefinition):
            return

        # Generate payload
        payload_size = random.randint(device_def.min_payload_bytes, device_def.max_payload_bytes)
        payload = random.randbytes(payload_size) if payload_size > 0 else None

        # Publish message
        self.publish_lock.acquire()
        message_counter = device_instance.message_count

        results = GlobalDefs.CLIENT_MODULE.publish_with_purpose(
            device_instance.mqtt_client,
            self.method,
            device_def.topic,
            device_instance.current_purpose,
            qos=self.current_config.qos,
            payload=payload,
            correlation_data=message_counter
        )

        now = time.time()

        for message_info, topic in results:
            if device_instance.mqtt_client_name not in self.pending_publishes:
                self.pending_publishes[device_instance.mqtt_client_name] = {}

            # Handle PM_1 topic encoding
            if self.method == GlobalDefs.PurposeManagementMethod.PM_1:
                purpose_start_index = topic.rfind('[')
                topic = topic[:purpose_start_index - 1]

            self.pending_publishes[device_instance.mqtt_client_name][message_info.mid] = (
                topic, device_instance.current_purpose, "DATA", now
            )
            
            # Save message counter for correlations
            device_instance.message_id_to_send_counter[message_info.mid] = message_counter

        self.publish_lock.release()

        # Mark as published
        device_instance.mark_published(elapsed_ms)

    def _calculate_optimal_sleep_time(self, test_config: TestConfiguration) -> float:
        """Calculate optimal sleep time based on next event and publication schedules"""
        min_sleep = 0.001  # 1ms minimum
        max_sleep = 0.01  # 10ms maximum

        # Check next scheduled event
        time_until_next_event = self.event_scheduler.get_time_until_next_event_ms()

        # Check next publication from any device
        min_pub_period = float('inf')
        for publisher in self.device_manager.get_all_publishers():
            if isinstance(publisher.device_definition, PublisherDefinition):
                min_pub_period = min(min_pub_period, publisher.device_definition.pub_period_ms)

        # Use the minimum of event time and publication period
        if time_until_next_event is not None:
            sleep_ms = min(time_until_next_event, min_pub_period / 10.0)
        else:
            sleep_ms = min_pub_period / 10.0

        sleep_seconds = sleep_ms / 1000.0
        return max(min_sleep, min(max_sleep, sleep_seconds))

    # Event Handlers
    def _handle_connect_all(self, params):
        """Connect all devices"""
        for device in self.device_manager.get_all_instances():
            self._connect_device(device)

    def _handle_disconnect_all(self, params):
        """Disconnect all devices"""
        for device in self.device_manager.get_all_instances():
            self._disconnect_device(device)

    def _handle_connect_devices(self, params):
        """Connect specific devices"""
        device_ids = params.get('devices', [])
        for device_id in device_ids:
            device = self.device_manager.get_device_instance(device_id)
            if device:
                self._connect_device(device)

    def _handle_disconnect_devices(self, params):
        """Disconnect specific devices"""
        device_ids = params.get('devices', [])
        for device_id in device_ids:
            device = self.device_manager.get_device_instance(device_id)
            if device:
                self._disconnect_device(device)

    def _handle_reconnect_devices(self, params):
        """Reconnect specific devices"""
        self._handle_connect_devices(params)

    def _handle_start_publishing(self, params):
        """Start publishing for specific devices"""
        device_ids = params.get('devices', [])
        for device_id in device_ids:
            device = self.device_manager.get_device_instance(device_id)
            if device and isinstance(device.device_definition, PublisherDefinition):
                device.is_publishing = True
                elapsed_ms = self.event_scheduler.get_elapsed_ms()
                device.last_publish_time_ms = elapsed_ms
                console_log(ConsoleLogLevel.DEBUG, f"Started publishing for {device_id}", __name__)

    def _handle_stop_publishing(self, params):
        """Stop publishing for specific devices"""
        device_ids = params.get('devices', [])
        for device_id in device_ids:
            device = self.device_manager.get_device_instance(device_id)
            if device:
                device.is_publishing = False
                console_log(ConsoleLogLevel.DEBUG, f"Stopped publishing for {device_id}", __name__)

    def _handle_change_purpose(self, params):
        """Change purpose for a specific device"""
        device_id = params.get('device')
        new_purpose = params.get('new_purpose')

        device = self.device_manager.get_device_instance(device_id)
        if device and isinstance(device.device_definition, PublisherDefinition):
            old_purpose = device.current_purpose
            device.current_purpose = new_purpose
            console_log(ConsoleLogLevel.DEBUG, f"Changed purpose for {device_id}: {old_purpose} -> {new_purpose}", __name__)

            # Re-register with broker if needed (for PM methods 3 and 4)
            device_def = device.device_definition
            GlobalDefs.CLIENT_MODULE.register_publish_purpose_for_topic(
                device.mqtt_client, self.method, device_def.topic, new_purpose
            )

    def _connect_device(self, device: DeviceInstance):
        """Connect a single device"""
        if device.is_connected:
            return

        result_code = GlobalDefs.CLIENT_MODULE.connect_client(
            device.mqtt_client, self.broker_address, self.broker_port
        )

        if result_code == 0:  # Success
            device.mqtt_client.loop_start()
            console_log(ConsoleLogLevel.DEBUG, f"Connecting device: {device.instance_id}", __name__)
        else:
            raise RuntimeError(f"Failed to connect device {device.instance_id}")

    def _disconnect_device(self, device: DeviceInstance):
        """Disconnect a single device"""
        if not device.is_connected:
            return

        GlobalDefs.CLIENT_MODULE.disconnect_client(device.mqtt_client)
        device.is_connected = False
        console_log(ConsoleLogLevel.INFO, f"Disconnected device: {device.instance_id}", __name__)

    def _disconnect_all_devices(self):
        """Disconnect all devices"""
        for device in self.device_manager.get_all_instances():
            self._disconnect_device(device)

    ###################################     
    #   CALLBACK FUNCTIONS
    ###################################
    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: mqtt.ConnectFlags, reason_code: ReasonCode, properties: Properties):
        """Callback for device connection"""
        if reason_code == 0:  # Success
            device_instance: DeviceInstance = userdata
            device_instance.is_connected = True

            GlobalDefs.LOGGING_MODULE.log_connect(time.time(), self.my_id, device_instance.mqtt_client_name)

            # Subscribe if this is a subscriber
            if isinstance(device_instance.device_definition, SubscriberDefinition):
                self._subscribe_device(device_instance)

            # Register publisher if needed
            if isinstance(device_instance.device_definition, PublisherDefinition):
                device_def = device_instance.device_definition
                GlobalDefs.CLIENT_MODULE.register_publish_purpose_for_topic(
                    device_instance.mqtt_client, self.method,
                    device_def.topic, device_instance.current_purpose
                )
                
            # TODO, add operations

    def _on_disconnect(self, client: mqtt.Client, userdata: Any, flags: mqtt.DisconnectFlags, reason_code: ReasonCode, properties: Properties):
        """Callback for device disconnection"""
        if reason_code == 0:  # Success
            device_instance: DeviceInstance = userdata
            device_instance.is_connected = False

            GlobalDefs.LOGGING_MODULE.log_disconnect(time.time(), self.my_id, device_instance.mqtt_client_name)

    def _subscribe_device(self, device: DeviceInstance):
        """Subscribe a device to its configured topics"""
        if not isinstance(device.device_definition, SubscriberDefinition):
            return

        device_def = device.device_definition

        self.subscribe_lock.acquire()

        results = GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(
            device.mqtt_client, self.method,
            device_def.topic_filter, device_def.purpose_filter,
            self.current_config.qos
        )

        now = time.time()

        for result_code, mid, sub_id in results:
            if result_code == 0:
                if device.mqtt_client_name not in self.pending_subscribes:
                    self.pending_subscribes[device.mqtt_client_name] = {}
                self.pending_subscribes[device.mqtt_client_name][mid] = (
                    device_def.topic_filter, device_def.purpose_filter, sub_id, now
                )
                device.subscribed_topics[device_def.topic_filter] = device_def.purpose_filter

        self.subscribe_lock.release()

    def _on_subscribe(self, client: mqtt.Client, userdata: Any, mid: Any, reason_code_list: List[ReasonCode], properties : Properties):
        
        # Make sure we're not in a subscribe
        self.subscribe_lock.acquire()
        
        # We can release immediately since this is a callback
        # We only had to block until the publications which will 
        # trigger this function had their information set
        self.subscribe_lock.release()
        
        device_instance: DeviceInstance = userdata
        
        # Check if message exists and was successful
        if device_instance.mqtt_client_name in self.pending_subscribes:
            if mid in self.pending_subscribes[device_instance.mqtt_client_name]: 
                    
                # We only do one subscribe per packet so this is always len one and is success for QoS 0/1/2
                if reason_code_list[0] == 0 or reason_code_list[0] == 1 or reason_code_list[0] == 2:
                    topic_filter, purpose_filter, sub_id, time = self.pending_subscribes[device_instance.mqtt_client_name][mid]
                    GlobalDefs.LOGGING_MODULE.log_subscribe(time, self.my_id, device_instance.mqtt_client_name, topic_filter, purpose_filter, sub_id)
        
    
    def _on_publish(self, client: mqtt.Client, userdata: Any, mid:int, reason_code: ReasonCode, properties: Properties):    
        
        # Make sure we're not in a publish
        self.publish_lock.acquire()
        
        # We can release immediately since this is a callback
        # We only had to block until the publications which will 
        # trigger this function had their information set
        self.publish_lock.release()
        
        device_instance: DeviceInstance = userdata
        
        # Check if message exists and was successful
        if device_instance.mqtt_client_name in self.pending_publishes:
            if mid in self.pending_publishes[device_instance.mqtt_client_name]: 
                
                corr_data = device_instance.message_id_to_send_counter[mid]
                    
                # If successful
                if reason_code == 0:
                    topic, purpose, op_type, time = self.pending_publishes[device_instance.mqtt_client_name][mid]
                    
                    # Do not log communications to the broker
                    # if not topic[0] == '$':
                    
                    # Check if operational or data
                    if op_type == "DATA":
                        # Log message
                        GlobalDefs.LOGGING_MODULE.log_publish(time, self.my_id, device_instance.mqtt_client_name, corr_data, topic, purpose, op_type)
                    else:
                        # Log message
                        GlobalDefs.LOGGING_MODULE.log_operation_publish(time, self.my_id, device_instance.mqtt_client_name, corr_data, topic, purpose, op_type, self.current_config.possible_operations[op_type])
                
                
    def _on_message_recv(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage):
        
        operational_message = False
        operation_type = ""
        sending_client = "UNKNOWN"
        op_message_type = "OP"
        correlation_data = -1
        sub_id: List[int] = list()
        device_instance: DeviceInstance = userdata
        
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
            GlobalDefs.LOGGING_MODULE.log_operation_recv(time.time(), self.my_id, device_instance.mqtt_client_name, sending_client, correlation_data, message.topic, operation_type, self.current_config.possible_operations[operation_type], op_message_type, sub_id[0])
        else:
            GlobalDefs.LOGGING_MODULE.log_recv(time.time(), self.my_id, device_instance.mqtt_client_name, sending_client, correlation_data, message.topic, "DATA", sub_id[0])