import logging
import itertools
from enum import Enum
import paho.mqtt.client as mqtt
from paho.mqtt.enums import MQTTProtocolVersion, CallbackAPIVersion

# Define enums for easier referencing later
class PurposeManagementMethod(Enum):
    PM_1 = "Purpose-Enconding Topics"
    PM_2 = "Per-Message Declaration"
    PM_3 = "Registration by Message"
    PM_4 = "Registration by Topic"

class C1RightsMethod(Enum):
    C1_1 = "Direct Publication"
    C1_2 = "Pre-Registration"

class C2RightsMethod(Enum):
    C2_1 = "Direct Publication"
    C2_2 = "Broker-Facilitatied"

class C3RightsMethod(Enum):
    C3_1 = "Direct Publication"
    C4_2 = "Broker-Facilitatied"

class BenchmarkClient:
    client_id: str
    mqtt_client: mqtt.Client
    logger: logging.Logger
 
    # Connect properties
    connect_waiting: bool = False

    # Test information
    class CorrectnessTestResults:
        client_id: str
        test_name: str
        success_count: int
        failure_count: int
        total_count: int
        failure_reasons: list[str]

        def __init__(self, client_id: str, test_name: str) -> None:
            self.client_id = client_id
            self.test_name = test_name
            self.success_count = 0
            self.failure_count = 0
            self.total_count = 0
            self.failure_reasons = list()

        def success(self) -> None:
            self.success_count += 1
            self.total_count += 1

        def failure(self, reason: str) -> None:
            self.failure_count += 1
            self.total_count += 1
            self.failure_reasons.append(reason)

        def get_results(self) -> str:
            result_str = f'Results for Client: {self.client_id} - Test: {self.test_name}\n'
            result_str += f'> Successes: {self.success_count} - Failures: {self.failure_count} - Total: {self.total_count}\n'
            for reason in self.failure_reasons:
                result_str += f'{reason}\n'
            return result_str

    current_correcness_test_results: CorrectnessTestResults

    ## CONSTRUCTOR ##

    def __init__(self, client_id: str, mqtt_version: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv5) -> None:
        """Initializes a wrapper for the Paho MQTT clients

        Parameters
        ----------
        client_id : str
            The client ID to assign to the client
        mqtt_version : MQTTProtocolVersion, optional
            The version of MQTT the clients should utilize (default is MQTTv5)
        """
        
        self.client_id = client_id

        # Instantiate client
        self.mqtt_client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt_version,
            reconnect_on_failure=False
        )
        
        # Create a logger for the client
        self.logger = logging.getLogger(client_id)
        self.mqtt_client.enable_logger(self.logger)

    ## CONNECTION METHODS ##

    def connect(self, broker_address: str, port: int = 1883, clean_start: bool = True) -> None:
        """Attempts to connect the client to a broker

        Parameters
        ----------
        broker_address : str
            The IP or FQDN of the broker
        port : int, optional
            The port on which to connect to the broker (default is 1883)
        timeout : int, optional
            The length of time to wait for the client to connect (default is paho default)
        clean_start : bool, optional
            Whether to clear existing session information for the client
        """

        # Send a connect message and start processing loop
        self.mqtt_client.on_connect = self._connect_callback
        self.mqtt_client.on_connect_fail = self._connect_fail_callback

        # Attempt to connect
        try:
            self.connect_waiting = True
            self.mqtt_client.connect(host=broker_address, port=port, clean_start=clean_start)
            self.mqtt_client.loop_start()
        except Exception as e:
            self.logger.error(f"{self.client_id} failed to connect to broker {broker_address}:{port}.")
            self.connect_waiting = False
            self.mqtt_client.loop_stop()
            

    def is_connected(self):
        if self.connect_waiting:
            return False
        else:
            return self.mqtt_client.is_connected()
        

    ## Callbacks
    def _connect_callback(self, client, userdata, connect_flags, reason_code, properties):
        self.connect_waiting = False

    def _connect_fail_callback(self, client, userdata):
        self.connect_waiting = False
        self.logger.debug(msg=f"{self.client_id} - failed to connect to broker")  


    ## PUBLISH METHODS ##

    def publish_with_purpose(self, topic: str, purpose: str, msg: str, method: PurposeManagementMethod) -> None:
        """Publishes an MQTT message with a defined purpose

        Parameters
        ----------
        topic : str
            The MQTT Topic
        purpose : str
            The defined purpose
        msg : str
            The MQTT payload
        method : PurposeManagementMethod
            The method to use for publishing the purpose
        """

        if method == PurposeManagementMethod.PM_1:
            # Under method 1, purpose is encoded as a topic
            purpose = purpose.replace('/', '|')
            purpose_subtopic = f'[{purpose}]'
            full_topic = f'{topic}/{purpose_subtopic}'
            self.mqtt_client.publish(topic=full_topic, payload=msg)

        elif method == PurposeManagementMethod.PM_2:
            raise NotImplementedError
        elif method == PurposeManagementMethod.PM_3:
            raise NotImplementedError
        elif method == PurposeManagementMethod.PM_4:
            raise NotImplementedError
        else:
            raise ValueError(f"Unknown method {method}")
        

    ## SUBSCRIBE METHODS ##
    def subscribe_with_purpose(self, topic: str, purpose_filter: str, method: PurposeManagementMethod) -> None:

        if method == PurposeManagementMethod.PM_1:
            # Under method 1, purposes are encoded as topics
            described_purposes = self.find_described_purposes(purpose_filter)
    
            # Convert the purpose list into topics
            topic_list = list()
            for purpose in described_purposes:
                purpose = purpose.replace('/', '|')
                purpose_subtopic = f'[{purpose}]'
                full_topic = f'{topic}/{purpose_subtopic}'
                topic_list.append((full_topic, 0))

            # Subscribe to each topic
            for topic in topic_list:
                self.mqtt_client.subscribe(topic=topic_list)

        elif method == PurposeManagementMethod.PM_2:
            raise NotImplementedError
        elif method == PurposeManagementMethod.PM_3:
            raise NotImplementedError
        elif method == PurposeManagementMethod.PM_4:
            raise NotImplementedError
        else:
            raise ValueError(f"Unknown method {method}")
        
    # Callbacks
    def purpose_management_correctness_message_callback(self, client, userdata, message):
        if message.payload.decode() == "GOOD":
            self.current_correcness_test_results.success()
        else:
            self.current_correcness_test_results.failure(f'Recieved message for non-approved purpose on {message.topic}')
    

    ## TEST METHODS ##
    def initialize_test(self, test_name: str) -> None:
        self.current_correcness_test_results = self.CorrectnessTestResults(self.client_id, test_name)

    ## UTILITY METHODS ##
        
    @staticmethod
    def find_described_purposes(purpose_filter: str) -> list[str]:

        # Break purpose filter into individual purposes
        filter_levels = purpose_filter.split('/')
        decomposed_levels = list()

        for level in filter_levels:
            if '{' in level:
                level = level.replace('{','').replace('}','').split(',')
            else:
                level = [level]

            decomposed_levels.append(level)

        described_purposes = list()
        decomposed_purpose_list = itertools.product(*decomposed_levels)
        for purpose_list in decomposed_purpose_list:
            purpose = '/'.join(purpose_list)
            if not './' in purpose:
                purpose = purpose.replace('/.', '')
                described_purposes.append(purpose)

        return described_purposes