import paho.mqtt.client as mqtt
from paho.mqtt.enums import MQTTProtocolVersion, CallbackAPIVersion
from typing import Callable, Optional, Tuple, List
from GlobalDefs import PurposeManagementMethod, C1RightsMethod, C2RightsMethod, C3RightsMethod

"""Initializes a Paho MQTTv5 client and returns it to the requester

Parameters
----------
client_id : str
    The client ID to assign to the client

Returns
----------
paho.mqtt.client.Client
    The created client
"""
def create_v5_client(client_id: str) -> mqtt.Client:

    # Instantiate client
    mqtt_client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=MQTTProtocolVersion.MQTTv5,
        reconnect_on_failure=False
    )

    return mqtt_client

"""Attempts to connect the client to a broker. Note that this function should ONLY
send a connect message and should not attempt to start the network loop, this is
handled by the benchmark.

Parameters
----------
client : paho.mqtt.client.Client
    The client to connect
success_callback : function
    A function to call on connect success
failure_callback : function
    A function to call on connect failure
broker_address : str
    The IP or FQDN of the broker
port : int, optional
    The port on which to connect to the broker (default is 1883)
clean_start : bool, optional
    Whether to clear existing session information for the client

Returns
----------
paho.mqtt.client.MQTTErrorCode
    The return code of the connect attempt
"""
def connect_client(client: mqtt.Client, broker_address: str, port: int = 1883, 
                  success_callback: Optional[Callable] = None, 
                  failure_callback: Optional[Callable] = None, 
                  clean_start: bool = True) -> mqtt.MQTTErrorCode:
        
    # We don't need to do any additional processing before using the callbacks, so we can set those directly
    if success_callback is not None:
        client.on_connect = success_callback
    if failure_callback is not None:
        client.on_connect_fail = failure_callback

    # Attempt to send connect packet
    try:
        return client.connect(host=broker_address, port=port, clean_start=clean_start)
    except Exception as e:
        return mqtt.MQTTErrorCode.MQTT_ERR_UNKNOWN

"""Attempts to SUBSCRIBE client to a topic filter in MQTT with a specified purpose filter

Parameters
----------
client : paho.mqtt.client.Client
    The client to subscribe with
method : Benchmark.PurposeManagementMethod
    The method of purpose management for the broker
callback : function
    A function to call on SUBACK
topic_filter : str
    The topic filter on which to subscribe
purpose_filter : str
    The purpose filter for which the messages will be used
subscriber_id : int, optional
    The subscriber id to assign to the subscription
qos : int, optional
    The quality of service for the subscription

Returns
----------
tuple[paho.mqtt.client.MQTTErrorCode, int | None]
    A tuple containing the error code and (if successful) the granted quality of service for the subscription
"""
def subscribe_with_purpose_filter(client: mqtt.Client, method: PurposeManagementMethod, 
                                  callback: Callable, topic_filter: str, purpose_filter: str, 
                                  subscriber_id: Optional[str] = None, qos: int = 0) -> Tuple[mqtt.MQTTErrorCode, Optional[int]]:
    
    if purpose_filter == None:
        purpose_filter = "*"

    properties = mqtt.Properties(packetType=mqtt.PacketTypes.SUBSCRIBE)
    properties.UserProperty = ('PF-SP', purpose_filter)
    
    client.on_message = callback  # Set the callback for incoming messages
    try:
        result, mid = client.subscribe(topic_filter, qos=qos, properties=properties)
        return result, mid  # Return error code and message ID
    except Exception:
        return mqtt.MQTTErrorCode.MQTT_ERR_UNKNOWN, None

"""Attempts to register a purpose filter for publications to a topic (Used only for PM_2 and PM_3)

Parameters
----------
client : paho.mqtt.client.Client
    The client to register
method : Benchmark.PurposeManagementMethod
    The method of purpose management for the broker
topic : str
    The topic on which to set the filter
purpose : str
    The purpose filter to register
qos : int, optional
    The quality of service for the message

Returns
----------
paho.mqtt.client.MQTTErrorCode
    The message publication information
"""
def register_publish_purpose_for_topic(client: mqtt.Client, method: PurposeManagementMethod, 
                                       topic: str, purpose: str, qos: int = 0) -> mqtt.MQTTMessageInfo:
    return  # Placeholder: Not implemented yet as not required for basic sync

"""Attempts to PUBLISH a message a topic in MQTT with a specified purpose filter

Parameters
----------
client : paho.mqtt.client.Client
    The client to publish with
method : Benchmark.PurposeManagementMethod
    The method of purpose management for the broker
topic : str
    The topic on which to publish
purpose : str, optional (for some methods)
    The purpose on which to send
qos : int, optional
    The quality of service for the message
payload : str, optional
    The payload to send within the message

Returns
----------
list[tuple[paho.mqtt.client.MQTTErrorCode, str]]
    A list of tuples which contain the error code of the message publication and the topic for the error code
    (As method PM_1, a single publication request may need to be sent to multiple topics)
"""
def publish_with_purpose(client: mqtt.Client, method: PurposeManagementMethod, 
                         topic: str, purpose: Optional[str] = None, qos: int = 0, 
                         retain: bool = False, payload: str = "") -> List[Tuple[mqtt.MQTTMessageInfo, str]]:
    try:
        if purpose == None:
            purpose = "*"

        properties = mqtt.Properties(packetType=mqtt.PacketTypes.PUBLISH)
        properties.UserProperty = ('PF-MP', purpose)

        msg_info = client.publish(topic, payload, qos=qos, retain=retain, properties=properties)
        return [(msg_info, topic)]  # Return list of (message info, topic) tuples
    except Exception:
        return [(mqtt.MQTTMessageInfo(mqtt.MQTTErrorCode.MQTT_ERR_UNKNOWN), topic)]
