import paho.mqtt.client as mqtt
from paho.mqtt.enums import MQTTProtocolVersion, CallbackAPIVersion
from typing import Callable, Optional, Tuple, List
import GlobalDefs

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
def subscribe_with_purpose_filter(client: mqtt.Client, method: GlobalDefs.PurposeManagementMethod, 
                                  callback: Callable, topic_filter: str, purpose_filter: str, 
                                  qos: int = 0) -> Tuple[mqtt.MQTTErrorCode, Optional[int]]:
    
    if purpose_filter == None:
        purpose_filter = GlobalDefs.ALL_PURPOSE_FILTER
        
    client.on_message = callback  # Set the callback for incoming messages
        
    # == Method 0 ==
    if method == GlobalDefs.PurposeManagementMethod.PM_0:
        
        # Under method 0, purposes are encoded as topics
        described_purposes = GlobalDefs.find_described_purposes(purpose_filter)

        # Convert the purpose list into topics
        topic_list = list()
        for purpose in described_purposes:
            purpose = purpose.replace('/', '|')
            purpose_subtopic = f'[{purpose}]'
            full_topic = f'{topic_filter}/{purpose_subtopic}'
            topic_list.append(full_topic)

        # Subscribe to each topic
        for topic in topic_list:
            result, mid = client.subscribe(topic, qos=qos)
            if result is not mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS:
                return result, mid
            
        return result, mid
    
    # == Method 1 and 2 ==
    elif method == GlobalDefs.PurposeManagementMethod.PM_1 or method == GlobalDefs.PurposeManagementMethod.PM_2:
        
        # Purpose filter is supplied on subscription
        properties = mqtt.Properties(packetType=mqtt.PacketTypes.SUBSCRIBE)
        properties.UserProperty = (GlobalDefs.PROPERTY_SP, purpose_filter)
        
        try:
            result, mid = client.subscribe(topic_filter, qos=qos, properties=properties)
            return result, mid  # Return error code and message ID
        except Exception:
            return mqtt.MQTTErrorCode.MQTT_ERR_UNKNOWN, None
        
        finally:
            return mqtt.MQTTErrorCode.MQTT_ERR_UNKNOWN, None

    # == Method 3 ==
    elif method == GlobalDefs.PurposeManagementMethod.PM_3:
        
        # Perform a normal subscribe first
        try:
            result, mid = client.subscribe(topic_filter, qos=qos)
        except Exception as e:
            return mqtt.MQTTErrorCode.MQTT_ERR_UNKNOWN, None
        
        # If it didn't fail, send registration for subscription purpose
        sp_reg_topic = f"{GlobalDefs.REG_BY_TOPIC_SUB_REG_TOPIC}/{topic_filter}[{purpose_filter}]"
        
        # If topic contains a wildcard, it needs to be replaced
        sp_reg_topic = sp_reg_topic.replace("#", "HASH").replace("+","PLUS")
        
        client.publish(sp_reg_topic, qos=qos)
        
        return result, mid  # Return error code and message ID
        
    # Not able to subscribe if method is invalid
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
def register_publish_purpose_for_topic(client: mqtt.Client, method: GlobalDefs.PurposeManagementMethod, 
                                       topic: str, purpose: str, qos: int = 0) -> mqtt.MQTTMessageInfo | None:
    
    # Not required for methods 0/1
    # == Method 2 == #
    if method == GlobalDefs.PurposeManagementMethod.PM_2:
        
        # Send registration containing desired MP to the registration topic
        property_value = f"{purpose}:{topic}"
        properties = mqtt.Properties(packetType=mqtt.PacketTypes.SUBSCRIBE)
        properties.UserProperty = (GlobalDefs.PROPERTY_MP, property_value)
        print(f"Regging {property_value}")
        return client.publish(GlobalDefs.REG_BY_MSG_REG_TOPIC, qos=qos, properties=properties)
    
    # == Method 3 ==
    elif method == GlobalDefs.PurposeManagementMethod.PM_3:
        
        # Send registration to custom registration topic
        mp_reg_topic = f"{GlobalDefs.REG_BY_TOPIC_PUB_REG_TOPIC}/{topic}[{purpose}]"
        return client.publish(mp_reg_topic, qos=qos)
        
    # Not required if method is invalid
    return None

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
def publish_with_purpose(client: mqtt.Client, method: GlobalDefs.PurposeManagementMethod, 
                         topic: str, purpose: Optional[str] = None, qos: int = 0, 
                         retain: bool = False, payload: str | None = None) -> List[Tuple[mqtt.MQTTMessageInfo, str]]:
    
    
    if purpose == None:
        purpose = GlobalDefs.ALL_PURPOSE_FILTER
        
    ret_list = list()
    
    # == Method 0 ==
    if method == GlobalDefs.PurposeManagementMethod.PM_0:
        
        # Need to send message to each purpose topic
        described_purposes = GlobalDefs.find_described_purposes(purpose)

        # Convert the purpose list into topics
        topic_list = list()
        for purpose in described_purposes:
            purpose = purpose.replace('/', '|')
            purpose_subtopic = f'[{purpose}]'
            full_topic = f'{topic}/{purpose_subtopic}'
            topic_list.append(full_topic)

        # Publish to each topic
        for curr_topic in topic_list:
            result = client.publish(curr_topic, qos=qos, retain=retain, payload=payload)
            ret_list.append((result, curr_topic))
    
    # == Method 1 ==
    elif method == GlobalDefs.PurposeManagementMethod.PM_1:
        
        # Publish with required MP as a property
        properties = mqtt.Properties(packetType=mqtt.PacketTypes.PUBLISH)
        properties.UserProperty = (GlobalDefs.PROPERTY_MP, purpose)
        
        msg_info = client.publish(topic, payload, qos=qos, retain=retain, properties=properties)
        return [(msg_info, topic)]  # Return list of (message info, topic) tuples
    
    # == Methods 2 and 3 == #
    elif method == GlobalDefs.PurposeManagementMethod.PM_2 or method == GlobalDefs.PurposeManagementMethod.PM_3:
        
        # This is just a normal publish
        msg_info = client.publish(topic, payload, qos=qos, retain=retain)
        return [(msg_info, topic)]  # Return list of (message info, topic) tuples
        
    # Not required if method is invalid
    return ret_list
