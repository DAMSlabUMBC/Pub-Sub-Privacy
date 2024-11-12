import paho.mqtt.client as mqtt

# Broker configuration
BROKER_ADDRESS = 'localhost'  # Change 
BROKER_PORT = 1883

# Topic
TOPIC = 'data/location'

# Subscription Purpose (PF-SP)
PF_SP = 'ads'

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[Subscriber] Connected successfully to broker.")
        # Define user properties for the subscription
        properties = mqtt.Properties(mqtt.PacketTypes.SUBSCRIBE)
        properties.UserProperty = [("PF-SP", PF_SP)]
        
        # Subscribe to the topic with the specified user property
        client.subscribe(TOPIC, qos=1, properties=properties)
        print(f"[Subscriber] Subscribed to topic '{TOPIC}' with PF-SP '{PF_SP}'")
    else:
        print(f"[Subscriber] Connection failed with code {rc}")

def on_message(client, userdata, message):
    print("\n[Subscriber] Received message:")
    print(f"Topic: {message.topic}")
    print(f"Payload: {message.payload.decode()}")
    
    # Check for user properties in the message
    if message.properties and message.properties.UserProperty:
        print("User Properties:")
        for prop in message.properties.UserProperty:
            print(f" - {prop[0]}: {prop[1]}")
    else:
        print("No User Properties.")

def subscriber():
    # Initialize MQTT client with MQTTv5 protocol
    client = mqtt.Client(client_id='subscriber', protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        # Connect to the MQTT broker
        client.connect(BROKER_ADDRESS, BROKER_PORT)
    except Exception as e:
        print(f"[Subscriber] Failed to connect to broker: {e}")
        return

    # Start the network loop and run indefinitely
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[Subscriber] Interrupted by user. Exiting...")
    finally:
        # Disconnect from the broker
        client.disconnect()
        print("[Subscriber] Disconnected from broker.")

if __name__ == "__main__":
    subscriber()
