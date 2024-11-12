import time
import paho.mqtt.client as mqtt

# Broker configuration
BROKER_ADDRESS = 'localhost'  # Change 
BROKER_PORT = 1883

# Topic
TOPIC = 'data/location'

# Message Purpose Filter (PF-MP)
PF_MP = 'ads/{third-party,targeted}'

# Publishing interval in seconds
PUBLISH_INTERVAL = 5  # Adjust as needed

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[Publisher] Connected successfully to broker.")
    else:
        print(f"[Publisher] Connection failed with code {rc}")

def publisher():
    # Initialize MQTT client with MQTTv5 protocol
    client = mqtt.Client(client_id='publisher', protocol=mqtt.MQTTv5)
    client.on_connect = on_connect

    try:
        # Connect to the MQTT broker
        client.connect(BROKER_ADDRESS, BROKER_PORT)
    except Exception as e:
        print(f"[Publisher] Failed to connect to broker: {e}")
        return

    # Start the network loop in a separate thread
    client.loop_start()

    try:
        while True:
            # Define user properties for the message
            properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
            properties.UserProperty = [("PF-MP", PF_MP)]

            # Define the payload
            payload = "Location data payload"

            # Publish the message
            result = client.publish(TOPIC, payload, qos=1, properties=properties)

            # Check if the publish was successful
            status = result[0]
            if status == mqtt.MQTT_ERR_SUCCESS:
                print(f"[Publisher] Published message with PF-MP '{PF_MP}' to topic '{TOPIC}'")
            else:
                print(f"[Publisher] Failed to publish message to topic '{TOPIC}'")

            # Wait for the next publish cycle
            time.sleep(PUBLISH_INTERVAL)
    except KeyboardInterrupt:
        print("\n[Publisher] Interrupted by user. Exiting...")
    finally:
        # Stop the network loop and disconnect
        client.loop_stop()
        client.disconnect()
        print("[Publisher] Disconnected from broker.")

if __name__ == "__main__":
    publisher()
