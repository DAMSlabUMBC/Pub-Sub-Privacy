import time
import paho.mqtt.client as mqtt

# Broker configuration
BROKER_ADDRESS = 'localhost' # Change
BROKER_PORT = 1883

# Topic
TOPIC = 'data/location'

# Message Purpose Filter (PF-MP)
PF_MP = 'ads/{third-party,targeted}'  

# Registration Topic
REGISTRATION_TOPIC = '$priv/purpose_management'

def publisher():
    def on_connect(client, userdata, flags, rc, properties=None):
        print(f"[Publisher] Connected with result code {rc}")

        # Register PF-MP for the topic with the purpose filter
        properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
        properties.UserProperty = [(f"PF-MP:{TOPIC}", PF_MP)]
        client.publish(REGISTRATION_TOPIC, payload='', qos=1, properties=properties)
        print(f"[Publisher] Registered PF-MP '{PF_MP}' for topic '{TOPIC}'")

        # Publish a message after registering PF-MP
        time.sleep(1)  # Give time for registration to process
        payload = "Location data payload"
        client.publish(TOPIC, payload, qos=1)
        print(f"[Publisher] Published message to topic '{TOPIC}'")

    client = mqtt.Client(client_id='publisher', protocol=mqtt.MQTTv5)
    client.on_connect = on_connect

    client.connect(BROKER_ADDRESS, BROKER_PORT)
    client.loop_start()

    # Wait for message to be sent
    time.sleep(3)
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    publisher()
