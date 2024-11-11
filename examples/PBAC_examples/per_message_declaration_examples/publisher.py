import time
import paho.mqtt.client as mqtt

# Broker configuration
BROKER_ADDRESS = 'localhost'
BROKER_PORT = 1883

# Topic
TOPIC = 'data/location'

# Message Purpose Filter (PF-MP)
PF_MP = 'ads/{third-party,targeted}'  

def publisher():
    def on_connect(client, userdata, flags, rc, properties=None):
        print("[Publisher] Connected with result code {}".format(rc))

        # Publish a single message with the PF-MP purpose filter
        properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
        properties.UserProperty = [("PF-MP", PF_MP)]
        payload = "Location data payload"
        client.publish(TOPIC, payload, qos=1, properties=properties)
        print("[Publisher] Published message with PF-MP '{}' to topic '{}'".format(PF_MP, TOPIC))

    client = mqtt.Client(client_id='publisher', protocol=mqtt.MQTTv5)
    client.on_connect = on_connect

    client.connect(BROKER_ADDRESS, BROKER_PORT)
    client.loop_start()

    # Wait for message to be sent
    time.sleep(2)
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    publisher()
