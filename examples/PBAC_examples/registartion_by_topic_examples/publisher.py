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

        # Register PF-MP for the topic via registration topic
        registration_topic = "$priv/MP_registration/f{TOPIC}/[{PF_MP}]"
        client.publish(registration_topic, payload='', qos=1)
        print("[Publisher] Registered PF-MP {PF_MP} for topic {TOPIC} via topic {registration_topic}")
        # Publish a message after registering PF-MP
        time.sleep(1)  # Give time for registration to process
        payload = "Location data payload"
        client.publish(TOPIC, payload, qos=1)
        print("[Publisher] Published message to topic '{}'".format(TOPIC))

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
