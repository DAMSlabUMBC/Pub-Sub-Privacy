import paho.mqtt.client as mqtt

# Broker configuration
BROKER_ADDRESS = 'localhost'
BROKER_PORT = 1883

# Topic
TOPIC = 'data/location'

# Subscription Purpose (PF-SP)
PF_SP = 'ads'

def subscriber():
    def on_connect(client, userdata, flags, rc, properties=None):
        print("[Subscriber] Connected with result code {}".format(rc))

        # Register PF-SP for the topic
        properties = mqtt.Properties(mqtt.PacketTypes.SUBSCRIBE)
        properties.UserProperty = [("PF-SP", PF_SP)]
        client.subscribe(TOPIC, qos=1, options=None, properties=properties)
        print("[Subscriber] Registered PF-SP '{}' for topic '{}'".format(PF_SP, TOPIC))

    def on_message(client, userdata, message):
        print("\n[Subscriber] Received message:")
        print("Topic: {}".format(message.topic))
        print("Payload: {}".format(message.payload.decode()))
        print("No User Properties in message (PF-MP registered by message).")

    client = mqtt.Client(client_id='subscriber', protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER_ADDRESS, BROKER_PORT)
    client.loop_forever()

if __name__ == "__main__":
    subscriber()
