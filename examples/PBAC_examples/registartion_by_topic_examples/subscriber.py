import paho.mqtt.client as mqtt

# Broker configuration
BROKER_ADDRESS = 'localhost'
BROKER_PORT = 1883

# Topic
TOPIC = 'data/location'

# Subscription Purpose (PF-SP)
PF_SP = 'ads'

def format_purpose_for_topic(purpose):
    return purpose.replace('/', '|')

def subscriber():
    def on_connect(client, userdata, flags, rc, properties=None):
        print("[Subscriber] Connected with result code {}".format(rc))

        # Register PF-SP for the topic via registration topic
        formatted_purpose = format_purpose_for_topic(PF_SP)
        registration_topic = "$priv/SP_registration/{}/[{}]".format(TOPIC, formatted_purpose)
        client.publish(registration_topic, payload='', qos=1)
        print("[Subscriber] Registered PF-SP '{}' for topic '{}' via topic '{}'".format(PF_SP, TOPIC, registration_topic))

        # Subscribe to the topic
        client.subscribe(TOPIC, qos=1)
        print("[Subscriber] Subscribed to topic '{}'".format(TOPIC))

    def on_message(client, userdata, message):
        print("\n[Subscriber] Received message:")
        print("Topic: {}".format(message.topic))
        print("Payload: {}".format(message.payload.decode()))
        print("No User Properties in message (PF-MP registered by topic).")

    client = mqtt.Client(client_id='subscriber', protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER_ADDRESS, BROKER_PORT)
    client.loop_forever()

if __name__ == "__main__":
    subscriber()
