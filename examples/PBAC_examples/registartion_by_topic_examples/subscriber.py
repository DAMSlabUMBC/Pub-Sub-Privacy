import paho.mqtt.client as mqtt

# Broker configuration
BROKER_ADDRESS = 'localhost' # Change
BROKER_PORT = 1883

# Topic
TOPIC = 'data/location'

# Subscription Purpose (PF-SP)
PF_SP = 'ads'

def subscriber():
    def on_connect(client, userdata, flags, rc, properties=None):
        print("[Subscriber] Connected with result code {}".format(rc))

        # Register PF-SP for the topic via registration topic
        formatted_purpose = format_purpose_for_topic(PF_SP)
        registration_topic = f"$priv/SP_registration/{TOPIC}/[{PF_SP}]"
        client.publish(registration_topic, payload='', qos=1)
        print(F"[Subscriber] Registered PF-SP '{PF_SP}' for topic '{TOPIC}' via topic '{registration_topic}'")

        # Subscribe to the topic
        client.subscribe(TOPIC, qos=1)
        print(f"[Subscriber] Subscribed to topic '{TOPIC}'")

    def on_message(client, userdata, message):
        print("\n[Subscriber] Received message:")
        print(f"Topic: {message.topic}".format(message.topic))
        print(f"Payload: {message.payload.decode()}")
        print("No User Properties in message (PF-MP registered by topic).")

    client = mqtt.Client(client_id='subscriber', protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER_ADDRESS, BROKER_PORT)
    client.loop_forever()

if __name__ == "__main__":
    subscriber()
