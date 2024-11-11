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

        # Subscribe with PF-SP
        properties = mqtt.Properties(mqtt.PacketTypes.SUBSCRIBE)
        properties.UserProperty = [("PF-SP", PF_SP)]
        client.subscribe(TOPIC, qos=1, options=None, properties=properties)
        print("[Subscriber] Subscribed to topic '{}' with PF-SP '{}'".format(TOPIC, PF_SP))

    def on_message(client, userdata, message):
        print("\n[Subscriber] Received message:")
        print("Topic: {}".format(message.topic))
        print("Payload: {}".format(message.payload.decode()))
        if message.properties is not None and message.properties.UserProperty is not None:
            print("User Properties:")
            for prop in message.properties.UserProperty:
                print(" - {}: {}".format(prop[0], prop[1]))
        else:
            print("No User Properties.")

    client = mqtt.Client(client_id='subscriber', protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER_ADDRESS, BROKER_PORT)
    client.loop_forever()

if __name__ == "__main__":
    subscriber()
