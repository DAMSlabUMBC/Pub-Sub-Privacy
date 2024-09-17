// deletion_request.c

#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include <stdio.h>
#include <string.h>

#define DELETION_REQUEST "delete/request"

// Plugin version function
int mosquitto_plugin_version(int supported_version_count, const int *supported_versions) {
    for(int i = 0; i < supported_version_count; i++) {
        if(supported_versions[i] == 5){
            return 5;  // Specify the version of the plugin API being used
        }
    }
    return -1;  // Version not supported
}

// Callback for handling incoming messages
static int on_message(int event, void *event_data, void *userdata) {
    struct mosquitto_evt_message *msg_event = (struct mosquitto_evt_message *)event_data;

    // Check if the message payload contains a deletion request
    if (strstr(msg_event->payload, DELETION_REQUEST) != NULL) {
        char *topic = msg_event->topic;

        // Prepare deletion request payload
        const char *deletion_payload = "Please delete data for the topic.";

        // Send the deletion request to all clients subscribed to the topic
        int result = mosquitto_broker_publish(
            NULL,  // NULL means publish to all clients
            topic,
            strlen(deletion_payload),
            (void *)deletion_payload,
            0,
            false,
            NULL
        );

        if (result == MOSQ_ERR_SUCCESS) {
            printf("Deletion request sent on topic '%s'.\n", topic);
        } else {
            printf("Failed to send deletion request on topic '%s'. Error: %d\n", topic, result);
        }
    }

    return MOSQ_ERR_SUCCESS;
}

// Plugin initialization
int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **userdata, struct mosquitto_opt *opts, int opt_count) {
    mosquitto_callback_register(identifier, MOSQ_EVT_MESSAGE, on_message, NULL, NULL);
    return MOSQ_ERR_SUCCESS;
}

// Plugin cleanup
int mosquitto_plugin_cleanup(void *userdata, struct mosquitto_opt *opts, int opt_count) {
    mosquitto_callback_unregister(userdata, MOSQ_EVT_MESSAGE, on_message, NULL);
    return MOSQ_ERR_SUCCESS;
}
