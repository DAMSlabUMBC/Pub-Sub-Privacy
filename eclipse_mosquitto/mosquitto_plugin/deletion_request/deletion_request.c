// deletion_request.c

#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include <stdio.h>
#include <string.h>

#define DELETION_REQUEST_SUFFIX "/deletion_request"
#define MAX_TOPIC_LENGTH 256

// Utility function to trim whitespace
static void trim_whitespace(char *str) {
    char *end;
    // Trim leading space
    while(*str == ' ') str++;
    // Trim trailing space
    end = str + strlen(str) - 1;
    while(end > str && (*end == ' ' || *end == '\n')) end--;
    *(end+1) = '\0';
}

// Callback to handle deletion requests
static int on_message(int event, void *event_data, void *userdata) {
    struct mosquitto_evt_message *msg_event = (struct mosquitto_evt_message *)event_data;
    const void *payload = msg_event->payload;
    char base_topic[MAX_TOPIC_LENGTH];

    if (payload == NULL || msg_event->topic == NULL) {
        return MOSQ_ERR_SUCCESS;
    }

    // Check if the topic ends with /deletion_request
    const char *suffix = DELETION_REQUEST_SUFFIX;
    size_t suffix_len = strlen(suffix);
    size_t topic_len = strlen(msg_event->topic);

    if (topic_len <= suffix_len || strcmp(msg_event->topic + topic_len - suffix_len, suffix) != 0) {
        // Not a deletion_request message
        return MOSQ_ERR_SUCCESS;
    }

    // Extract the base_topic by removing the /deletion_request suffix
    strncpy(base_topic, msg_event->topic, topic_len - suffix_len);
    base_topic[topic_len - suffix_len] = '\0';
    trim_whitespace(base_topic);

    // Log the deletion request
    printf("Received deletion request on topic '%s'\n", msg_event->topic);

    // Define the payload for deletion
    const char *delete_payload = "delete_requested";

    // Publish "delete_requested" to the base_topic
    int rc = mosquitto_broker_publish(
        NULL,                   // Use broker's client ID
        base_topic,             // Publish to the base_topic
        strlen(delete_payload),
        (void *)delete_payload,
        1,                      // QoS level 1
        false,                  // No retain
        NULL                    // No properties (MQTT v5)
    );

    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "Failed to publish deletion request to '%s': %s\n",
                base_topic, mosquitto_strerror(rc));
    } else {
        printf("Published deletion request to '%s'\n", base_topic);
    }

    return MOSQ_ERR_SUCCESS;
}

// Plugin initialization
int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **userdata,
                          struct mosquitto_opt *opts, int opt_count) {
    *userdata = identifier; // Store identifier in userdata for cleanup

    // Register callback for message events
    mosquitto_callback_register(identifier, MOSQ_EVT_MESSAGE, on_message, NULL, NULL);

    printf("Deletion Request Plugin Initialized.\n");
    return MOSQ_ERR_SUCCESS;
}

// Plugin cleanup
int mosquitto_plugin_cleanup(void *userdata, struct mosquitto_opt *opts, int opt_count) {
    mosquitto_plugin_id_t *identifier = (mosquitto_plugin_id_t *)userdata;

    // Unregister callback
    mosquitto_callback_unregister(identifier, MOSQ_EVT_MESSAGE, on_message, NULL);

    printf("Deletion Request Plugin Cleaned Up.\n");
    return MOSQ_ERR_SUCCESS;
}
