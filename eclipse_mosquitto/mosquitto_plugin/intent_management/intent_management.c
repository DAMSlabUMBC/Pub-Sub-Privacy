// intent_management.c

#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include <stdio.h>
#include <string.h>

#define MAX_INTENTS_LENGTH 1024

// Global variable to hold allowed intents
static char allowed_intents[MAX_INTENTS_LENGTH] = "";

// Plugin version function
int mosquitto_plugin_version(int supported_version_count, const int *supported_versions) {
    for(int i = 0; i < supported_version_count; i++) {
        if(supported_versions[i] == 5){
            return 5;  // Specify the version of the plugin API being used
        }
    }
    return -1;  // Version not supported
}

// Callback to check if the subscriber's intent is allowed
static int on_acl_check(int event, void *event_data, void *userdata) {
    struct mosquitto_evt_acl_check *acl_check = (struct mosquitto_evt_acl_check *)event_data;
    char topic[256];
    char intent[256];

    if (acl_check->topic == NULL || acl_check->client == NULL) {
        return MOSQ_ERR_ACL_DENIED;
    }

    // Extract the intent from the topic assuming the format "topic,intent"
    sscanf(acl_check->topic, "%255[^,],%255s", topic, intent);

    // Check if the intent is in the allowed intents list
    if (strstr(allowed_intents, intent) == NULL) {
        char message[1024];
        printf(message, sizeof(message), "Intent '%s' is not allowed for topic '%s'. Allowed intents: %s\n", intent, topic, allowed_intents);

        // Send the allowed intents back to the client
        int result = mosquitto_broker_publish(
            mosquitto_client_id(acl_check->client),  // Send to the client itself
            topic,
            strlen(message),
            (void *)message,
            0,
            false,
            NULL
        );

        if (result == MOSQ_ERR_SUCCESS) {
            printf("Intent notification sent to client '%s'.\n", mosquitto_client_id(acl_check->client));
        } else {
            printf("Failed to send intent notification to client '%s'. Error: %d\n", mosquitto_client_id(acl_check->client), result);
        }

        return MOSQ_ERR_SUCCESS;  // Allow the subscription but inform about the allowed intents
    }

    return MOSQ_ERR_SUCCESS;
}

// Callback to handle messages and set allowed intents
static int on_message(int event, void *event_data, void *userdata) {
    struct mosquitto_evt_message *msg_event = (struct mosquitto_evt_message *)event_data;
    char new_intents[MAX_INTENTS_LENGTH];

    // Assuming the publisher sets the allowed intents in the payload or metadata
    strncpy(new_intents, (char *)msg_event->payload, sizeof(new_intents) - 1);
    new_intents[sizeof(new_intents) - 1] = '\0'; // Ensure null termination

    // Replace the existing allowed intents with the new ones
    strncpy(allowed_intents, new_intents, sizeof(allowed_intents) - 1);
    allowed_intents[sizeof(allowed_intents) - 1] = '\0';
    printf("Allo", allowed_intents);

    return MOSQ_ERR_SUCCESS;
}

// Plugin initialization
int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **userdata, struct mosquitto_opt *opts, int opt_count) {
    mosquitto_callback_register(identifier, MOSQ_EVT_ACL_CHECK, on_acl_check, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_MESSAGE, on_message, NULL, NULL);
    return MOSQ_ERR_SUCCESS;
}

// Plugin cleanup
int mosquitto_plugin_cleanup(void *userdata, struct mosquitto_opt *opts, int opt_count) {
    mosquitto_callback_unregister(userdata, MOSQ_EVT_ACL_CHECK, on_acl_check, NULL);
    mosquitto_callback_unregister(userdata, MOSQ_EVT_MESSAGE, on_message, NULL);
    return MOSQ_ERR_SUCCESS;
}
