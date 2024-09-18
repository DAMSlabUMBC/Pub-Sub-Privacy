// intent_management.c

#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#define MAX_INTENTS_LENGTH 1024
#define MAX_TOPIC_LENGTH 256
#define MAX_INTENT_LENGTH 256
#define ALLOWED_INTENTS_TOPIC_SUFFIX "/set_intents"

// Structure to map base topics to their allowed intents
typedef struct topic_intents {
    char base_topic[MAX_TOPIC_LENGTH];
    char allowed_intents[MAX_INTENTS_LENGTH];
    struct topic_intents *next;
} topic_intents_t;

// Head of the linked list
static topic_intents_t *intents_head = NULL;

// Mutex for thread-safe access
static pthread_mutex_t intents_mutex = PTHREAD_MUTEX_INITIALIZER;

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

// Callback to handle messages and set allowed intents
static int on_message(int event, void *event_data, void *userdata) {
    struct mosquitto_evt_message *msg_event = (struct mosquitto_evt_message *)event_data;
    const void *payload = msg_event->payload;
    size_t payloadlen = msg_event->payloadlen;
    char new_intents[MAX_INTENTS_LENGTH];
    char topic[MAX_TOPIC_LENGTH];
    char base_topic[MAX_TOPIC_LENGTH];

    if (payload == NULL || msg_event->topic == NULL) {
        return MOSQ_ERR_SUCCESS;
    }

    // Check if the topic ends with /set_intents
    const char *suffix = ALLOWED_INTENTS_TOPIC_SUFFIX;
    size_t suffix_len = strlen(suffix);
    size_t topic_len = strlen(msg_event->topic);

    if (topic_len <= suffix_len || strcmp(msg_event->topic + topic_len - suffix_len, suffix) != 0) {
        // Not a set_intents message
        return MOSQ_ERR_SUCCESS;
    }

    // Extract the base_topic by removing the suffix
    strncpy(base_topic, msg_event->topic, topic_len - suffix_len);
    base_topic[topic_len - suffix_len] = '\0';
    trim_whitespace(base_topic);

    // Ensure payload length is within bounds
    if (payloadlen >= MAX_INTENTS_LENGTH) {
        fprintf(stderr, "Payload too long to set allowed intents.\n");
        return MOSQ_ERR_SUCCESS;
    }

    strncpy(new_intents, (const char *)payload, sizeof(new_intents) - 1);
    new_intents[sizeof(new_intents) - 1] = '\0'; // Ensure null termination
    trim_whitespace(new_intents);

    pthread_mutex_lock(&intents_mutex);

    // Search for the base_topic in the list
    topic_intents_t *current = intents_head;
    while(current) {
        if(strcmp(current->base_topic, base_topic) == 0) {
            // Update existing allowed intents
            strncpy(current->allowed_intents, new_intents, sizeof(current->allowed_intents) - 1);
            current->allowed_intents[sizeof(current->allowed_intents) - 1] = '\0';
            printf("Updated allowed intents for '%s': %s\n", current->base_topic, current->allowed_intents);
            pthread_mutex_unlock(&intents_mutex);
            return MOSQ_ERR_SUCCESS;
        }
        current = current->next;
    }

    // If base_topic not found, add a new entry
    topic_intents_t *new_node = mosquitto_malloc(sizeof(topic_intents_t));
    if(new_node){
        strncpy(new_node->base_topic, base_topic, sizeof(new_node->base_topic) - 1);
        new_node->base_topic[sizeof(new_node->base_topic) - 1] = '\0';
        strncpy(new_node->allowed_intents, new_intents, sizeof(new_node->allowed_intents) - 1);
        new_node->allowed_intents[sizeof(new_node->allowed_intents) - 1] = '\0';
        new_node->next = intents_head;
        intents_head = new_node;
        printf("Set allowed intents for '%s': %s\n", new_node->base_topic, new_node->allowed_intents);
    } else {
        fprintf(stderr, "Failed to allocate memory for new topic_intents node.\n");
    }

    pthread_mutex_unlock(&intents_mutex);
    return MOSQ_ERR_SUCCESS;
}

// Callback to check if the subscriber's intent is allowed
static int on_acl_check(int event, void *event_data, void *userdata) {
    struct mosquitto_evt_acl_check *acl_check = (struct mosquitto_evt_acl_check *)event_data;
    char topic[MAX_TOPIC_LENGTH];
    char intent[MAX_INTENT_LENGTH];
    char base_topic[MAX_TOPIC_LENGTH];
    char allowed_intents_copy[MAX_INTENTS_LENGTH];

    if (acl_check->topic == NULL || acl_check->client == NULL) {
        return MOSQ_ERR_ACL_DENIED;
    }

    // Extract the base_topic and intent from the topic assuming the format "base_topic,intent"
    // Example: "topic123/music"
    sscanf(acl_check->topic, "%255[^,],%255s", base_topic, intent);

    // Trim any whitespace
    trim_whitespace(base_topic);
    trim_whitespace(intent);

    pthread_mutex_lock(&intents_mutex);

    // Search for the base_topic in the list
    topic_intents_t *current = intents_head;
    while(current) {
        if(strcmp(current->base_topic, base_topic) == 0) {
            // Found the base_topic, check if the intent is allowed
            if(strstr(current->allowed_intents, intent) != NULL) {
                // Intent is allowed
                pthread_mutex_unlock(&intents_mutex);
                return MOSQ_ERR_SUCCESS;
            } else {
                // Intent is not allowed, prepare the notification message
                strncpy(allowed_intents_copy, current->allowed_intents, sizeof(allowed_intents_copy) - 1);
                allowed_intents_copy[sizeof(allowed_intents_copy) - 1] = '\0';
                pthread_mutex_unlock(&intents_mutex);

                char message[1024];
                snprintf(message, sizeof(message),
                         "Intent '%s' is not allowed for topic '%s'. Allowed intents: %s\n",
                         intent, base_topic, allowed_intents_copy);

                // Get the client ID
                const char *client_id = mosquitto_client_id(acl_check->client);

                // Prepare the topic to notify the client
                char notify_topic[MAX_TOPIC_LENGTH];
                snprintf(notify_topic, sizeof(notify_topic), "allowed_intents/%s", client_id);

                // Publish the notification message to the client
                int result = mosquitto_broker_publish(
                    NULL,               // Use broker's client ID
                    notify_topic,       // Publish to the client's specific topic
                    strlen(message),
                    (void *)message,
                    0,                  // QoS level 0
                    false,              // No retain
                    NULL                // No properties (MQTT v5)
                );

                if (result == MOSQ_ERR_SUCCESS) {
                    printf("Intent notification sent to client '%s'.\n", client_id);
                } else {
                    printf("Failed to send intent notification to client '%s'. Error: %d\n",
                           client_id, result);
                }

                return MOSQ_ERR_ACL_DENIED; // Deny the subscription
            }
        }
        current = current->next;
    }

    pthread_mutex_unlock(&intents_mutex);

    // If base_topic not found, deny the subscription
    return MOSQ_ERR_ACL_DENIED;
}

// Plugin initialization
int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **userdata,
                          struct mosquitto_opt *opts, int opt_count) {
    *userdata = identifier; // Store identifier in userdata for cleanup

    // Register callbacks
    mosquitto_callback_register(identifier, MOSQ_EVT_MESSAGE, on_message, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_ACL_CHECK, on_acl_check, NULL, NULL);

    printf("Intent Management Plugin Initialized.\n");
    return MOSQ_ERR_SUCCESS;
}

// Plugin cleanup
int mosquitto_plugin_cleanup(void *userdata, struct mosquitto_opt *opts, int opt_count) {
    mosquitto_plugin_id_t *identifier = (mosquitto_plugin_id_t *)userdata;

    // Unregister callbacks
    mosquitto_callback_unregister(identifier, MOSQ_EVT_ACL_CHECK, on_acl_check, NULL);
    mosquitto_callback_unregister(identifier, MOSQ_EVT_MESSAGE, on_message, NULL);

    // Free the linked list
    pthread_mutex_lock(&intents_mutex);
    topic_intents_t *current = intents_head;
    while(current){
        topic_intents_t *to_free = current;
        current = current->next;
        mosquitto_free(to_free);
    }
    intents_head = NULL;
    pthread_mutex_unlock(&intents_mutex);

    printf("Intent Management Plugin Cleaned Up.\n");
    return MOSQ_ERR_SUCCESS;
}
