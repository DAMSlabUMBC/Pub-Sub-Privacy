/* per_message_declaration.c */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include "mqtt_pbac.h"

/* External global variables from mqtt_pbac.c */
extern sp_entry_t *sp_list;
extern mp_entry_t *mp_list;
extern pthread_mutex_t pbac_mutex;

/* Callback for ACL check events */
int callback_acl_check(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_acl_check *ed = event_data;
    const char *client_id = mosquitto_client_id(ed->client);
    const char *topic = ed->topic;
    int access = ed->access; // MOSQ_ACL_SUBSCRIBE, MOSQ_ACL_WRITE, or MOSQ_ACL_READ

    if (access == MOSQ_ACL_SUBSCRIBE) {
        /* Handle SUBSCRIBE operation */
        /* Extract SP from User Properties */
        const mosquitto_property *prop = ed->properties;
        char *sp_filter = NULL;

        while (prop) {
            if (mosquitto_property_identifier(prop) == MQTT_PROP_USER_PROPERTY) {
                char *key = NULL;
                char *value = NULL;
                mosquitto_property_read_string_pair((mosquitto_property *)prop, MQTT_PROP_USER_PROPERTY, &key, &value, false);

                if (strcmp(key, "SP") == 0) {
                    sp_filter = strdup(value);
                }

                mosquitto_free(key);
                mosquitto_free(value);

                if (sp_filter) break;
            }
            prop = mosquitto_property_next(prop);
        }

        if (sp_filter) {
            /* Store the SP */
            store_sp(&sp_list, client_id, topic, sp_filter);
            free(sp_filter);
            return MOSQ_ERR_SUCCESS; // Allow the subscription
        } else {
            /* No SP provided, deny the subscription */
            return MOSQ_ERR_ACL_DENIED;
        }
    } else if (access == MOSQ_ACL_WRITE) {
        /* Handle PUBLISH operation */
        /* Extract MP from User Properties */
        const mosquitto_property *prop = ed->properties;
        char *mp_filter = NULL;

        while (prop) {
            if (mosquitto_property_identifier(prop) == MQTT_PROP_USER_PROPERTY) {
                char *key = NULL;
                char *value = NULL;
                mosquitto_property_read_string_pair((mosquitto_property *)prop, MQTT_PROP_USER_PROPERTY, &key, &value, false);

                if (strcmp(key, "MP") == 0) {
                    mp_filter = strdup(value);
                }

                mosquitto_free(key);
                mosquitto_free(value);

                if (mp_filter) break;
            }
            prop = mosquitto_property_next(prop);
        }

        if (!mp_filter) {
            /* No MP provided, deny the publish */
            return MOSQ_ERR_ACL_DENIED;
        }

        /* Store the MP per topic */
        store_mp(&mp_list, topic, mp_filter);
        free(mp_filter);

        return MOSQ_ERR_SUCCESS; // Allow the publish
    } else if (access == MOSQ_ACL_READ) {
        /* Handle READ operation (message delivery to subscriber) */
        /* Retrieve the MP associated with the topic */
        char *mp_filter = NULL;

        pthread_mutex_lock(&pbac_mutex);

        /* Find the MP for the topic */
        mp_entry_t *mp_entry = mp_list;
        while (mp_entry) {
            if (strcmp(mp_entry->topic, topic) == 0) {
                mp_filter = strdup(mp_entry->mp_filter);
                break;
            }
            mp_entry = mp_entry->next;
        }

        pthread_mutex_unlock(&pbac_mutex);

        if (!mp_filter) {
            /* No MP found for this topic */
            mp_filter = strdup("");
        }

        /* Check purpose compatibility */
        int compatible = check_purpose_compatibility(topic, client_id, mp_filter);

        free(mp_filter);

        if (compatible) {
            return MOSQ_ERR_SUCCESS; // Allow the message delivery
        } else {
            return MOSQ_ERR_ACL_DENIED; // Deny the message delivery
        }
    }

    /* For other access types, allow by default */
    return MOSQ_ERR_SUCCESS;
}

/* Plugin initialization function */
int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **userdata, struct mosquitto_opt *options, int option_count)
{
    int rc;

    /* Register the ACL check callback */
    rc = mosquitto_callback_register(identifier, MOSQ_EVT_ACL_CHECK, callback_acl_check, NULL, NULL);
    if (rc != MOSQ_ERR_SUCCESS) return rc;

    return MOSQ_ERR_SUCCESS;
}

/* Plugin cleanup function */
int mosquitto_plugin_cleanup(void *userdata, struct mosquitto_opt *options, int option_count)
{
    /* Free SP and MP lists */
    free_sp_list(&sp_list);
    free_mp_list(&mp_list);

    /* Destroy mutex */
    pthread_mutex_destroy(&pbac_mutex);

    return MOSQ_ERR_SUCCESS;
}
