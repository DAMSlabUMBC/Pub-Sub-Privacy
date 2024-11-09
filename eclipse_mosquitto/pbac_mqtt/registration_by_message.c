/* registration_by_message.c */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sqlite3.h>
#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include "mqtt_pbac.h"

/* External global variables from mqtt_pbac.c */
extern sp_entry_t *sp_list;
extern mp_entry_t *mp_list;
extern pthread_mutex_t pbac_mutex;

/* SQLite database handle */
sqlite3 *db = NULL;

/* Define the special topic for MP registration */
#define MP_REGISTRATION_TOPIC "$priv/purpose_management"

/* Function prototypes */
char **split_csv(const char *str, int *count);
void store_subscriber(const char *client_id, const char *topic, const char *sp_filter);

/* Helper function to split a string by commas */
char **split_csv(const char *str, int *count)
{
    char *s = strdup(str);
    if (!s) {
        *count = 0;
        return NULL;
    }
    char *token;
    char **tokens = NULL;
    int tokens_count = 0;

    token = strtok(s, ",");
    while (token != NULL) {
        char **temp = realloc(tokens, sizeof(char *) * (tokens_count + 1));
        if (!temp) {
            // Memory allocation failed
            free(s);
            for (int i = 0; i < tokens_count; i++) {
                free(tokens[i]);
            }
            free(tokens);
            *count = 0;
            return NULL;
        }
        tokens = temp;
        tokens[tokens_count++] = strdup(token);
        token = strtok(NULL, ",");
    }
    free(s);
    *count = tokens_count;
    return tokens;
}

/* Function to initialize the SQLite database */
int initialize_database()
{
    int rc = sqlite3_open(":memory:", &db);
    if (rc != SQLITE_OK) {
        mosquitto_log_printf(MOSQ_LOG_ERR, "Cannot open SQLite database: %s", sqlite3_errmsg(db));
        return rc;
    }

    const char *create_table_sql = "CREATE TABLE subscribers ("
                                   "client_id TEXT,"
                                   "topic TEXT,"
                                   "sp_filter TEXT);"; // Ensure 'sp_filter' is included
    char *errmsg = NULL;
    rc = sqlite3_exec(db, create_table_sql, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
        mosquitto_log_printf(MOSQ_LOG_ERR, "Cannot create table: %s", errmsg);
        sqlite3_free(errmsg);
        return rc;
    }

    return SQLITE_OK;
}

/* Function to store subscriber information in the database */
void store_subscriber(const char *client_id, const char *topic, const char *sp_filter)
{
    const char *insert_sql = "INSERT INTO subscribers (client_id, topic, sp_filter) VALUES (?, ?, ?);";
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, insert_sql, -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, client_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, topic, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, sp_filter, -1, SQLITE_STATIC);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

/* Function to get all subscribers to a topic */
void get_subscribers(const char *topic, char ***client_ids, int *count)
{
    const char *select_sql = "SELECT DISTINCT client_id FROM subscribers WHERE topic = ?;";
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, select_sql, -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, topic, -1, SQLITE_STATIC);

    *client_ids = NULL;
    *count = 0;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char *client_id = sqlite3_column_text(stmt, 0);
        char **temp = realloc(*client_ids, sizeof(char *) * (*count + 1));
        if (!temp) {
            // Memory allocation failed
            sqlite3_finalize(stmt);
            return;
        }
        *client_ids = temp;
        (*client_ids)[*count] = strdup((const char *)client_id);
        (*count)++;
    }

    sqlite3_finalize(stmt);
}

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

                mosquitto_property_read_string_pair(prop, MQTT_PROP_USER_PROPERTY, &key, &value, false);

                if (key && value && strcmp(key, "SP") == 0) {
                    sp_filter = strdup(value);
                    mosquitto_free(key);
                    mosquitto_free(value);
                    break; // Assume only one SP per SUBSCRIBE
                }

                mosquitto_free(key);
                mosquitto_free(value);
            }
            prop = mosquitto_property_next(prop);
        }

        if (sp_filter) {
            /* Store the SP */
            store_sp(&sp_list, client_id, topic, sp_filter);
            store_subscriber(client_id, topic, sp_filter); // Store in SQLite
            free(sp_filter);
            return MOSQ_ERR_SUCCESS; // Allow the subscription
        } else {
            /* No SP provided, deny the subscription */
            return MOSQ_ERR_ACL_DENIED;
        }
    } else if (access == MOSQ_ACL_WRITE) {
        /* Handle PUBLISH operation */
        if (strcmp(topic, MP_REGISTRATION_TOPIC) == 0) {
            /* This is an MP registration message */
            /* Extract MP registrations from User Properties */
            const mosquitto_property *prop = ed->properties;
            bool retroactive = false;
            char *mp_topics = NULL;
            char *mp_filter = NULL;

            while (prop) {
                if (mosquitto_property_identifier(prop) == MQTT_PROP_USER_PROPERTY) {
                    char *key = NULL;
                    char *value = NULL;

                    mosquitto_property_read_string_pair(prop, MQTT_PROP_USER_PROPERTY, &key, &value, false);

                    if (key && value) {
                        if (strcmp(key, "MP") == 0) {
                            mp_topics = strdup(value);
                        } else if (strcmp(key, "MP-Filter") == 0) {
                            mp_filter = strdup(value);
                        } else if (strcmp(key, "MP-Retroactive") == 0) {
                            if (strcmp(value, "true") == 0) {
                                retroactive = true;
                            }
                        }
                    }

                    mosquitto_free(key);
                    mosquitto_free(value);
                }
                prop = mosquitto_property_next(prop);
            }

            if (mp_topics && mp_filter) {
                /* Split mp_topics by commas */
                int topic_count = 0;
                char **topics = split_csv(mp_topics, &topic_count);

                for (int i = 0; i < topic_count; i++) {
                    /* Store the MP for each topic */
                    store_mp(&mp_list, topics[i], mp_filter);
                }

                if (retroactive) {
                    /* Notify relevant subscribers */
                    for (int i = 0; i < topic_count; i++) {
                        char **client_ids = NULL;
                        int client_count = 0;
                        get_subscribers(topics[i], &client_ids, &client_count);

                        for (int j = 0; j < client_count; j++) {
                            /* Publish a notification to each client's notification topic */
                            char notification_topic[256];
                            snprintf(notification_topic, sizeof(notification_topic), "$priv/notifications/%s", client_ids[j]);
                            const char *payload = "Retroactive MP update applied to your subscriptions.";
                            mosquitto_broker_publish(NULL, notification_topic, strlen(payload), (void *)payload, 1, false, NULL);
                            free(client_ids[j]);
                        }
                        free(client_ids);
                    }
                }

                /* Free allocated memory */
                for (int i = 0; i < topic_count; i++) {
                    free(topics[i]);
                }
                free(topics);
                free(mp_topics);
                free(mp_filter);
            } else {
                /* Missing MP or MP-Filter */
                if (mp_topics) free(mp_topics);
                if (mp_filter) free(mp_filter);
                return MOSQ_ERR_ACL_DENIED;
            }

            return MOSQ_ERR_SUCCESS; // Allow the publish to the registration topic
        } else {
            /* Regular publish to other topics */
            return MOSQ_ERR_SUCCESS; // Allow the publish
        }
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
            /* No MP registered for this topic */
            /* Treat the message as containing personal data with no allowed purposes */
            mp_filter = strdup(""); // Empty MP filter
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
int mosquitto_plugin_version(int supported_version_count, const int *supported_versions)
{
	int i;

	for(i=0; i<supported_version_count; i++){
		if(supported_versions[i] == 5){
			return 5;
		}
	}
	return -1;
}

/* Plugin initialization function */
int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **userdata, struct mosquitto_opt *options, int option_count)
{
    int rc;

    /* Initialize the mutex */
    pthread_mutex_init(&pbac_mutex, NULL);

    /* Initialize the SQLite database */
    rc = initialize_database();
    if (rc != SQLITE_OK) {
        return MOSQ_ERR_UNKNOWN;
    }

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

    /* Close SQLite database */
    if (db) {
        sqlite3_close(db);
        db = NULL;
    }

    /* Destroy mutex */
    pthread_mutex_destroy(&pbac_mutex);

    return MOSQ_ERR_SUCCESS;
}
