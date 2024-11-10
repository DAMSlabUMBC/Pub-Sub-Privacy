/* registration_by_topic.c */

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

/* Global Variables */
static sqlite3 *db = NULL;
static pthread_mutex_t db_mutex;

/* Prefixes for MP and SP registration topics */
#define MP_REGISTRATION_PREFIX "$priv/MP_registration/"
#define SP_REGISTRATION_PREFIX "$priv/SP_registration/"

/* Function to initialize the SQLite database */
int initialize_database(const char *db_path)
{
    int rc;
    char *errmsg = NULL;

    /* Use the provided db_path instead of hardcoded filename */
    rc = sqlite3_open(db_path, &db);
    if (rc != SQLITE_OK) {
        mosquitto_log_printf(MOSQ_LOG_ERR, "Cannot open SQLite database: %s", sqlite3_errmsg(db));
        return rc;
    }

    const char *create_table_sql = "CREATE TABLE subscribers ("
                                   "client_id TEXT,"
                                   "topic TEXT);";
    rc = sqlite3_exec(db, create_table_sql, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
        mosquitto_log_printf(MOSQ_LOG_ERR, "Cannot create table: %s", errmsg);
        sqlite3_free(errmsg);
        return rc;
    }

    return SQLITE_OK;
}

/* Function to store subscriber information in the database */
void store_subscriber(const char *client_id, const char *topic)
{
    const char *insert_sql = "INSERT INTO subscribers (client_id, topic) VALUES (?, ?);";
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, insert_sql, -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, client_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, topic, -1, SQLITE_STATIC);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

/* Callback for ACL check events */
int callback_acl_check(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_acl_check *ed = event_data;
    const char *client_id = mosquitto_client_id(ed->client);
    const char *topic = ed->topic;
    int access = ed->access; // MOSQ_ACL_SUBSCRIBE, MOSQ_ACL_WRITE, or MOSQ_ACL_READ

    if (access == MOSQ_ACL_WRITE) {
        /* Handle PUBLISH operations */
        if (strncmp(topic, MP_REGISTRATION_PREFIX, strlen(MP_REGISTRATION_PREFIX)) == 0) {
            /* MP Registration */
            const char *remaining_topic = topic + strlen(MP_REGISTRATION_PREFIX);
            char *topic_copy = strdup(remaining_topic);
            char *mp_topic = strtok(topic_copy, "/");
            char *mp_filter = strtok(NULL, "/");

            if (mp_topic && mp_filter) {
                /* Store the MP */
                store_mp(&mp_list, mp_topic, mp_filter);
            } else if (mp_topic && !mp_filter) {
                /* If no MP is provided, use "*" */
                store_mp(&mp_list, mp_topic, "*");
            }

            free(topic_copy);

            return MOSQ_ERR_SUCCESS; // Allow the publish to the registration topic
        } else if (strncmp(topic, SP_REGISTRATION_PREFIX, strlen(SP_REGISTRATION_PREFIX)) == 0) {
            /* SP Registration */
            const char *remaining_topic = topic + strlen(SP_REGISTRATION_PREFIX);
            char *topic_copy = strdup(remaining_topic);
            char *sp_topic = strtok(topic_copy, "/");
            char *sp_filter = strtok(NULL, "/");

            if (sp_topic && sp_filter) {
                /* Store the SP */
                store_sp(&sp_list, client_id, sp_topic, sp_filter);
                store_subscriber(client_id, sp_topic); // Store in SQLite
            } else if (sp_topic && !sp_filter) {
                /* No SP filter provided, deny registration */
                free(topic_copy);
                return MOSQ_ERR_ACL_DENIED;
            }

            free(topic_copy);

            return MOSQ_ERR_SUCCESS; // Allow the publish to the registration topic
        } else {
            /* Regular publish to other topics */
            return MOSQ_ERR_SUCCESS; // Allow the publish
        }
    } else if (access == MOSQ_ACL_SUBSCRIBE) {
        /* Handle SUBSCRIBE operations */
        /* Check if the subscriber has registered an SP for this topic */
        int has_sp = 0;

        pthread_mutex_lock(&pbac_mutex);

        sp_entry_t *sp_entry = sp_list;
        while (sp_entry) {
            if (strcmp(sp_entry->client_id, client_id) == 0 && strcmp(sp_entry->topic, topic) == 0) {
                has_sp = 1;
                break;
            }
            sp_entry = sp_entry->next;
        }

        pthread_mutex_unlock(&pbac_mutex);

        if (has_sp) {
            /* Allow the subscription */
            return MOSQ_ERR_SUCCESS;
        } else {
            /* Deny the subscription */
            return MOSQ_ERR_ACL_DENIED;
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
            /* Treat the message as containing personal data and does not allow any purposes */
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

/* Plugin Version */
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
    char *db_path = NULL;

    /* Initialize mutex */
    pthread_mutex_init(&db_mutex, NULL);

    /* Read the database path from the environment variable */
    db_path = getenv("GDPR_PLUGIN_DB_PATH");

    if (db_path == NULL)
    {
        fprintf(stderr, "Database path not specified in configuration.\n");
        return MOSQ_ERR_UNKNOWN;
    }

    /* Initialize database with the specified path */
    rc = initialize_database(db_path);
    if (rc != SQLITE_OK)
    {
        free(db_path);
        return MOSQ_ERR_UNKNOWN;
    }

    free(db_path);
    
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
