/* gdpr_rights_plugin.c */

#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include <mqtt_protocol.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* Database handle */
static sqlite3 *db = NULL;
static pthread_mutex_t db_mutex;

/* Topic definitions */
#define RR_TOPIC "RR"
#define RRS_TOPIC_PREFIX "RRS/"   // Subscriber-Keyed Right Request topic
#define RSYS_TOPIC "RSYS"
#define RN_TOPIC "RN"
#define RNP_TOPIC_PREFIX "RNP/"   // Publisher-Keyed Right Notification topic

/* Function Declarations */
void handle_rights_invocation(const char *publisher_id, struct mosquitto_evt_message *ed);

/* Initialize the database and create tables if they don't exist */
int init_database(const char *db_path)
{
    int rc;
    char *errmsg = NULL;

    const char *create_clients_table_sql =
        "CREATE TABLE IF NOT EXISTS clients ("
        "client_id TEXT PRIMARY KEY,"
        "gdpr_information TEXT);";

    const char *create_data_flows_table_sql =
        "CREATE TABLE IF NOT EXISTS data_flows ("
        "publisher_id TEXT,"
        "subscriber_id TEXT,"
        "topic TEXT,"
        "purpose TEXT,"
        "PRIMARY KEY (publisher_id, subscriber_id, topic, purpose));";

    rc = sqlite3_open(db_path, &db);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return rc;
    }

    rc = sqlite3_exec(db, create_clients_table_sql, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL error in CREATE TABLE clients: %s\n", errmsg);
        sqlite3_free(errmsg);
        sqlite3_close(db);
        return rc;
    }

    rc = sqlite3_exec(db, create_data_flows_table_sql, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL error in CREATE TABLE data_flows: %s\n", errmsg);
        sqlite3_free(errmsg);
        sqlite3_close(db);
        return rc;
    }

    return SQLITE_OK;
}

/* Store or update client GDPR information in the database */
void store_client_gdpr_info(const char *client_id, const char *gdpr_info)
{
    pthread_mutex_lock(&db_mutex);

    sqlite3_stmt *stmt;
    const char *sql = "INSERT INTO clients (client_id, gdpr_information) VALUES (?, ?) "
                      "ON CONFLICT(client_id) DO UPDATE SET gdpr_information = excluded.gdpr_information;";

    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, client_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, gdpr_info ? gdpr_info : "", -1, SQLITE_TRANSIENT);

    if (sqlite3_step(stmt) != SQLITE_DONE)
    {
        fprintf(stderr, "Could not execute statement: %s\n", sqlite3_errmsg(db));
    }

    sqlite3_finalize(stmt);

    pthread_mutex_unlock(&db_mutex);
}

/* Retrieve GDPR information for a given client ID */
char *get_gdpr_information(const char *client_id)
{
    pthread_mutex_lock(&db_mutex);

    sqlite3_stmt *stmt;
    const char *sql = "SELECT gdpr_information FROM clients WHERE client_id = ?;";
    char *gdpr_info = NULL;

    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, client_id, -1, SQLITE_STATIC);

    if (sqlite3_step(stmt) == SQLITE_ROW)
    {
        const unsigned char *info = sqlite3_column_text(stmt, 0);
        if (info)
        {
            gdpr_info = strdup((const char *)info);
        }
    }

    sqlite3_finalize(stmt);

    pthread_mutex_unlock(&db_mutex);

    return gdpr_info;
}

/* Store data flow information */
void add_data_flow(const char *publisher_id, const char *subscriber_id, const char *topic, const char *purpose)
{
    pthread_mutex_lock(&db_mutex);

    sqlite3_stmt *stmt;
    const char *sql = "INSERT OR IGNORE INTO data_flows (publisher_id, subscriber_id, topic, purpose) VALUES (?, ?, ?, ?);";

    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, publisher_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, subscriber_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, topic, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, purpose ? purpose : "", -1, SQLITE_TRANSIENT);

    if (sqlite3_step(stmt) != SQLITE_DONE)
    {
        fprintf(stderr, "Could not execute data flow statement: %s\n", sqlite3_errmsg(db));
    }

    sqlite3_finalize(stmt);

    pthread_mutex_unlock(&db_mutex);
}

/* Retrieve subscribers who have received data from a publisher */
void get_subscribers_of_publisher(const char *publisher_id, char ***subscriber_ids, int *subscriber_count)
{
    pthread_mutex_lock(&db_mutex);

    sqlite3_stmt *stmt;
    const char *sql = "SELECT DISTINCT subscriber_id FROM data_flows WHERE publisher_id = ?;";
    *subscriber_ids = NULL;
    *subscriber_count = 0;

    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, publisher_id, -1, SQLITE_STATIC);

    while (sqlite3_step(stmt) == SQLITE_ROW)
    {
        const unsigned char *subscriber_id = sqlite3_column_text(stmt, 0);
        *subscriber_ids = realloc(*subscriber_ids, sizeof(char *) * (*subscriber_count + 1));
        (*subscriber_ids)[*subscriber_count] = strdup((const char *)subscriber_id);
        (*subscriber_count)++;
    }

    sqlite3_finalize(stmt);

    pthread_mutex_unlock(&db_mutex);
}

/* Extract GDPR information from properties */
char *get_gdpr_info_from_properties(const mosquitto_property *props)
{
    const mosquitto_property *prop = NULL;

    for (prop = props; prop != NULL; prop = mosquitto_property_next(prop))
    {
        if (mosquitto_property_identifier(prop) == MQTT_PROP_USER_PROPERTY)
        {
            char *name = NULL;
            char *value = NULL;

            prop = mosquitto_property_read_string_pair(prop, MQTT_PROP_USER_PROPERTY, &name, &value, false);
            if (name && value && strcmp(name, "GDPR-Information") == 0)
            {
                char *gdpr_info = strdup(value);
                free(name);
                free(value);
                return gdpr_info;
            }
            if (name) free(name);
            if (value) free(value);
            if (!prop)
            {
                break;
            }
        }
    }
    return NULL;
}

/* Callback for ACL Check */
int on_acl_check(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_acl_check *ed = event_data;
    const char *client_id = mosquitto_client_id(ed->client);
    int access = ed->access;
    const mosquitto_property *props = ed->properties;

    if (access == MOSQ_ACL_SUBSCRIBE)
    {
        /* Intercept SUBSCRIBE messages to extract GDPR information */
        char *gdpr_info = get_gdpr_info_from_properties(props);
        if (gdpr_info)
        {
            /* Store GDPR information for the client */
            store_client_gdpr_info(client_id, gdpr_info);
            free(gdpr_info);
            return MOSQ_ERR_SUCCESS;
        }
        else
        {
            /* GDPR-Information not provided; reject the subscription */
            fprintf(stderr, "GDPR-Information not provided in SUBSCRIBE by client '%s'\n", client_id);
            return MOSQ_ERR_ACL_DENIED;
        }
    }

    /* Allow other ACL checks to proceed */
    return MOSQ_ERR_SUCCESS;
}

/* Callback for Message Event */
int on_message_event(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_message *ed = event_data;
    const char *subscriber_id = mosquitto_client_id(ed->client);
    const char *topic = ed->topic;
    const mosquitto_property *props = ed->properties;

    /* Extract Purpose and ClientID (PublisherID) from properties */
    char *purpose = NULL;
    char *publisher_id = NULL;

    const mosquitto_property *prop = NULL;

    for (prop = props; prop != NULL; prop = mosquitto_property_next(prop))
    {
        if (mosquitto_property_identifier(prop) == MQTT_PROP_USER_PROPERTY)
        {
            char *name = NULL;
            char *value = NULL;

            prop = mosquitto_property_read_string_pair(prop, MQTT_PROP_USER_PROPERTY, &name, &value, false);
            if (name && value)
            {
                if (strcmp(name, "Purpose") == 0)
                {
                    purpose = strdup(value);
                }
                else if (strcmp(name, "ClientID") == 0)
                {
                    publisher_id = strdup(value);
                }
            }
            if (name) free(name);
            if (value) free(value);
            if (!prop)
            {
                break;
            }
        }
    }

    if (publisher_id)
    {
        /* Store data flow information */
        add_data_flow(publisher_id, subscriber_id, topic, purpose);
        free(publisher_id);
    }

    if (purpose)
    {
        free(purpose);
    }

    return MOSQ_ERR_SUCCESS;
}

/* Callback for Rights Invocation Messages */
int on_rights_invocation_event(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_message *ed = event_data;
    const char *publisher_id = mosquitto_client_id(ed->client);
    const char *topic = ed->topic;

    if (strcmp(topic, RSYS_TOPIC) == 0)
    {
        /* Handle rights invocation */
        handle_rights_invocation(publisher_id, ed);

        /* Do not forward this message */
        return MOSQ_ERR_SUCCESS;
    }

    /* Allow other messages to be processed normally */
    return MOSQ_ERR_SUCCESS;
}

/* Handle Rights Invocation */
void handle_rights_invocation(const char *publisher_id, struct mosquitto_evt_message *ed)
{
    const mosquitto_property *props = ed->properties;

    char *gdpr_right = NULL;
    char *gdpr_filter = NULL;
    char *response_topic = NULL;         // Changed from const char * to char *
    void *correlation_data = NULL;       // Changed from const void * to void *
    uint16_t correlation_data_len = 0;

    const mosquitto_property *prop = NULL;

    for (prop = props; prop != NULL; prop = mosquitto_property_next(prop))
    {
        int identifier = mosquitto_property_identifier(prop);

        if (identifier == MQTT_PROP_USER_PROPERTY)
        {
            char *name = NULL;
            char *value = NULL;

            prop = mosquitto_property_read_string_pair(prop, MQTT_PROP_USER_PROPERTY, &name, &value, false);
            if (name && value)
            {
                if (strcmp(name, "GDPR-Right") == 0)
                {
                    gdpr_right = strdup(value);
                }
                else if (strcmp(name, "GDPR-Filter") == 0)
                {
                    gdpr_filter = strdup(value);
                }
            }
            if (name) free(name);
            if (value) free(value);
            if (!prop)
            {
                break;
            }
        }
        else if (identifier == MQTT_PROP_RESPONSE_TOPIC)
        {
            prop = mosquitto_property_read_string(prop, MQTT_PROP_RESPONSE_TOPIC, &response_topic, false);
            if (!response_topic)
            {
                fprintf(stderr, "Failed to read response topic\n");
            }
            if (!prop)
            {
                break;
            }
        }
        else if (identifier == MQTT_PROP_CORRELATION_DATA)
        {
            prop = mosquitto_property_read_binary(prop, MQTT_PROP_CORRELATION_DATA, &correlation_data, &correlation_data_len, false);
            if (!correlation_data)
            {
                fprintf(stderr, "Failed to read correlation data\n");
            }
            if (!prop)
            {
                break;
            }
        }
    }

    if (gdpr_right && response_topic && correlation_data)
    {
        /* Identify relevant subscribers */
        char **subscriber_ids = NULL;
        int subscriber_count = 0;

        get_subscribers_of_publisher(publisher_id, &subscriber_ids, &subscriber_count);

        for (int i = 0; i < subscriber_count; i++)
        {
            /* Prepare properties for the request */
            mosquitto_property *request_props = NULL;

            /* Add GDPR-Right property */
            mosquitto_property_add_string_pair(&request_props, MQTT_PROP_USER_PROPERTY, "GDPR-Right", gdpr_right);

            /* Add GDPR-Filter property if present */
            if (gdpr_filter)
            {
                mosquitto_property_add_string_pair(&request_props, MQTT_PROP_USER_PROPERTY, "GDPR-Filter", gdpr_filter);
            }

            /* Add PublisherID property */
            mosquitto_property_add_string_pair(&request_props, MQTT_PROP_USER_PROPERTY, "PublisherID", publisher_id);

            /* Add Correlation Data */
            mosquitto_property_add_binary(&request_props, MQTT_PROP_CORRELATION_DATA, correlation_data, correlation_data_len);

            /* Publish the request to the subscriber's RRS topic */
            char rrs_topic[256];
            snprintf(rrs_topic, sizeof(rrs_topic), "%s%s", RRS_TOPIC_PREFIX, subscriber_ids[i]);

            int ret = mosquitto_broker_publish(
                NULL,
                rrs_topic,
                0,
                NULL,
                1,
                false,
                request_props);

            if (ret != MOSQ_ERR_SUCCESS)
            {
                fprintf(stderr, "Failed to publish right invocation to subscriber '%s'\n", subscriber_ids[i]);
            }

            mosquitto_property_free_all(&request_props);
        }

        /* Send broker's own GDPR information to the publisher via response topic */
        char *broker_gdpr_info = get_gdpr_information("broker_id"); // Replace "broker_id" with actual broker ID if available

        mosquitto_property *response_props = NULL;
        mosquitto_property_add_binary(&response_props, MQTT_PROP_CORRELATION_DATA, correlation_data, correlation_data_len);

        if (broker_gdpr_info)
        {
            mosquitto_property_add_string_pair(&response_props, MQTT_PROP_USER_PROPERTY, "GDPR-Information", broker_gdpr_info);
            free(broker_gdpr_info);
        }

        int ret = mosquitto_broker_publish(
            NULL,
            response_topic,
            0,
            NULL,
            1,
            false,
            response_props);

        if (ret != MOSQ_ERR_SUCCESS)
        {
            fprintf(stderr, "Failed to send GDPR info to publisher '%s'\n", publisher_id);
        }

        mosquitto_property_free_all(&response_props);

        /* Clean up */
        for (int i = 0; i < subscriber_count; i++)
        {
            free(subscriber_ids[i]);
        }
        free(subscriber_ids);
        free(gdpr_right);
        if (gdpr_filter)
        {
            free(gdpr_filter);
        }
        if (response_topic)
        {
            free(response_topic);
        }
        if (correlation_data)
        {
            free(correlation_data);
        }
    }
    else
    {
        fprintf(stderr, "Incomplete GDPR rights invocation from publisher '%s'\n", publisher_id);
    }
}

/* Plugin version */
int mosquitto_plugin_version(int supported_version_count, const int *supported_versions)
{
    int i;

    for (i = 0; i < supported_version_count; i++)
    {
        if (supported_versions[i] == 5)
        {
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

    /* Initialize database */
    rc = init_database(db_path);
    if (rc != SQLITE_OK)
    {
        return MOSQ_ERR_UNKNOWN;
    }
    
    free(db_path);

    /* Register callbacks */
    mosquitto_callback_register(identifier, MOSQ_EVT_ACL_CHECK, on_acl_check, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_MESSAGE, on_message_event, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_MESSAGE, on_rights_invocation_event, NULL, NULL);

    return MOSQ_ERR_SUCCESS;
}

/* Plugin cleanup function */
int mosquitto_plugin_cleanup(void *userdata, struct mosquitto_opt *options, int option_count)
{
    /* Close database */
    pthread_mutex_lock(&db_mutex);
    if (db)
    {
        sqlite3_close(db);
        db = NULL;
    }
    pthread_mutex_unlock(&db_mutex);
    pthread_mutex_destroy(&db_mutex);

    return MOSQ_ERR_SUCCESS;
}
