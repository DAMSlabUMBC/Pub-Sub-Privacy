/* gdpr_plugin.c */

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

/* Initialize the database and create tables if they don't exist */
int init_database()
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

    rc = sqlite3_open("gdpr_plugin.db", &db);
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

/* Extract purpose from properties */
char *get_purpose_from_properties(const mosquitto_property *props)
{
    // Currently returns error find out why
    const mosquitto_property *prop = props;
    while (prop)
    {
        if (prop->identifier == MQTT_PROP_USER_PROPERTY)
        {
            if (prop->name && prop->value)
            {
                if (strcmp(prop->name, "Purpose") == 0)
                {
                    return strdup(prop->value);
                }
            }
        }
        prop = prop->next;
    }
    return NULL;
}

/* Callback for ACL Check */
int on_acl_check(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_acl_check *ed = event_data;
    const char *client_id = mosquitto_client_id(ed->client);
    const char *topic = ed->topic;
    int access = ed->access;
    const mosquitto_property *props = ed->properties;

    /* Preform access control checks here based on rights */
    return MOSQ_ERR_SUCCESS;
}

/* Callback for Message Event */
int on_message_event(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_message *ed = event_data;
    const char *subscriber_id = mosquitto_client_id(ed->client);
    const char *topic = ed->topic;
    const mosquitto_property *props = ed->properties;

    /* Extract Purpose and PublisherID from properties */
    char *purpose = NULL;
    char *publisher_id = NULL;

    const mosquitto_property *prop = props;
    while (prop)
    {
        if (prop->identifier == MQTT_PROP_USER_PROPERTY)
        {
            if (prop->name && prop->value)
            {
                if (strcmp(prop->name, "Purpose") == 0)
                {
                    purpose = strdup(prop->value);
                }
                else if (strcmp(prop->name, "ClientID") == 0)
                {
                    publisher_id = strdup(prop->value);
                }
            }
        }
        prop = prop->next;
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

/* Callback for Control Messages */
int on_control_event(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_control *ed = event_data;
    const char *client_id = mosquitto_client_id(ed->client);
    const char *topic = ed->topic;
    const mosquitto_property *props = ed->properties;

    if (strcmp(topic, "$CONTROL/gdpr/registration") == 0)
    {
        /* Extract GDPR-Info from properties */
        char *gdpr_info = NULL;
        const mosquitto_property *prop = props;
        while (prop)
        {
            if (prop->identifier == MQTT_PROP_USER_PROPERTY)
            {
                if (prop->name && prop->value)
                {
                    if (strcmp(prop->name, "GDPR-Info") == 0)
                    {
                        gdpr_info = strdup(prop->value);
                        break;
                    }
                }
            }
            prop = prop->next;
        }

        if (gdpr_info)
        {
            store_client_gdpr_info(client_id, gdpr_info);
            free(gdpr_info);
        }

        /* Do not forward this message */
        return MOSQ_ERR_SUCCESS;
    }

    /* Allow other control messages to be processed normally */
    return MOSQ_ERR_SUCCESS;
}

/* Callback for Rights Invocation Messages */
int on_rights_invocation_event(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_message *ed = event_data;
    const char *publisher_id = mosquitto_client_id(ed->client);
    const char *topic = ed->topic;

    if (strcmp(topic, "$gdpr/rights/system") == 0)
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

    /* Extract GDPR-Right and GDPR-Filter from properties */
    const mosquitto_property *prop = props;
    while (prop)
    {
        if (prop->identifier == MQTT_PROP_USER_PROPERTY)
        {
            if (prop->name && prop->value)
            {
                if (strcmp(prop->name, "GDPR-Right") == 0)
                {
                    gdpr_right = strdup(prop->value);
                }
                else if (strcmp(prop->name, "GDPR-Filter") == 0)
                {
                    gdpr_filter = strdup(prop->value);
                }
            }
        }
        prop = prop->next;
    }

    if (gdpr_right)
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

            /* Publish the request directly to the subscriber */
            mosquitto_broker_publish(subscriber_ids[i], NULL, 0, NULL, 1, false, request_props);
        }

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
    }
}

/* Plugin version */
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

    /* Initialize mutex */
    pthread_mutex_init(&db_mutex, NULL);

    /* Initialize database */
    rc = init_database();
    if (rc != SQLITE_OK)
    {
        return MOSQ_ERR_UNKNOWN;
    }

    /* Register callbacks */
    mosquitto_callback_register(identifier, MOSQ_EVT_ACL_CHECK, on_acl_check, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_MESSAGE, on_message_event, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_CONTROL, on_control_event, "$CONTROL/gdpr/#", NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_MESSAGE, on_rights_invocation_event, "$gdpr/rights/system", NULL);

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
