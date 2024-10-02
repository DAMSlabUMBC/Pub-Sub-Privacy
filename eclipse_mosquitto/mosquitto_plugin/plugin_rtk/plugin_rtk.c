#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

#define DB_PATH "mqtt_tracking.db"

// Utility to create and initialize the database
void init_db() {
    sqlite3 *db;
    char *err_msg = 0;

    if (sqlite3_open(DB_PATH, &db) == SQLITE_OK) {
        const char *sql = 
            "CREATE TABLE IF NOT EXISTS delivery_log ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "subscriber_id TEXT NOT NULL, "
            "topic TEXT NOT NULL, "
            "sent_time DATETIME DEFAULT CURRENT_TIMESTAMP, "
            "received_time DATETIME, "
            "status TEXT NOT NULL);";
        sqlite3_exec(db, sql, 0, 0, &err_msg);
        sqlite3_close(db);
    } else {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
    }
}

// SQLite function to retrieve message logs for RTK
void get_message_logs(const char *topic, char *response, size_t response_size) {
    sqlite3 *db;
    sqlite3_stmt *stmt;
    const char *sql = "SELECT subscriber_id, sent_time, received_time, status FROM delivery_log WHERE topic = ?;";
    sqlite3_open(DB_PATH, &db);
    sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    sqlite3_bind_text(stmt, 1, topic, -1, SQLITE_STATIC);

    snprintf(response, response_size, "RTK Report for topic '%s':\n", topic);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *subscriber_id = (const char *)sqlite3_column_text(stmt, 0);
        const char *sent_time = (const char *)sqlite3_column_text(stmt, 1);
        const char *received_time = (const char *)sqlite3_column_text(stmt, 2);
        const char *status = (const char *)sqlite3_column_text(stmt, 3);
        snprintf(response + strlen(response), response_size - strlen(response),
                 "Subscriber: %s, Sent: %s, Received: %s, Status: %s\n", 
                 subscriber_id, sent_time, received_time ? received_time : "Pending", status);
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// Handle RTK requests and send response back to the publisher as payload
static int on_message(int event, void *event_data, void *userdata) {
    struct mosquitto_evt_message *msg_event = (struct mosquitto_evt_message *)event_data;
    const char *topic = msg_event->topic;
    const char *payload = (const char *)msg_event->payload;

    if (strstr(payload, "rtk")) {
        // Prepare the RTK response
        char response[1024] = {0};
        get_message_logs(topic, response, sizeof(response));

        // Send the RTK report as a response in the same flow
        mosquitto_broker_publish(NULL, msg_event->topic, strlen(response), response, 0, false, NULL);
    }

    return MOSQ_ERR_SUCCESS;
}

// Plugin initialization
int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **userdata,
                          struct mosquitto_opt *opts, int opt_count) {
    *userdata = identifier; // Store identifier in userdata for cleanup

    // Initialize SQLite database
    init_db();

    // Register callback for message events
    mosquitto_callback_register(identifier, MOSQ_EVT_MESSAGE, on_message, NULL, NULL);

    printf("RTK and Delivery Tracking Plugin Initialized.\n");
    return MOSQ_ERR_SUCCESS;
}

// Plugin cleanup
int mosquitto_plugin_cleanup(void *userdata, struct mosquitto_opt *opts, int opt_count) {
    mosquitto_plugin_id_t *identifier = (mosquitto_plugin_id_t *)userdata;

    // Unregister callbacks
    mosquitto_callback_unregister(identifier, MOSQ_EVT_MESSAGE, on_message, NULL);

    printf("RTK and Delivery Tracking Plugin Cleaned Up.\n");
    return MOSQ_ERR_SUCCESS;
}
