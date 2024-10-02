#include <mosquitto.h>
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define DB_PATH "/opt/mosquitto/logs/mqtt_delivery_log.db"
#define MAX_MESSAGE_SIZE 1024
#define BROKER_ADDRESS "visionpc01.cs.umbc.edu"
#define BROKER_PORT 1883

struct mosquitto *mosq = NULL;  // Global Mosquitto client instance

// Initialize SQLite database
void init_db() {
    sqlite3 *db;
    char *err_msg = NULL;

    if (sqlite3_open(DB_PATH, &db) != SQLITE_OK) {
        fprintf(stderr, "Failed to open SQLite database: %s\n", sqlite3_errmsg(db));
        return;
    }

    const char *sql = "CREATE TABLE IF NOT EXISTS delivery_log ("
                      "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                      "client_id TEXT NOT NULL, "
                      "topic TEXT NOT NULL, "
                      "sent_time DATETIME DEFAULT CURRENT_TIMESTAMP);";

    if (sqlite3_exec(db, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "SQLite error: %s\n", err_msg);
        sqlite3_free(err_msg);
    }

    sqlite3_close(db);
}

// Log message delivery in SQLite
void log_message_delivery(const char *client_id, const char *topic) {
    sqlite3 *db;
    sqlite3_stmt *stmt;
    const char *sql = "INSERT INTO delivery_log (client_id, topic) VALUES (?, ?);";

    if (sqlite3_open(DB_PATH, &db) != SQLITE_OK) {
        fprintf(stderr, "Failed to open SQLite database: %s\n", sqlite3_errmsg(db));
        return;
    }

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare SQLite statement: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return;
    }

    sqlite3_bind_text(stmt, 1, client_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, topic, -1, SQLITE_STATIC);

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        fprintf(stderr, "Failed to insert data into SQLite database: %s\n", sqlite3_errmsg(db));
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// Handle RTK (Right to Know) requests
void handle_rtk_request(const char *topic) {
    sqlite3 *db;
    sqlite3_stmt *stmt;
    char sql[MAX_MESSAGE_SIZE];

    if (sqlite3_open(DB_PATH, &db) != SQLITE_OK) {
        fprintf(stderr, "Failed to open SQLite database: %s\n", sqlite3_errmsg(db));
        return;
    }

    snprintf(sql, sizeof(sql), "SELECT client_id FROM delivery_log WHERE topic = ? AND sent_time > DATETIME('now', '-1 day');");
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare SQLite statement: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return;
    }

    sqlite3_bind_text(stmt, 1, topic, -1, SQLITE_STATIC);
    printf("RTK Report for topic '%s':\n", topic);

    char rtk_response[MAX_MESSAGE_SIZE] = "RTK Report: ";
    strncat(rtk_response, topic, sizeof(rtk_response) - strlen(rtk_response) - 1);
    strncat(rtk_response, " - Clients: ", sizeof(rtk_response) - strlen(rtk_response) - 1);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *client_id = (const char *)sqlite3_column_text(stmt, 0);
        printf("Client: %s\n", client_id);
        strncat(rtk_response, client_id, sizeof(rtk_response) - strlen(rtk_response) - 2);
        strncat(rtk_response, " ", sizeof(rtk_response) - strlen(rtk_response) - 1);
    }

    // Publish RTK report to the original topic
    int rc = mosquitto_publish(mosq, NULL, topic, strlen(rtk_response), rtk_response, 1, true);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "Failed to publish RTK report: %s\n", mosquitto_strerror(rc));
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// Handle deletion request notification
void handle_deletion_request(const char *topic) {
    sqlite3 *db;
    sqlite3_stmt *stmt;
    char sql[MAX_MESSAGE_SIZE];

    if (sqlite3_open(DB_PATH, &db) != SQLITE_OK) {
        fprintf(stderr, "Failed to open SQLite database: %s\n", sqlite3_errmsg(db));
        return;
    }

    snprintf(sql, sizeof(sql), "SELECT client_id FROM delivery_log WHERE topic = ? AND sent_time > DATETIME('now', '-1 day');");
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare SQLite statement: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return;
    }

    sqlite3_bind_text(stmt, 1, topic, -1, SQLITE_STATIC);
    printf("Processing deletion request for topic '%s'\n", topic);

    char deletion_message[MAX_MESSAGE_SIZE];
    snprintf(deletion_message, sizeof(deletion_message), "Publisher requests deletion of topic '%s'", topic);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *client_id = (const char *)sqlite3_column_text(stmt, 0);
        printf("Notifying client '%s' of deletion request for topic '%s'.\n", client_id, topic);

        int rc = mosquitto_publish(mosq, NULL, client_id, strlen(deletion_message), deletion_message, 1, true);
        if (rc != MOSQ_ERR_SUCCESS) {
            fprintf(stderr, "Failed to publish deletion request to client '%s': %s\n", client_id, mosquitto_strerror(rc));
        }
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

// Callback for handling messages (RTK and deletion requests)
void on_message(struct mosquitto *mosq, void *userdata, const struct mosquitto_message *message) {
    const char *topic = message->topic;

    if (strncmp(topic, "deletion_request,", 17) == 0) {
        const char *target_topic = topic + 17;
        handle_deletion_request(target_topic);
    } else if (strncmp(topic, "rtk,", 4) == 0) {
        const char *target_topic = topic + 4;
        handle_rtk_request(target_topic);
    } else {
        // Log the delivery of a regular message
        const char *client_id = mosquitto_client_id(mosq);
        log_message_delivery(client_id, topic);
    }
}

// Main function
int main() {
    // Initialize the Mosquitto library
    mosquitto_lib_init();
    mosq = mosquitto_new(NULL, true, NULL);
    if (!mosq) {
        fprintf(stderr, "Failed to create Mosquitto client.\n");
        return 1;
    }

    // Set callbacks
    mosquitto_message_callback_set(mosq, on_message);

    // Connect to the MQTT broker
    if (mosquitto_connect(mosq, BROKER_ADDRESS, BROKER_PORT, 60) != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "Failed to connect to broker.\n");
        return 1;
    }

    // Subscribe to all topics
    if (mosquitto_subscribe(mosq, NULL, "#", 0) != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "Failed to subscribe to all topics.\n");
        return 1;
    }

    // Initialize SQLite database
    init_db();

    // Start the Mosquitto client loop
    mosquitto_loop_start(mosq);

    printf("MQTT wrapper script is running...\nPress Ctrl+C to exit.\n");

    // Main loop to keep the script running
    while (1) {
        // Keep running indefinitely
    }

    // Clean up
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();

    return 0;
}
