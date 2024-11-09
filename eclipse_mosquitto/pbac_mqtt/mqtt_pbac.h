/* mqtt_pbac.h */

#ifndef MQTT_PBAC_H
#define MQTT_PBAC_H

#include <pthread.h>
#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include <mqtt_protocol.h>

/* Mutex for thread safety */
extern pthread_mutex_t pbac_mutex;

/* Subscription Purpose (SP) entry */
typedef struct sp_entry {
    char *client_id;
    char *topic;               // Subscription topic
    char *sp_filter;           // Original SP filter
    char **sp_purposes;        // Array of expanded SPs
    int sp_purpose_count;      // Number of SPs
    struct sp_entry *next;
} sp_entry_t;

/* Message Purpose (MP) entry */
typedef struct mp_entry {
    char *topic;               // Topic associated with the MP
    char *mp_filter;           // Original MP filter
    char **mp_purposes;        // Array of expanded MPs
    int mp_purpose_count;      // Number of MPs
    struct mp_entry *next;
} mp_entry_t;

/* Function prototypes */

/* Purpose Filter Expansion */
char **expand_purpose_filter(const char *filter, int *count);
void free_expanded_purposes(char **purposes, int count);

/* SP List Management */
void store_sp(sp_entry_t **list, const char *client_id, const char *topic, const char *sp_filter);
void remove_sp_entry(sp_entry_t **list, const char *client_id, const char *topic);
void free_sp_list(sp_entry_t **list);

/* MP List Management */
void store_mp(mp_entry_t **list, const char *topic, const char *mp_filter);
void remove_mp_entry(mp_entry_t **list, const char *topic);
void free_mp_list(mp_entry_t **list);

/* Purpose Compatibility Check */
int check_purpose_compatibility(const char *topic, const char *client_id, const char *mp_filter);

/* Additional Function Prototypes for 3 pbac ways */
void handle_sp_registration(struct mosquitto_evt_message *ed);
void handle_published_message(struct mosquitto_evt_message *ed);

#endif /* MQTT_PBAC_H */

