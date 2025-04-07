/* mqtt_pbac.c */

#include "mqtt_pbac.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

/* Define the global mutex */
pthread_mutex_t pbac_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Global lists */
sp_entry_t *sp_list = NULL;
mp_entry_t *mp_list = NULL;

/* Function prototypes */
void expand_purpose_filter_recursive(const char *filter, char ***purposes, int *count);

/* Split a string by a delimiter */
char **split_string(const char *str, const char *delim, int *count)
{
    char *s = strdup(str);
    if (!s) {
        *count = 0;
        return NULL;
    }
    char *token;
    char **tokens = NULL;
    int tokens_count = 0;

    token = strtok(s, delim);
    while (token != NULL) {
        char **temp = realloc(tokens, sizeof(char *) * (tokens_count + 1));
        if (!temp) {
            // Memory allocation failed
            free(s);
            free_expanded_purposes(tokens, tokens_count);
            *count = 0;
            return NULL;
        }
        tokens = temp;
        tokens[tokens_count++] = strdup(token);
        token = strtok(NULL, delim);
    }
    free(s);
    *count = tokens_count;
    return tokens;
}

/* Expand the purpose filter into all the possible purposes */
char **expand_purpose_filter(const char *filter, int *count)
{
    char **purposes = NULL;
    *count = 0;
    expand_purpose_filter_recursive(filter, &purposes, count);
    return purposes;
}

/* Recursively expands a purpose filter */
void expand_purpose_filter_recursive(const char *filter, char ***purposes, int *count)
{
    if (!filter || strlen(filter) == 0) {
        return;
    }

    const char *open_brace = strchr(filter, '{');
    const char *close_brace = strchr(filter, '}');

    if (open_brace && close_brace && open_brace < close_brace) {
        size_t prefix_len = open_brace - filter;
        char *prefix = strndup(filter, prefix_len);
        if (!prefix) return;

        size_t options_len = close_brace - open_brace - 1;
        char *options_str = strndup(open_brace + 1, options_len);
        if (!options_str) {
            free(prefix);
            return;
        }

        const char *suffix = close_brace + 1;

        int options_count = 0;
        char **options = split_string(options_str, ",", &options_count);
        if (!options && options_count > 0) {
            free(prefix);
            free(options_str);
            return;
        }

        for (int i = 0; i < options_count; i++) {
            size_t new_filter_len = strlen(prefix) + strlen(options[i]) + strlen(suffix) + 1;
            char *new_filter = malloc(new_filter_len);
            if (!new_filter) continue;
            snprintf(new_filter, new_filter_len, "%s%s%s", prefix, options[i], suffix);
            expand_purpose_filter_recursive(new_filter, purposes, count);
            free(new_filter);
        }

        free(prefix);
        free(options_str);
        for (int i = 0; i < options_count; i++) {
            free(options[i]);
        }
        free(options);
    } else {
        // No braces left so add the filter as is
        char **temp = realloc(*purposes, sizeof(char *) * (*count + 1));
        if (!temp) return;
        *purposes = temp;
        (*purposes)[*count] = strdup(filter);
        (*count)++;
    }
}

/* Frees the memory allocated for expanded purposes */
void free_expanded_purposes(char **purposes, int count)
{
    if (purposes) {
        for (int i = 0; i < count; i++) {
            free(purposes[i]);
        }
        free(purposes);
    }
}

/* SP List Management */
void store_sp(sp_entry_t **list, const char *client_id, const char *topic, const char *sp_filter)
{
    sp_entry_t *new_entry = malloc(sizeof(sp_entry_t));
    if (!new_entry) return;

    new_entry->client_id = strdup(client_id);
    new_entry->topic = strdup(topic);
    new_entry->sp_filter = strdup(sp_filter);
    new_entry->sp_purposes = expand_purpose_filter(sp_filter, &new_entry->sp_purpose_count);
    new_entry->next = NULL;

    pthread_mutex_lock(&pbac_mutex);
    new_entry->next = *list;
    *list = new_entry;
    pthread_mutex_unlock(&pbac_mutex);
}

/* Removes a SP entry */
void remove_sp_entry(sp_entry_t **list, const char *client_id, const char *topic)
{
    pthread_mutex_lock(&pbac_mutex);
    sp_entry_t *current = *list;
    sp_entry_t *prev = NULL;

    while (current) {
        if (strcmp(current->client_id, client_id) == 0 && strcmp(current->topic, topic) == 0) {
            if (prev) {
                prev->next = current->next;
            } else {
                *list = current->next;
            }
            free(current->client_id);
            free(current->topic);
            free(current->sp_filter);
            free_expanded_purposes(current->sp_purposes, current->sp_purpose_count);
            free(current);
            break;
        }
        prev = current;
        current = current->next;
    }
    pthread_mutex_unlock(&pbac_mutex);
}

/* Frees the entire SP list */
void free_sp_list(sp_entry_t **list)
{
    pthread_mutex_lock(&pbac_mutex);
    sp_entry_t *current = *list;
    while (current) {
        sp_entry_t *next = current->next;
        free(current->client_id);
        free(current->topic);
        free(current->sp_filter);
        free_expanded_purposes(current->sp_purposes, current->sp_purpose_count);
        free(current);
        current = next;
    }
    *list = NULL;
    pthread_mutex_unlock(&pbac_mutex);
}

/* MP List Management */
void store_mp(mp_entry_t **list, const char *topic, const char *mp_filter)
{
    mp_entry_t *new_entry = malloc(sizeof(mp_entry_t));
    if (!new_entry) return;

    new_entry->topic = strdup(topic);
    new_entry->mp_filter = strdup(mp_filter);
    new_entry->mp_purposes = expand_purpose_filter(mp_filter, &new_entry->mp_purpose_count);
    new_entry->next = NULL;

    pthread_mutex_lock(&pbac_mutex);
    new_entry->next = *list;
    *list = new_entry;
    pthread_mutex_unlock(&pbac_mutex);
}

/* Removes a MP entry */
void remove_mp_entry(mp_entry_t **list, const char *topic)
{
    pthread_mutex_lock(&pbac_mutex);
    mp_entry_t *current = *list;
    mp_entry_t *prev = NULL;

    while (current) {
        if (strcmp(current->topic, topic) == 0) {
            if (prev) {
                prev->next = current->next;
            } else {
                *list = current->next;
            }
            free(current->topic);
            free(current->mp_filter);
            free_expanded_purposes(current->mp_purposes, current->mp_purpose_count);
            free(current);
            break;
        }
        prev = current;
        current = current->next;
    }
    pthread_mutex_unlock(&pbac_mutex);
}


/* Frees the entire MP list */
void free_mp_list(mp_entry_t **list)
{
    pthread_mutex_lock(&pbac_mutex);
    mp_entry_t *current = *list;
    while (current) {
        mp_entry_t *next = current->next;
        free(current->topic);
        free(current->mp_filter);
        free_expanded_purposes(current->mp_purposes, current->mp_purpose_count);
        free(current);
        current = next;
    }
    *list = NULL;
    pthread_mutex_unlock(&pbac_mutex);
}

/* Checks if a MP is compatiable with an SP*/
int check_purpose_compatibility(const char *topic, const char *client_id, const char *mp_filter)
{
    int mp_purpose_count = 0;
    char **mp_purposes = expand_purpose_filter(mp_filter, &mp_purpose_count);
    if (!mp_purposes && mp_purpose_count > 0) {
        return 0; // Not compatible
    }
    pthread_mutex_lock(&pbac_mutex);
    
    /* Find SP entries matching the topic and client_id */
    sp_entry_t *current_sp = sp_list;
    while (current_sp) {
        if (strcmp(current_sp->client_id, client_id) == 0 && strcmp(current_sp->topic, topic) == 0) {
            /* Compare purposes */
            for (int i = 0; i < mp_purpose_count; i++) {
                const char *mp_purpose = mp_purposes[i]; 
		for (int j = 0; j < current_sp->sp_purpose_count; j++) {
                     const char *sp_purpose = current_sp->sp_purposes[j];
                     if (strncmp(mp_purpose, sp_purpose, strlen(sp_purpose)) == 0 && (mp_purpose[strlen(sp_purpose)] == '/' || mp_purpose[strlen(sp_purpose)] == '\0')) {
                         /* Purposes match; allow */
                         pthread_mutex_unlock(&pbac_mutex);
                         free_expanded_purposes(mp_purposes, mp_purpose_count);
                         return 1; // Compatible
                     }
                }
            }
        }
        current_sp = current_sp->next;
    }
    pthread_mutex_unlock(&pbac_mutex);
    free_expanded_purposes(mp_purposes, mp_purpose_count);
                           
    /* No compatible SP found */
    return 0; // Not compatible
}
