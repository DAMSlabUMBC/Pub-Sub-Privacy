CC = gcc
CFLAGS = -fPIC -I/usr/include
LDFLAGS = -shared
LDLIBS = -lpthread

# Define all plugins except mqtt_pbac.so
PLUGINS = per_message_declaration.so registration_by_message.so registration_by_topic.so

all: $(PLUGINS)

# Compile mqtt_pbac.o (shared object)
mqtt_pbac.o: mqtt_pbac.c mqtt_pbac.h
        $(CC) $(CFLAGS) -c mqtt_pbac.c -o mqtt_pbac.o

# Compile per_message_declaration.so linking with mqtt_pbac.o
per_message_declaration.so: per_message_declaration.o mqtt_pbac.o
        $(CC) -o $@ $^ $(LDFLAGS) $(LDLIBS)

# Compile registration_by_message.so linking with mqtt_pbac.o
registration_by_message.so: registration_by_message.o mqtt_pbac.o
        $(CC) -o $@ $^ $(LDFLAGS) $(LDLIBS)

# Compile registration_by_topic.so linking with mqtt_pbac.o and sqlite3
registration_by_topic.so: registration_by_topic.o mqtt_pbac.o
        $(CC) -o $@ $^ $(LDFLAGS) $(LDLIBS) -lsqlite3

%.o: %.c mqtt_pbac.h
        $(CC) $(CFLAGS) -c $< -o $@

# Clean target
clean:
        rm -f *.o *.so

