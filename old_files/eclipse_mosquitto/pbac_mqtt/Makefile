# Compiler and Flags
CC = gcc
CFLAGS = -fPIC -I/usr/include -I/usr/local/include
LDFLAGS = -shared
LDLIBS = -lpthread -lsqlite3

# Plugins to Build
PLUGINS = per_message_declaration.so registration_by_message.so registration_by_topic.so

# Default Target
all: $(PLUGINS)

# Compile mqtt_pbac.o
mqtt_pbac.o: mqtt_pbac.c mqtt_pbac.h
	$(CC) $(CFLAGS) -c mqtt_pbac.c -o $@

# Pattern Rule for .o Files
%.o: %.c mqtt_pbac.h
	$(CC) $(CFLAGS) -c $< -o $@

# Compile per_message_declaration.so
per_message_declaration.so: per_message_declaration.o mqtt_pbac.o
	$(CC) -o $@ $^ $(LDFLAGS) $(LDLIBS)

# Compile registration_by_message.so
registration_by_message.so: registration_by_message.o mqtt_pbac.o
	$(CC) -o $@ $^ $(LDFLAGS) $(LDLIBS)

# Compile registration_by_topic.so
registration_by_topic.so: registration_by_topic.o mqtt_pbac.o
	$(CC) -o $@ $^ $(LDFLAGS) $(LDLIBS)

# Clean Up
clean:
	rm -f *.o *.so
