# Compiler and Flags
CC = gcc
CFLAGS = -fPIC -I/usr/include
LDFLAGS = -shared -lpthread -lmosquitto -lsqlite3

# Plugins to Build
PLUGINS = per_message_declaration.so registration_by_message.so registration_by_topic.so

# Default Target
all: $(PLUGINS)

# Compile mqtt_pbac.o
mqtt_pbac.o: mqtt_pbac.c mqtt_pbac.h
	$(CC) $(CFLAGS) -c mqtt_pbac.c -o mqtt_pbac.o

libmqtt_pbac.a: mqtt_pbac.o
	ar rcs $@ $^

# Compile per_message_declaration.so
per_message_declaration.so: per_message_declaration.o libmqtt_pbac.a
	$(CC) -o $@ $^ libmqtt_pbac.a $(LDFLAGS)

# Compile registration_by_message.so
registration_by_message.so: registration_by_message.o libmqtt_pbac.a
	$(CC) -o $@ $^ libmqtt_pbac.a $(LDFLAGS)

# Compile registration_by_topic.so
registration_by_topic.so: registration_by_topic.o libmqtt_pbac.a 
	$(CC) -o $@ $^ libmqtt_pbac.a $(LDFLAGS)

# Pattern Rule for .o Files
%.o: %.c mqtt_pbac.h
	$(CC) $(CFLAGS) -c $< -o $@

# Clean Up
clean:
	rm -f *.o *.so libmqtt_pbac.a
