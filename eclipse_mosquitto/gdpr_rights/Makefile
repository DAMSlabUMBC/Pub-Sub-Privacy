# Makefile for compiling gdpr_rights_plugin.c into gdpr_rights_plugin.so

CC = gcc
CFLAGS = -fPIC -I/usr/include -I/usr/include/mosquitto
LDFLAGS = -shared
LDLIBS = -lpthread -lsqlite3

PLUGIN = gdpr_rights_plugin.so

all: $(PLUGIN)

$(PLUGIN): gdpr_rights_plugin.o
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

gdpr_rights_plugin.o: gdpr_rights_plugin.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o *.so
