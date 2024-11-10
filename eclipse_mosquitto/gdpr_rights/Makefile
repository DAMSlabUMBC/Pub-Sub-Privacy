# Makefile for compiling gdpr_rights_plugin.c into gdpr_rights_plugin.so

CC = gcc
CFLAGS = -fPIC -I/usr/include
LDFLAGS = -shared -lpthread -lsqlite3 -lmosquitto
TARGET = gdpr_rights_plugin.so
SOURCES = gdpr_rights_plugin.c
OBJECTS = $(SOURCES:.c=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) -o $@ $(OBJECTS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET)
