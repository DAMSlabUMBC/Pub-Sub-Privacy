CC = gcc
CFLAGS = -fPIC -Wall
LDFLAGS = -shared
MOSQUITTO_INC = /usr/include/mosquitto
TARGET = intent_management.so

all: $(TARGET)

$(TARGET): intent_management.c
	$(CC) $(CFLAGS) -I$(MOSQUITTO_INC) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)

