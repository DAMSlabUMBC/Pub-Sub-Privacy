CC = gcc
CFLAGS = -fPIC -Wall
LDFLAGS = -shared
MOSQUITTO_INC = /usr/include/mosquitto
TARGET = deletion_request.so

all: $(TARGET)

$(TARGET): deletion_request.c
	$(CC) $(CFLAGS) -I$(MOSQUITTO_INC) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)

