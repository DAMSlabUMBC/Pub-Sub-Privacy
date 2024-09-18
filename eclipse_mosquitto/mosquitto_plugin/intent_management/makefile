CC = gcc
CFLAGS = -Wall -fPIC -I/usr/include/mosquitto
LDFLAGS = -shared

TARGET = intent_management.so
SRCS = intent_management.c

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRCS) $(LDFLAGS) -lmosquitto

clean:
	rm -f $(TARGET)
