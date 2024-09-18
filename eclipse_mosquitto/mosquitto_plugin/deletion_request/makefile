CC = gcc
CFLAGS = -Wall -fPIC -I/usr/include/mosquitto
LDFLAGS = -shared

TARGET = deletion_request.so
SRCS = deletion_request.c

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRCS) $(LDFLAGS) -lmosquitto

clean:
	rm -f $(TARGET)
