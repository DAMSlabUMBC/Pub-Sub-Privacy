# Compiler
CC = gcc

# Compiler flags
CFLAGS = -Wall -fPIC -I/usr/include/mosquitto

# Linker flags for shared libraries
LDFLAGS = -shared -lsqlite3 -lmosquitto

# Output file name
TARGET = rtk_delivery_tracking.so

# Source file
SRC = rtk_delivery_tracking.c

# Build the plugin
all: $(TARGET)

# Build shared object from source file
$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET) $(LDFLAGS)

# Clean the build
clean:
	rm -f $(TARGET)
