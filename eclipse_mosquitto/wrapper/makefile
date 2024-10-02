# Compiler
CC = gcc

# Compiler flags
CFLAGS = -Wall -O2

# Libraries for linking
LIBS = -lmosquitto -lsqlite3

# Source files
SRCS = mqtt_wrapper.c

# Output binary name
TARGET = mqtt_wrapper

# Build target
all: $(TARGET)

# Linking and building the executable
$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRCS) $(LIBS)

# Clean up the build
clean:
	rm -f $(TARGET)
