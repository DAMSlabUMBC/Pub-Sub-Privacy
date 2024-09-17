FROM eclipse-mosquitto:latest

# Install necessary packages
RUN apk update && apk add build-base openssl-dev mosquitto-dev

# Copy local mosquitto headers
ADD mosquitto_headers/mosquitto_plugin.h /usr/include/
ADD mosquitto_headers/mosquitto_broker.h /usr/include/
ADD mosquitto_headers/mosquitto.h /usr/include/

# Debug step: List contents of the headers directory
RUN ls -l /usr/include/

# Set up build directory
RUN mkdir -p /build

# Copy plugin source files and Makefile
ADD mosquitto_plugin/deletion_request /build/deletion_request
ADD mosquitto_plugin/intent_management /build/intent_management

# Set working directory
WORKDIR /build/deletion_request

# Build the plugins
RUN make

# Set working directory
WORKDIR /build/intent_management

# Build the plugins
RUN make

# Debug step: List contents of the build directory to verify shared libraries are built
RUN ls -l /build/deletion_request
RUN ls -l /build/intent_management

# Create the plugins directory in the final image
RUN mkdir -p /mosquitto/plugins

# Copy the built plugins to the plugins directory
RUN cp /build/deletion_request/deletion_request.so /mosquitto/plugins/
RUN cp /build/intent_management/intent_management.so /mosquitto/plugins/

# Copy the configuration file
ADD mosquitto.conf /mosquitto/config/mosquitto.conf
