FROM eclipse-mosquitto:latest

# Install necessary packages including SQLite and supervisor
RUN apk update && \
    apk add --no-cache build-base openssl-dev mosquitto-dev sqlite-dev sqlite supervisor

# Copy Mosquitto headers for plugin development
ADD mosquitto_headers/mosquitto_plugin.h /usr/include/
ADD mosquitto_headers/mosquitto_broker.h /usr/include/
ADD mosquitto_headers/mosquitto.h /usr/include/

# Debug step: List contents of the headers directory
RUN ls -l /usr/include/

# Set up build directory for plugins
RUN mkdir -p /build/intent_management
RUN mkdir -p /build/mqtt_wrapper

# Copy plugin source files and Makefile for intent management plugin
ADD mosquitto_plugin/intent_management /build/intent_management

# Copy the wrapper script and Makefile
ADD mosquitto_plugin/wrapper /build/mqtt_wrapper

# Set working directory for intent management plugin and build
WORKDIR /build/intent_management
RUN make

# Set working directory for wrapper script and build it using Makefile
WORKDIR /build/mqtt_wrapper
RUN make

# Debug step: List contents of the build directories to verify shared libraries and binaries
RUN ls -l /build/intent_management
RUN ls -l /build/mqtt_wrapper

# Create plugins directory in the final image
RUN mkdir -p /mosquitto/plugins

# Copy the built intent management plugin to the plugins directory
RUN cp /build/intent_management/intent_management.so /mosquitto/plugins/

# Create a directory for SQLite database files
RUN mkdir -p /opt/mosquitto/logs

# Copy the configuration file for Mosquitto
ADD mosquitto.conf /mosquitto/config/mosquitto.conf

# Copy the supervisor configuration file
ADD supervisord.conf /etc/supervisord.conf

# Expose default MQTT ports
EXPOSE 1883 9001

# Set entrypoint to Supervisor to run both the Mosquitto broker and wrapper script
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
