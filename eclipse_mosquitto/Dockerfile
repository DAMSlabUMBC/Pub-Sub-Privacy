##############################################
# Stage 1: Build Mosquitto and its plugins
##############################################
FROM alpine:latest AS builder

# Install build dependencies (using libxslt instead of xsltproc)
RUN apk update && apk add --no-cache \
    build-base \
    cmake \
    libxslt \
    openssl-dev \
    sqlite-dev \
    gdb \
    make \
    musl-dev \
    strace \
    bash \
    git \
    c-ares-dev \
    cjson-dev \
    util-linux-dev \
    libwebsockets-dev \
    linux-headers \
    ca-certificates \
    asciidoc \
    xmlto \
    cunit-dev \
    argon2-dev

# Copy Mosquitto source and header files
COPY mosquitto-develop /mosquitto-develop
COPY mosquitto-develop/include/mosquitto /usr/include/mosquitto
ADD mosquitto-develop/include/mqtt_protocol.h /usr/include/
ADD mosquitto-develop/include/mosquittopp.h /usr/include/
ADD mosquitto-develop/include/mosquitto_plugin.h /usr/include/
ADD mosquitto-develop/include/mosquitto_broker.h /usr/include/
ADD mosquitto-develop/include/mosquitto.h /usr/include/

WORKDIR /mosquitto-develop

# Configure, build, and install Mosquitto
RUN cmake -DWITH_WEBSOCKETS=ON \
          -DWITH_PLUGINS=ON \
          -DWITH_SYSTEMD=OFF \
          -DWITH_APPS=OFF \
          -DWITH_TESTS=OFF \
          -DWITH_EXAMPLES=OFF \
          -DWITH_DOCS=OFF \
          -DCMAKE_INSTALL_PREFIX=/usr/local . && \
    make && \
    make install && \
    cp src/mosquitto /usr/local/sbin/mosquitto

# (Optional) Verify that headers are installed
RUN ls /usr/local/include/mosquitto.h && \
    ls /usr/local/include/mosquitto_broker.h && \
    ls /usr/local/include/mosquitto_plugin.h

# Prepare directories for plugin builds and installation.
# We install plugins into /usr/local/lib/mosquitto/plugins.
RUN mkdir -p /build/intent_plugin /build/gdpr_plugin /usr/local/lib/mosquitto/plugins

# Copy plugin source code into the builder stage
COPY pbac_mqtt/ /build/intent_plugin/
COPY gdpr_rights/ /build/gdpr_plugin/

# Build the Intent Plugin and copy its plugins into the designated directory
WORKDIR /build/intent_plugin
RUN make && \
    cp per_message_declaration.so registration_by_message.so registration_by_topic.so /usr/local/lib/mosquitto/plugins/

# Build the GDPR Plugin and copy it
WORKDIR /build/gdpr_plugin
RUN make && \
    cp gdpr_rights_plugin.so /usr/local/lib/mosquitto/plugins/

# Create a directory for the SQLite DB and an empty DB file
RUN mkdir -p /usr/local/lib/mosquitto/plugins/db && \
    touch /usr/local/lib/mosquitto/plugins/db/mqtt_pbac.db
# (An environment variable is set here for reference.)
ENV GDPR_PLUGIN_DB_PATH=/usr/local/lib/mosquitto/plugins/db/mqtt_pbac.db

##############################################
# Stage 2: Create the final runtime image
##############################################
FROM alpine:latest

# Install runtime dependencies
RUN apk update && apk add --no-cache \
    openssl \
    sqlite-libs \
    c-ares \
    cjson \
    libwebsockets \
    ca-certificates \
    bash \
    argon2-libs

# Copy Mosquitto binaries, libraries, and plugins from the builder
COPY --from=builder /usr/local/ /usr/local/

# Copy your Mosquitto configuration file (ensure mosquitto.conf is in your build context)
COPY mosquitto.conf /usr/local/etc/mosquitto/mosquitto.conf

# After creating the mosquitto user, setting up /mosquitto, and creating the symlink:
    RUN adduser -D mosquitto && \
    mkdir -p /mosquitto/log && \
    touch /mosquitto/log/mosquitto.log && \
    chmod 0700 /mosquitto/log/mosquitto.log && \
    chown -R mosquitto:mosquitto /mosquitto && \
    ln -s /usr/local/lib/mosquitto/plugins /mosquitto/plugins && \
    chown -R mosquitto:mosquitto /usr/local/lib/mosquitto/plugins/db

# Set PATH so that executables are available
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# Expose Mosquitto's default port
EXPOSE 1883

# Start Mosquitto using the provided configuration file
CMD ["/bin/sh", "-c", "export GDPR_PLUGIN_DB_PATH=/mosquitto/plugins/db/mqtt_pbac.db && exec mosquitto -c /usr/local/etc/mosquitto/mosquitto.conf"]
