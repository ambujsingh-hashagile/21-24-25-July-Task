# Use the official Debezium Connect base image
FROM debezium/connect:1.9

# Install the Debezium transformers plugin
RUN confluent-hub install --no-prompt debezium/debezium-connector-transformer:latest

# Install the confluent-hub tool
USER root
RUN wget -O /usr/local/bin/confluent-hub https://platform-registry.client.confluent.io/v3.3.0/api/ccu/version/latest/download/confluent-hub-linux-amd64 \
    && chmod +x /usr/local/bin/confluent-hub

# Switch back to the default Debezium user
USER debezium
