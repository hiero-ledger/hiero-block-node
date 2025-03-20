#!/usr/bin/env bash

# This script creates a '.env' file that is used for docker-compose as input for environment variables
# for the simulator services.

echo "Creating .env file for simulator services..."

# Generate .env file with default values
cat > .env << EOL
GRPC_SERVER_ADDRESS=block-node-server
PROMETHEUS_ENDPOINT_ENABLED=true

# For publisher service
PUBLISHER_BLOCK_STREAM_SIMULATOR_MODE=PUBLISHER_CLIENT
PUBLISHER_PROMETHEUS_ENDPOINT_PORT_NUMBER=9998

# For consumer service
CONSUMER_BLOCK_STREAM_SIMULATOR_MODE=CONSUMER
CONSUMER_PROMETHEUS_ENDPOINT_PORT_NUMBER=9997

# Logging props
JAVA_TOOL_OPTIONS=-Djava.util.logging.config.file=/app/logs/config/logging.properties
EOL

# Output the values
echo ".env properties:"
cat .env
echo
