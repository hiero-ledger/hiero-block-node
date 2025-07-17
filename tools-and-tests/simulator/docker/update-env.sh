#!/usr/bin/env bash

# This script creates a '.env' file that is used for docker-compose as input for environment variables
# for the simulator services.

echo "Creating .env file for simulator services..."

# Generate .env file with default values
cat > .env << EOL
GRPC_SERVER_ADDRESS=host.docker.internal
PROMETHEUS_ENDPOINT_ENABLED=true

# For publisher service
PUBLISHER_BLOCK_STREAM_SIMULATOR_MODE=PUBLISHER_CLIENT
PUBLISHER_PROMETHEUS_ENDPOINT_PORT_NUMBER=9998

# For consumer service
CONSUMER_BLOCK_STREAM_SIMULATOR_MODE=CONSUMER
CONSUMER_PROMETHEUS_ENDPOINT_PORT_NUMBER=9997
EOL

logging_config_file_arg="-Djava.util.logging.config.file=/opt/hiero/block-node/logs/config/logging.properties"
debug_arg="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5006"
# determine if we should include debug opts
[ "$1" = true ] && is_debug=true || is_debug=false
if [ true = "$is_debug" ]; then
  # The server will wait for the debugger to attach on port 5006
  echo "JAVA_TOOL_OPTIONS=${logging_config_file_arg} ${debug_arg}" >> .env
else
  echo "JAVA_TOOL_OPTIONS=${logging_config_file_arg}" >> .env
fi

# Output the values
echo ".env properties:"
cat .env
echo
