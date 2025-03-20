#!/bin/bash

# Check if the script received an integer argument
if [ -z "$1" ] || ! [[ "$1" =~ ^[0-9]+$ ]]; then
    echo "Usage: $0 <number_of_consumer_containers>"
    exit 1
fi

echo "Starting $1 consumer container(s)..."
container_count=$1
for ((i = 0; i < container_count; i++)); do
  docker run -d \
             --name block-node-consumer-$i \
             --network block-node_default \
             --env BLOCK_STREAM_SIMULATOR_MODE=CONSUMER \
             --env GRPC_SERVER_ADDRESS=172.19.0.1 \
             hedera-block-simulator
done
