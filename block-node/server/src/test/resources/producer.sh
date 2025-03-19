#!/bin/bash
# set -x


usage_error() {
  echo "Usage: $0 <integer> [positive-integer]"
  exit 1
}

# Check if the first argument is provided and is an integer
if [ "$#" -lt 1 ] || ! [[ "$1" =~ ^[0-9]+$ ]]; then
  usage_error
fi

# Check if the second argument is provided and if it's a positive integer
if [ "$#" -eq 2 ] && ! [[ "$2" =~ ^[1-9][0-9]*$ ]]; then
  usage_error
fi

# If the script reaches here, the parameters are valid
echo "The provided integer is: $1"
if [ "$#" -eq 2 ]; then
  echo "The optional positive integer is: $2"
fi

generate_header() {
    local block_header_number=$1

    # Interpolate the integer parameter into the JSON template
    local result
    result=$(echo "$header_template" | jq --argjson number "$block_header_number" ".block_header.number = $block_header_number")

    echo "$result"
}

generate_event() {
    local creator_node_id=$1

    # Interpolate the integer parameter into the JSON template
    local result
    result=$(echo "$event_template" | jq --argjson creator_id "$creator_node_id" ".event_header.event_core.creator_node_id = $creator_node_id")

    echo "$result"
}

generate_block_proof() {
    local block_number=$1

    # Interpolate the integer parameter into the JSON template
    local result
    result=$(echo "$block_proof_template" | jq --argjson block "$block_number" ".block_proof.block = $block_number")

    echo "$result"
}

GRPC_SERVER="localhost:8080"
GRPC_METHOD="com.hedera.hapi.block.BlockStreamService/publishBlockStream"
PATH_TO_PROTO="./block_service.proto"

echo "Starting producer..."

# Signal handler to handle SIGINT (Ctrl+C)
function cleanup {
  echo "Received SIGINT, stopping..."
  kill $GRPC_PID
  exit 0
}

# Trap SIGINT
trap cleanup SIGINT

# Read the JSON template from the file
header_template=$(cat "templates/header_template.json")

# Read the JSON template from the file
block_proof_template=$(cat "templates/block_proof_template.json")

# Read the JSON template from the file
event_template=$(cat "templates/event_template.json")

# Generate and push messages to the gRPC server as a producer.
# Response messages from the gRPC server are printed to stdout.
(
  iter=$1
  block_items=10
  while true; do

    # Start the BlockItems array
    echo "{"
    echo "\"block_items\": {"
    echo "\"block_items\": ["
    # Generate 10 BlockItems per Block
    for ((i=1; i<=$block_items; i++))
    do
      if [[ $i -eq 1 ]]; then
        result=$(generate_header $iter)
        echo "$result,"
      elif [[ $i -eq $block_items ]]; then
        result=$(generate_block_proof $iter)
        echo "$result"
      else
        result=$(generate_event $i)
        echo "$result,"
      fi

      sleep 0.01
    done

    echo "]"
    echo "}"
    echo "}"

    if [ "$iter" -eq "$2" ]; then
      exit 0
    fi
    ((iter++))

  done
) | grpcurl -plaintext -proto $PATH_TO_PROTO -d @ $GRPC_SERVER $GRPC_METHOD &

GRPC_PID=$!

# Wait for grpcurl to finish
wait $GRPC_PID

echo "Finished"
