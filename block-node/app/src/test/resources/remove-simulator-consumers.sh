#!/bin/bash


function stop_containers() {
  # List all running containers with names starting with "block-node-consumer"
  running_containers=$(docker ps --filter "name=^block-node-consumer" --format "{{.ID}} {{.Names}}")

  # Check if any matching containers exist
  if [[ -z "$running_containers" ]]; then
    echo "No running containers found with names starting with 'block-node-consumer'."
  else
    echo "Stopping and removing block-node-consumer containers:"
    echo "$running_containers"

    # Loop through each container and stop/remove it
    while IFS= read -r running_container; do
      container_id=$(echo "$running_container" | awk '{print $1}')
      container_name=$(echo "$running_container" | awk '{print $2}')

      echo "Stopping container: $container_name ($container_id)"
      docker stop "$container_id"
    done <<< "$running_containers"
  fi
}

function remove_containers() {
  stopped_containers=$(docker ps -a --filter "name=^block-node-consumer" --filter "status=exited" --format "{{.ID}} {{.Names}}")
  if [[ -z "$stopped_containers" ]]; then
    echo "No stopped containers found with names starting with 'block-node-consumer'."
  else
    echo "Removing stopped block-node-consumer containers:"
    echo "$stopped_containers"

    while IFS= read -r stopped_container; do
      container_id=$(echo "$stopped_container" | awk '{print $1}')
      container_name=$(echo "$stopped_container" | awk '{print $2}')

      echo "Removing container: $container_name ($container_id)"
      docker rm "$container_id"
    done <<< "$stopped_containers"
  fi
}

stop_containers
remove_containers
