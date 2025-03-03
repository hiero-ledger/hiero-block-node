#!/bin/bash

# Function to handle SIGINT
cleanup() {
    echo "Caught SIGINT signal! Terminating all background processes..."
    for pid in "${pids[@]}"; do
        kill "$pid"
        echo "Terminated process with PID: $pid"
    done
    exit 1
}

# Trap SIGINT (Ctrl+C) and call cleanup function
trap cleanup SIGINT

# Check if the script received an integer argument
if [ -z "$1" ] || ! [[ "$1" =~ ^[0-9]+$ ]]; then
    echo "Usage: $0 <number_of_iterations>"
    exit 1
fi

# Number of times to invoke the consumer script
num_iterations=$1
pids=()
dir_name="test_output"

# Check if the directory exists
if [ -d "./$dir_name" ]; then
    echo "Directory '$dir_name' exists. Removing files inside it..."
    # Remove files inside the directory
    rm -f "./$dir_name"/*
else
    echo "Directory '$dir_name' does not exist. Creating it..."
    # Create the directory
    mkdir "./$dir_name"
fi


# Loop to invoke consumer.sh and store PIDs
for ((i = 0; i < num_iterations; i++)); do
    ./consumer.sh 0 > "./$dir_name/bg_pid_$i.txt" 2>&1 &
    bg_pid=$!
    pids+=("$bg_pid")
    echo "Started consumer.sh with PID: $bg_pid"
done

# Wait for all background processes to complete
for pid in "${pids[@]}"; do
    wait "$pid"
done
