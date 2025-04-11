#!/bin/bash

# Default values
DEFAULT_INITIAL_LATENCY=1000
DEFAULT_JITTER=500
DEFAULT_BANDWIDTH=64
DEFAULT_INCREASE_TIME=10
MAX_LATENCY=12000
PACKET_LOSS=5  # Packet loss in percentage

# Parameters with default values
INITIAL_LATENCY=${INITIAL_LATENCY:-$DEFAULT_INITIAL_LATENCY}
JITTER=${JITTER:-$DEFAULT_JITTER}
BANDWIDTH=${BANDWIDTH:-$DEFAULT_BANDWIDTH}
INCREASE_TIME=${INCREASE_TIME:-$DEFAULT_INCREASE_TIME}
CURRENT_LATENCY=$INITIAL_LATENCY
MAX_LATENCY=${MAX_LATENCY:-$MAX_LATENCY}
PACKET_LOSS=${PACKET_LOSS:-$PACKET_LOSS}

# Function to apply network configuration
apply_tc_config() {
    # Remove any existing qdisc configuration on eth0
    tc qdisc del dev eth0 root 2>/dev/null
    # Apply the new latency, jitter, and packet loss configuration
    tc qdisc add dev eth0 root handle 1: netem delay ${CURRENT_LATENCY}ms ${JITTER}ms distribution normal loss ${PACKET_LOSS}%
    # Apply the bandwidth limitation
    tc qdisc add dev eth0 parent 1:1 handle 10: tbf rate ${BANDWIDTH}kbit burst 32kbit latency 50ms
    echo "Updated configuration: Latency = ${CURRENT_LATENCY}ms, Jitter = ${JITTER}ms, Bandwidth = ${BANDWIDTH}kbit, Packet Loss = ${PACKET_LOSS}% - (distribution normal)"
}

# Initial configuration
apply_tc_config
echo "Initial configuration applied: Latency = ${CURRENT_LATENCY}ms, Jitter = ${JITTER}ms, Bandwidth = ${BANDWIDTH}kbit, Packet Loss = ${PACKET_LOSS}% - (distribution normal)"

while true; do
    sleep $INCREASE_TIME
    CURRENT_LATENCY=$((CURRENT_LATENCY + 1000))
    if [ $CURRENT_LATENCY -gt $MAX_LATENCY ]; then
        CURRENT_LATENCY=$INITIAL_LATENCY
    fi
    apply_tc_config
done
