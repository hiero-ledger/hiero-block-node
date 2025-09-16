# Print ENV Values
echo "-----  Configuration of Consumer Variables:  -----"
echo "GRPC_SERVER: $GRPC_SERVER"
echo "GRPC_METHOD: $GRPC_METHOD"
echo "PATH_TO_PROTO: $PATH_TO_PROTO"
echo "PROTO_IMPORT_PATH: $PROTO_IMPORT_PATH"
echo "-----  Configuration of Latency Variables:  -----"
echo "INITIAL_LATENCY: $INITIAL_LATENCY"
echo "JITTER: $JITTER"
echo "BANDWIDTH: $BANDWIDTH"
echo "INCREASE_TIME: $INCREASE_TIME"
echo "MAX_LATENCY: $MAX_LATENCY"
echo "PACKET_LOSS: $PACKET_LOSS"

# First Start consumer without any network latency so it connects without issues.
consumer.sh 1 1000 &
# Then start the network latency configuration script.
configure_latency.sh
