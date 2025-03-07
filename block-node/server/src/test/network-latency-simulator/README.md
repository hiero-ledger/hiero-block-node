# Network Latency Simulator

Due to the asynchronous nature of the Streaming Service, it is important to test the system under different network conditions, such as high latency, packet loss, and jitter. This tool allows you to simulate different network conditions by adding latency, packet loss, low bandwidth and jitter to the network traffic.

And making sure that a single `consumer` that is experiencing network issues does not affect the other `consumers` that are consuming the same stream from the BlockNode.

This test aims to make sure that the system is resilient to network issues and that a single consumer that is experiencing network issues does not affect the other consumers that are consuming the same stream from the BlockNode.

## Running Locally

1. Move to the `network-latency-simulator` directory.
2. Prepare the Test Context, Build the Docker Image with the network latency simulator.
```bash
cd server/src/test/network-latency-simulator

./setup.sh

docker build -t network-latency-simulator .
```

3. Start the BlockNode Server. (Follow instructions on main README.md)
   - Due to the Latency, the consumers might be disconnected from the BlockNode, since the current timeout is 1500 ms, you should increase the timeout to 100000ms to be able to correctly test the network issues. (see main README.md of the server for more details on how to change the timeout)
4. Start the producer and a single consumer (this consumer will be the control one without any network issues).
```bash
/server/src/test/resources/producer.sh 1 1000 # this will produce 1000 blocks
/server/src/test/resources/consumer.sh 1 1000 # this will consume 1000 blocks
```
5. Start the consumer inside the network latency simulator container, you can start as many as you want.
```bash
docker run -it --cap-add=NET_ADMIN network-latency-simulator
```

The consumer inside the container will start consuming the blocks from the BlockNode, and you can see the network issues being simulated.
The network latency simulator will simulate the following network issues:
- Latency, increases every 10 seconds (by default) by 1000ms
- Packet Loss (Drops 10% of the packets)
- Low Bandwidth, limits the bandwidth to 64kbps.
- Jitter, adds 500ms of jitter (latency variability) to the network.

There are some environment variables that you can set to change the behavior of the network latency simulator:

**configure_latency.sh:**
- `LATENCY_INCREASE_INTERVAL`: The interval in seconds to increase the latency, default is 10 seconds.
- `INITIAL_LATENCY`: The initial latency to start with, default is 500ms, once the MAX latency is reached, it will reset to the initial latency.
- `JITTER`: The jitter to add to the network, default is 500ms.
- `BANDWIDTH`: The bandwidth to limit the network to, default is 64kbps.
- `INCREASE_TIME`: The time in seconds to increase the latency, default is 10 (seconds).
- `MAX_LATENCY`: The maximum latency to reach, default is 12000 (ms).

**consumer.sh:**
- `GRPC_SERVER`: The gRPC server to connect to, default is `host.docker.internal:8080`, connects to the host BlockNode.
- `GRPC_METHOD`: The gRPC method to call, default is `BlockStreamGrpc/StreamSource`.
- `PATH_TO_PROTO`: The path to the proto file, default is `/usr/local/protos/blockstream.proto` (inside the container).
- `PROTO_IMPORT_PATH`: The import path of the proto file, default is `/usr/local/protos` (inside the container).

Example of how to set the environment variables when running the container:
```bash
docker run -it --cap-add=NET_ADMIN -e LATENCY_INCREASE_INTERVAL=5 -e INITIAL_LATENCY=1000 -e JITTER=1000 -e BANDWIDTH=128 -e INCREASE_TIME=5 -e MAX_LATENCY=10000 network-latency-simulator
```
