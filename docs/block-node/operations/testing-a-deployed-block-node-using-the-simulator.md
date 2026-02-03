# Testing a Deployed Block Node Using the Simulator

## Overview

After deploying a Block Node using either manual Kubernetes configuration or Solo Weaver, you must verify that the node is functioning correctly by testing its ability to receive and process streamed blocks.

This guide walks you through testing your Block Node deployment using a simulator Docker container that publishes test blocks to the node.

The testing process uses a Docker Compose file to create a simulator publisher container.
This container streams blocks to your deployed Block Node, verifying connectivity and block processing functionality.

## Prerequisites

Before you begin, ensure you have:

- A running Block Node deployment on your system using one of the following methods:
  - [**Manual Single-Node Kubernetes Deployment**](./single-node-k8s-deployment.md)
  - [**Solo Weaver Single-Node Kubernetes Deployment**](./solo-weaver-single-node-k8s-deployment.md)
- **[Docker](https://docs.docker.com/get-started/get-docker/) and Docker Compose** installed and available on your machine where you will run the simulator.
- The **gRPC** service address and port for your Block Node:
  - For Local deployment: `localhost:40840`
  - For cloud deployment server address:

    ```bash
    kubectl get svc -n block-node
    ```
- A gRPC client tool for verification (optional but recommended):
  - **Postman** (with gRPC support), or
  - **grpcurl** command-line tool

## Step 1: Create the Simulator Docker Compose File

1. Create a new file named **`docker-compose-publisher.yaml`** in the root of your project (or any location where you plan to run the test):

   ```bash
   touch docker-compose-publisher.yaml
   ```
2. Add the following Docker Compose configuration to the file:

   ```yaml
   services:
     simulator-publisher:
       container_name: simulator-publisher
       image: ghcr.io/hiero-ledger/hiero-block-node/simulator-image:<BLOCK_NODE_VERSION_TAG>
       environment:
         - BLOCK_STREAM_SIMULATOR_MODE=PUBLISHER_CLIENT
         - GRPC_SERVER_ADDRESS=<BLOCK_NODE_HOST>
         - GRPC_PORT=40840
         - GENERATOR_START_BLOCK_NUMBER=0
         - GENERATOR_END_BLOCK_NUMBER=100
   ```

- Replace `<BLOCK_NODE_HOST>` with the actual service IP or hostname for your Block Node.
- Replace `40840` with the gRPC port exposed by your Block Node service.
- Replace `<BLOCK_NODE_VERSION_TAG>` with the Block Node version you are testing (for example, `0.27.0`). Always use a simulator image tag that matches your Block Node version.
  See the [package registry](https://github.com/hiero-ledger/hiero-block-node/pkgs/container/hiero-block-node%2Fsimulator-image) for available tags.

### **Choose the Correct Block Node Address:**

Use the appropriate GRPC_SERVER_ADDRESS value based on where your Block Node is running:

- **Local machine (Kubernetes on your VM)**: Use the Kubernetes service IP from the **`EXTERNAL-IP`** column (e.g., **`10.96.15.190`**).
- **Docker container on the same machine**: Use the Kubernetes service IP since Docker containers can reach Kubernetes services on the host VM, but **`host.docker.internal`** may not work in all cloud environments.
- **Different machine/network**: Use the public external IP from your cloud provider (if available).

### **Environment Variable Explanation:**

The following variables are the minimum required to run the simulator publisher against your Block Node:

- **BLOCK_STREAM_SIMULATOR_MODE**: Set to **`PUBLISHER_CLIENT`** so the simulator acts as a publisher and stream blocks to the Block Node.
- **GRPC_SERVER_ADDRESS**: The Kubernetes service IP of your Block Node (e.g., **`10.96.0.15`**).
- **GRPC_PORT**: The gRPC port exposed by the Block Node service (e.g., **`40840`**).
- **GENERATOR_START_BLOCK_NUMBER**: First block number the simulator will generate (for example, **`0`**).
- **GENERATOR_END_BLOCK_NUMBER**: Last block number the simulator will generate (for example, **`100`**).

For the complete and official Block Node configuration reference, see the [Block Node configuration document](https://github.com/hiero-ledger/hiero-block-node/blob/main/docs/block-node/configuration.md).

## Step 2: Run the Docker Compose File

1. From the directory containing **`docker-compose-publisher.yaml`**, run the Docker Compose command to start the simulator:

   ```bash
   docker compose -f docker-compose-publisher.yaml up
   ```
2. The simulator will begin publishing test blocks to your Block Node. You will see logs indicating the blocks are being streamed.
3. The simulator will continue to stream blocks until it reaches the specified block number in the configuration (for example, block 100), at which point it will automatically stop.

## Step 3: Verify Block Streaming

Once the Docker Compose container is running, verify that blocks are being streamed to your Block Node:

1. **Check Simulator Logs**:

   Monitor the terminal where `docker-compose up` is running, the output will show the simulator publishing blocks:

   ```bash
   [+] Running 1/1
    ✔ Container simulator-publisher  Recreated                                                                                                                                  0.1s
   Attaching to simulator-publisher
   simulator-publisher  | Dec 30, 2025 9:49:47 PM org.hiero.block.simulator.BlockStreamSimulator main
   simulator-publisher  | INFO: Starting Block Stream Simulator!
   simulator-publisher  | Dec 30, 2025 9:49:47 PM org.hiero.block.simulator.generator.CraftBlockStreamManager <init>
   simulator-publisher  | INFO: Block Stream Simulator will use Craft mode for block management
   simulator-publisher  | 2025-12-30 21:49:47.384+0000 INFO    BlockStreamSimulatorApp#start            Block Stream Simulator started initializing components...
   simulator-publisher  | 2025-12-30 21:49:47.402+0000 INFO    SimulatorConfigurationLogger#log         =======================================================================================
   simulator-publisher  | 2025-12-30 21:49:47.402+0000 INFO    SimulatorConfigurationLogger#log         Simulator Configuration
   simulator-publisher  | 2025-12-30 21:49:47.403+0000 INFO    SimulatorConfigurationLogger#log         =======================================================================================
   simulator-publisher  | 2025-12-30 21:49:47.404+0000 INFO    SimulatorConfigurationLogger#log         blockStream.blockItemsBatchSize=1000
   simulator-publisher  | 2025-12-30 21:49:47.404+0000 INFO    SimulatorConfigurationLogger#log         blockStream.delayBetweenBlockItems=1500000
   simulator-publisher  | 2025-12-30 21:49:47.405+0000 INFO    SimulatorConfigurationLogger#log         blockStream.endStreamEarliestBlockNumber=0
   simulator-publisher  | 2025-12-30 21:49:47.405+0000 INFO    SimulatorConfigurationLogger#log         blockStream.endStreamFrequency=0
   simulator-publisher  | 2025-12-30 21:49:47.406+0000 INFO    SimulatorConfigurationLogger#log         blockStream.endStreamLatestBlockNumber=0
   simulator-publisher  | 2025-12-30 21:49:47.406+0000 INFO    SimulatorConfigurationLogger#log         blockStream.endStreamMode=NONE
   simulator-publisher  | 2025-12-30 21:49:47.406+0000 INFO    SimulatorConfigurationLogger#log         blockStream.lastKnownStatusesCapacity=10
   simulator-publisher  | 2025-12-30 21:49:47.407+0000 INFO    SimulatorConfigurationLogger#log         blockStream.maxBlockItemsToStream=100000
   simulator-publisher  | 2025-12-30 21:49:47.407+0000 INFO    SimulatorConfigurationLogger#log         blockStream.midBlockFailOffset=0
   simulator-publisher  | 2025-12-30 21:49:47.408+0000 INFO    SimulatorConfigurationLogger#log         blockStream.midBlockFailType=NONE
   simulator-publisher  | 2025-12-30 21:49:47.408+0000 INFO    SimulatorConfigurationLogger#log         blockStream.millisecondsPerBlock=1000
   simulator-publisher  | 2025-12-30 21:49:47.408+0000 INFO    SimulatorConfigurationLogger#log         blockStream.simulatorMode=PUBLISHER_CLIENT
   simulator-publisher  | 2025-12-30 21:49:47.409+0000 INFO    SimulatorConfigurationLogger#log         blockStream.streamingMode=MILLIS_PER_BLOCK
   simulator-publisher  | 2025-12-30 21:49:47.409+0000 INFO    SimulatorConfigurationLogger#log         consumer.endBlockNumber=-1
   simulator-publisher  | 2025-12-30 21:49:47.409+0000 INFO    SimulatorConfigurationLogger#log         consumer.slowDownForBlockRange=10-30
   simulator-publisher  | 2025-12-30 21:49:47.410+0000 INFO    SimulatorConfigurationLogger#log         consumer.slowDownMilliseconds=2

   ```
2. **Confirm streaming success**:

   At the end of the logs, look for the summary:

   ```bash
   simulator-publisher  | 2025-12-30 21:50:17.720+0000 INFO    PublisherClientModeHandler#millisPerBlockStreaming Block Stream Simulator has stopped
   simulator-publisher  | 2025-12-30 21:50:17.720+0000 INFO    PublisherClientModeHandler#millisPerBlockStreaming Number of BlockItems sent by the Block Stream Simulator: 1574
   simulator-publisher  | 2025-12-30 21:50:17.721+0000 INFO     PublisherClientModeHandler#millisPerBlockStreaming Number of Blocks sent by the Block Stream Simulator: 31
   ```

## Step 4: Clean Up After Testing

After testing is complete:

1. The Docker Compose container will automatically stop once it finishes streaming the configured number of test blocks.
2. To manually clean up resources:
   1. If the container is still running, stop it with **Ctrl+C** in the terminal where `docker-compose up` is running.
   2. Then remove the test blocks that were streamed and clean up resources, run:

      ```bash
      docker compose -f docker-compose-publisher.yaml down
      ```

   This will stop and remove the simulator publisher container, allowing you to perform additional testing runs if needed.

### **Resetting the Block Node after simulator testing**

If you want to run a new simulator test from a clean state, or prepare the same deployment to receive real block streams, reset the Block Node data and restart the Block Node workload.

> **Caution:** These commands permanently delete all current block data from the Block Node. Only run them in non‑production environments, or when you explicitly intend to clear test data.

For **single-node Kubernetes deployments**, run:

- Delete live and historic data directories inside the Block Node pod:

  ```bash
  kubectl -n ${NAMESPACE} exec ${POD} -- sh -c 'rm -rf /opt/hiero/block-node/data/live/* /opt/hiero/block-node/data/historic/*'
  ```

  > Note: Replace `${NAMESPACE}` and `${POD}` with values from your deployment: `${NAMESPACE}` is the Kubernetes namespace where your Block Node is running (for example, block-node), and `${POD}` is the Block Node pod name returned by `kubectl get pods -n ${NAMESPACE}` (for example, block-node-0).

- Restart the Block Node pod so it starts with a clean data directory:

  ```bash
  kubectl -n ${NAMESPACE} delete pod ${POD}
  ```

For **Docker based Block Node deployments**, run the equivalent inside the Block Node container:

```bash
# Clear live and historic data directories
docker exec <BLOCK_NODE_CONTAINER_NAME> sh -c 'rm -rf /opt/hiero/block-node/data/live/* /opt/hiero/block-node/data/historic/*'

# Restart the container
docker restart <BLOCK_NODE_CONTAINER_NAME>
```

After the pod or container restarts, the Block Node will come up with empty live and historic data directories, ready for a fresh simulator run or to begin consuming real block streams.
