// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode.impl;

import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Inject;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.grpc.ConsumerStreamGrpcClient;
import org.hiero.block.simulator.mode.SimulatorModeHandler;

/**
 * The {@code ConsumerModeHandler} class implements the {@link SimulatorModeHandler} interface
 * and provides the behavior for a mode where only consumption of block data
 * occurs.
 *
 * <p>This mode handles single operation in the block streaming process, utilizing the
 * {@link BlockStreamConfig} for configuration settings. It is designed for scenarios where
 * the simulator needs to handle the consumption of blocks.
 *
 * <p>For now, the actual start behavior is not implemented, as indicated by the
 * {@link UnsupportedOperationException}.
 */
public class ConsumerModeHandler implements SimulatorModeHandler {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // Service dependencies
    private final ConsumerStreamGrpcClient consumerStreamGrpcClient;

    /**
     * Constructs a new {@code ConsumerModeHandler} with the specified dependencies.
     *
     * @param consumerStreamGrpcClient The client for consuming blocks via gRPC
     * @throws NullPointerException if any parameter is null
     */
    @Inject
    public ConsumerModeHandler(@NonNull final ConsumerStreamGrpcClient consumerStreamGrpcClient) {
        this.consumerStreamGrpcClient = requireNonNull(consumerStreamGrpcClient);
    }

    /**
     * Initializes the gRPC channel for block consumption.
     */
    @Override
    public void init() {
        consumerStreamGrpcClient.init();
        LOGGER.log(INFO, "gRPC Channel initialized for consuming blocks.");
    }

    /**
     * Starts consuming blocks from the stream beginning at genesis (block 0).
     * Currently, requests an infinite stream of blocks starting from genesis.
     *
     * @throws InterruptedException if the consumption process is interrupted
     */
    @Override
    public void start() throws InterruptedException {
        LOGGER.log(INFO, "Block Stream Simulator is starting in consumer mode.");
        consumerStreamGrpcClient.requestBlocks();
    }

    /**
     * Gracefully stops block consumption and shuts down the gRPC client.
     *
     * @throws InterruptedException if the shutdown process is interrupted
     */
    @Override
    public void stop() throws InterruptedException {
        consumerStreamGrpcClient.completeStreaming();
    }
}
