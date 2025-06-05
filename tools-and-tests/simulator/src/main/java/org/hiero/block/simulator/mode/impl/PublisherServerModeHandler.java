// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode.impl;

import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Inject;
import org.hiero.block.simulator.grpc.PublishStreamGrpcServer;
import org.hiero.block.simulator.mode.SimulatorModeHandler;

/**
 * The {@code PublisherServerModeHandler} class implements the {@link SimulatorModeHandler} interface
 * and provides the behavior for a mode where the simulator acts as a server accepting block stream data
 * via the publish protocol.
 *
 * <p>This mode manages a gRPC server that listens for incoming block stream connections. It handles
 * the initialization, startup, and shutdown of the server components through the {@link PublishStreamGrpcServer}.
 */
public class PublisherServerModeHandler implements SimulatorModeHandler {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // Service dependencies
    private final PublishStreamGrpcServer publishStreamGrpcServer;
    /**
     * Constructs a new {@code PublisherServerModeHandler} with the specified configuration.
     */
    @Inject
    public PublisherServerModeHandler(@NonNull final PublishStreamGrpcServer publishStreamGrpcServer) {
        this.publishStreamGrpcServer = requireNonNull(publishStreamGrpcServer);
    }

    /**
     * Initializes the publisher server mode by setting up the gRPC server.
     * This method must be called before {@link #start()}.
     */
    @Override
    public void init() {
        publishStreamGrpcServer.init();
        LOGGER.log(INFO, "gRPC Server initialized for receiving blocks using publish protocol.");
    }

    /**
     * Starts the publisher server mode by activating the gRPC server to begin accepting connections
     * and receiving block stream data. This method should only be called after {@link #init()}.
     */
    @Override
    public void start() {
        publishStreamGrpcServer.start();
        LOGGER.log(INFO, "gRPC Server started successfully.");
    }

    /**
     * Stops the publisher server mode by gracefully shutting down the gRPC server.
     * This method ensures all resources are properly released.
     *
     * @throws InterruptedException if the shutdown process is interrupted
     */
    @Override
    public void stop() throws InterruptedException {
        publishStreamGrpcServer.shutdown();
    }
}
