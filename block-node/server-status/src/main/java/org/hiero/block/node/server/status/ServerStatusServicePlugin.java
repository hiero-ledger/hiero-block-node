// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import static java.lang.System.Logger.Level.DEBUG;
import static java.util.Objects.requireNonNull;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.List;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.api.protoc.BlockNodeServiceGrpc;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/**
 * Plugin that implements the BlockNodeService and provides the 'serverStatus' RPC.
 */
public class ServerStatusServicePlugin implements BlockNodePlugin, ServiceInterface {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block provider */
    private HistoricalBlockFacility blockProvider;
    /** The block node context, used to provide access to facilities */
    private BlockNodeContext context;
    /** Counter for the number of requests */
    private Counter requestCounter;

    /**
     * Handle a request for server status
     *
     * @param request the request containing the available blocks, state snapshot status and software version
     * @return the response containing the block or an error status
     */
    private ServerStatusResponse handleServiceStatusRequest(@NonNull final ServerStatusRequest request) {
        LOGGER.log(DEBUG, "Handling ServerStatusRequest");
        requestCounter.increment();

        final ServerStatusResponse.Builder serverStatusResponse = ServerStatusResponse.newBuilder();
        final long firstAvailableBlock = blockProvider.availableBlocks().min();
        final long lastAvailableBlock = blockProvider.availableBlocks().max();

        // TODO(#579) Should get from state config or status, which would be provided by the context from responsible
        // facility
        boolean onlyLatestState = false;

        // TODO(#1139) Should get construct a block node version object from application config, which would be provided
        // by
        // the context from responsible facility

        return serverStatusResponse
                .firstAvailableBlock(firstAvailableBlock)
                .lastAvailableBlock(lastAvailableBlock)
                .onlyLatestState(onlyLatestState)
                .build();
    }

    // ==== BlockNodePlugin Methods ====================================================================================
    @Override
    public String name() {
        return "ServerStatusServicePlugin";
    }

    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        requireNonNull(serviceBuilder);
        this.context = requireNonNull(context);
        this.blockProvider = requireNonNull(context.historicalBlockProvider());

        // Create the metrics
        requestCounter = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "server-status-requests")
                        .withDescription("Number of server status requests"));

        // Register this service
        serviceBuilder.registerGrpcService(this);
    }
    // ==== ServiceInterface Methods ===================================================================================
    /**
     * BlockNodeService methods define the gRPC methods available on the BlockNodeService.
     */
    enum BlockNodeServiceMethod implements Method {
        /**
         * The serverStatus method retrieves the status of this Block Node.
         */
        serverStatus
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public String serviceName() {
        String[] parts = fullName().split("\\.");
        return parts[parts.length - 1];
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public String fullName() {
        return BlockNodeServiceGrpc.SERVICE_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Method> methods() {
        return Arrays.asList(BlockNodeServiceMethod.values());
    }

    /**
     * {@inheritDoc}
     *
     * This is called each time a new request is received.
     */
    @NonNull
    @Override
    public Pipeline<? super Bytes> open(
            @NonNull Method method, @NonNull RequestOptions requestOptions, @NonNull Pipeline<? super Bytes> pipeline)
            throws GrpcException {
        final BlockNodeServiceMethod blockNodeServiceMethod = (BlockNodeServiceMethod) method;
        return switch (blockNodeServiceMethod) {
            case serverStatus:
                yield Pipelines.<ServerStatusRequest, ServerStatusResponse>unary()
                        .mapRequest(ServerStatusRequest.PROTOBUF::parse)
                        .method(this::handleServiceStatusRequest)
                        .mapResponse(ServerStatusResponse.PROTOBUF::toBytes)
                        .respondTo(pipeline)
                        .build();
        };
    }
}
