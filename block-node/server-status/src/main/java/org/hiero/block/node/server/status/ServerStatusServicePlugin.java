// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import static java.lang.System.Logger.Level.TRACE;
import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/**
 * Plugin that implements the BlockNodeService and provides the 'serverStatus' RPC.
 */
public class ServerStatusServicePlugin implements BlockNodePlugin, BlockNodeServiceInterface {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block provider */
    private HistoricalBlockFacility blockProvider;
    /** The block node context, used to provide access to facilities */
    private BlockNodeContext context;
    /** Counter for the number of status requests */
    private LongCounter.Measurement requestStatusCounter;
    /** Counter for the number of detail requests */
    private LongCounter.Measurement requestDetailCounter;

    /**
     * Handle a request for server status
     *
     * @param request the request containing the available blocks, state snapshot status and software version
     * @return the response containing the block or an error status
     */
    @Override
    @NonNull
    public ServerStatusResponse serverStatus(@NonNull final ServerStatusRequest request) {
        requestStatusCounter.increment();

        final ServerStatusResponse.Builder serverStatusResponseBuilder = ServerStatusResponse.newBuilder();
        final long firstAvailableBlock = blockProvider.availableBlocks().min();
        final long lastAvailableBlock = blockProvider.availableBlocks().max();

        // TODO(#579) Should get from state config or status, which would be provided by the context from responsible
        // facility
        boolean onlyLatestState = false;

        // TODO(#1139) Should get construct a block node version object from application config, which would be provided
        // by
        // the context from responsible facility

        // Build the response
        ServerStatusResponse response = serverStatusResponseBuilder
                .firstAvailableBlock(firstAvailableBlock)
                .lastAvailableBlock(lastAvailableBlock)
                .onlyLatestState(onlyLatestState)
                .build();

        // Log request and response
        LOGGER.log(
                TRACE, "Received server status request: {0}, and will respond with response: {1}", request, response);

        return response;
    }

    /**
     * Handle a request for server status details
     *
     * @param request the request containing the available blocks, state snapshot status and software version
     * @return the response containing the block or an error status
     */
    @Override
    @NonNull
    public ServerStatusDetailResponse serverStatusDetail(@NonNull final ServerStatusRequest request) {
        requestDetailCounter.increment();

        ServerStatusDetailResponse.Builder detailsBuilder = ServerStatusDetailResponse.newBuilder();

        // add in version information
        detailsBuilder.versionInformation(context.blockNodeVersions());

        BlockRange.Builder blockRangeBuilder = BlockRange.newBuilder();

        List<BlockRange> blockRanges = new ArrayList<>();

        for (LongRange longRange : context.historicalBlockProvider()
                .availableBlocks()
                .streamRanges()
                .toList()) {
            blockRanges.add(blockRangeBuilder
                    .rangeStart(longRange.start())
                    .rangeEnd(longRange.end())
                    .build());
        }
        // return the block availability information.
        return detailsBuilder.availableRanges(blockRanges).build();
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

        final MetricRegistry metricRegistry = context.metricRegistry();

        // Create the metrics for server status
        requestStatusCounter = metricRegistry
                .register(LongCounter.builder(MetricKey.of("server_status_requests", LongCounter.class)
                                .addCategory(METRICS_CATEGORY))
                        .setDescription("Number of server status requests"))
                .getOrCreateNotLabeled();

        // Create the metrics for server status
        requestDetailCounter = metricRegistry
                .register(LongCounter.builder(MetricKey.of("server_status_details_requests", LongCounter.class)
                                .addCategory(METRICS_CATEGORY))
                        .setDescription("Number of server status details requests"))
                .getOrCreateNotLabeled();

        // Register this service
        serviceBuilder.registerGrpcService(this);
    }
}
