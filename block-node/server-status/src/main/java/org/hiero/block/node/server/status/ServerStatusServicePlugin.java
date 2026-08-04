// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import static java.lang.System.Logger.Level.TRACE;
import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/**
 * Plugin that implements the BlockNodeService and provides the 'serverStatus' RPC.
 */
public class ServerStatusServicePlugin implements BlockNodePlugin, BlockNodeServiceInterface {
    /** Metric key for the number of server status requests */
    public static final MetricKey<LongCounter> METRIC_SERVER_STATUS_REQUESTS =
            MetricKey.of("server_status_requests", LongCounter.class).addCategory(METRICS_CATEGORY);
    /** Metric key for the number of server status details requests */
    public static final MetricKey<LongCounter> METRIC_SERVER_STATUS_DETAILS_REQUESTS =
            MetricKey.of("server_status_details_requests", LongCounter.class).addCategory(METRICS_CATEGORY);

    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block provider */
    private HistoricalBlockFacility blockProvider;
    /** The block node context, used to provide access to facilities */
    private volatile BlockNodeContext blockNodeContext;
    /** The earliest block number this node is configured to manage */
    private long earliestManagedBlock;
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

        final ApplicationStateFacility stateFacility = blockNodeContext.applicationStateFacility();
        final ServerStatusResponse.Builder serverStatusResponseBuilder = ServerStatusResponse.newBuilder();
        // Read min and max from a single reference so both come from a consistent snapshot
        final BlockRangeSet availableBlocks = blockProvider.availableBlocks();
        final long firstAvailableBlock = availableBlocks.min();
        final long highestAvailableBlock = availableBlocks.max();
        long nextExpectedBlock = stateFacility.nextExpectedBlock();
        if (nextExpectedBlock < earliestManagedBlock) {
            nextExpectedBlock = UNKNOWN_BLOCK_NUMBER;
        }

        // TODO(#579) Should get from state config or status, which would be provided by
        //     calls to the responsible plugin.
        boolean onlyLatestState = true;

        // TODO(#1139) Should construct a block node version object from application config,
        //     which would be provided from the application state

        // Build the response
        ServerStatusResponse response = serverStatusResponseBuilder
                .firstAvailableBlock(firstAvailableBlock)
                .lastAvailableBlock(highestAvailableBlock)
                .nextExpectedBlock(nextExpectedBlock)
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

        // blockNodeContext is volatile, assign to local variable so reference stays consistent
        final BlockNodeContext context = blockNodeContext;

        // serverStatus has the latest max available block. serverStatusDetail has an up to .5s old
        // snapshot. This will align the lastBlock range end with what serverStatus would report without
        // having to rebuild the entire available Blocks list.
        List<BlockRange> fixedAvailable = !context.availableBlocks().isEmpty()
                        && context.availableBlocks().getLast().rangeEnd()
                                != blockProvider.availableBlocks().max()
                ? fixAvailable(context.availableBlocks())
                : context.availableBlocks();

        // Return detailed block node status information. Every field is read from the
        // periodically-refreshed context snapshot: this keeps the response internally consistent
        // and avoids recomputing the merged available ranges on every request (the context already
        // holds the merged List<BlockRange> maintained by the application state facility).
        return ServerStatusDetailResponse.newBuilder()
                .versionInformation(context.blockNodeVersions())
                .availableRanges(fixedAvailable)
                .storedRanges(context.storedBlocks())
                .tssData(context.tssData())
                .nodeAddressBook(context.nodeAddressBook())
                .rangedAddressBookHistory(context.rangedAddressBookHistory())
                .build();
    }

    List<BlockRange> fixAvailable(List<BlockRange> availableBlocks) {
        BlockRange lastBlockRange = availableBlocks.getLast();
        BlockRange newLastBlockRange = BlockRange.newBuilder()
                .rangeStart(lastBlockRange.rangeStart())
                .rangeEnd(blockProvider.availableBlocks().max())
                .build();

        availableBlocks.set(availableBlocks.size() - 1, newLastBlockRange);
        return availableBlocks;
    }

    // ==== BlockNodePlugin Methods ====================================================================================
    @Override
    public String name() {
        return "ServerStatusServicePlugin";
    }

    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        requireNonNull(serviceBuilder);
        this.blockNodeContext = requireNonNull(context);
        this.blockProvider = requireNonNull(context.historicalBlockProvider());
        this.earliestManagedBlock =
                context.configuration().getConfigData(NodeConfig.class).earliestManagedBlock();

        final MetricRegistry metricRegistry = context.metricRegistry();

        // Create the metrics for server status
        requestStatusCounter = metricRegistry
                .register(LongCounter.builder(METRIC_SERVER_STATUS_REQUESTS)
                        .setDescription("Number of server status requests"))
                .getOrCreateNotLabeled();

        // Create the metrics for server status
        requestDetailCounter = metricRegistry
                .register(LongCounter.builder(METRIC_SERVER_STATUS_DETAILS_REQUESTS)
                        .setDescription("Number of server status details requests"))
                .getOrCreateNotLabeled();

        // Register this service; a null port (the default) shares server.port
        final Integer port =
                context.configuration().getConfigData(ServerStatusConfig.class).port();
        serviceBuilder.registerGrpcService(port, this);
    }

    /**
     * {@inheritDoc}
     * This method is called on a separate thread. Make sure this.context is marked as `volatile`
     */
    @Override
    public void onContextUpdate(BlockNodeContext context) {
        this.blockNodeContext = context;
    }
}
