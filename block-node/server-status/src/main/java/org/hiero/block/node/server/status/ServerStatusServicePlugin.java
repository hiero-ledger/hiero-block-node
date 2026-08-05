// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.hiero.block.node.spi.historicalblocks.LongRange;
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
    /** Scheduler for the periodic status heartbeat; null when the heartbeat is disabled. */
    private ScheduledExecutorService heartbeatExecutor;

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
        final long firstAvailableBlock = blockProvider.availableBlocks().min();
        long highestAvailableBlock = blockProvider.availableBlocks().max();
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
        BlockNodeContext context = blockNodeContext;

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

        // return detailed block node status information.
        return detailsBuilder
                .availableRanges(blockRanges)
                // @todo(3004) change to use context.availableBlocks() when that becomes the source of truth
                // .availableRanges(context.availableBlocks())
                .storedRanges(context.storedBlocks())
                .tssData(context.tssData())
                .nodeAddressBook(context.nodeAddressBook())
                .rangedAddressBookHistory(context.rangedAddressBookHistory())
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

    @Override
    public void start() {
        final int periodSeconds = blockNodeContext
                .configuration()
                .getConfigData(ServerStatusConfig.class)
                .heartbeatPeriodSeconds();
        if (periodSeconds <= 0) {
            LOGGER.log(DEBUG, "Server status heartbeat disabled (heartbeatPeriodSeconds={0})", periodSeconds);
            return;
        }
        heartbeatExecutor = blockNodeContext
                .threadPoolManager()
                .createSingleThreadScheduledExecutor("server-status-heartbeat", this::onHeartbeatThreadException);
        heartbeatExecutor.scheduleAtFixedRate(this::runHeartbeat, periodSeconds, periodSeconds, TimeUnit.SECONDS);
        LOGGER.log(DEBUG, "Server status heartbeat scheduled every {0}s", periodSeconds);
    }

    @Override
    public void stop() {
        if (heartbeatExecutor == null) {
            return;
        }
        // Stop scheduling new runs and let an in-flight heartbeat finish before forcing termination,
        // so we do not interrupt logStatusHeartbeat() mid-emit.
        heartbeatExecutor.shutdown();
        try {
            if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                heartbeatExecutor.shutdownNow();
            }
        } catch (final InterruptedException e) {
            heartbeatExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /// Runs one heartbeat, guarding against exceptions so a single failure does not cancel the
    /// fixed-rate schedule.
    private void runHeartbeat() {
        try {
            logStatusHeartbeat();
        } catch (final RuntimeException e) {
            LOGGER.log(WARNING, "Failed to emit server status heartbeat", e);
        }
    }

    /// Emits the single periodic `INFO` status line: the available block range and next expected
    /// block. This is the one progression signal available to operators at `INFO` without
    /// enabling `DEBUG`; per-block progress is intentionally left to metrics.
    void logStatusHeartbeat() {
        if (!LOGGER.isLoggable(INFO)) {
            return;
        }
        final BlockRangeSet availableBlocks = blockProvider.availableBlocks();
        final long nextExpectedBlock =
                blockNodeContext.applicationStateFacility().nextExpectedBlock();
        LOGGER.log(
                INFO,
                "Status heartbeat: oldestBlock={0} newestBlock={1} nextExpected={2}",
                availableBlocks.min(),
                availableBlocks.max(),
                nextExpectedBlock);
    }

    /// Logs an uncaught exception escaping the heartbeat thread at `ERROR`.
    private void onHeartbeatThreadException(final Thread thread, final Throwable throwable) {
        LOGGER.log(ERROR, "Uncaught exception in server-status heartbeat thread", throwable);
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
