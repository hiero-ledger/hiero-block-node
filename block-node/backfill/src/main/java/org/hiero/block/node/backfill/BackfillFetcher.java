// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.swirlds.metrics.api.Counter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.backfill.client.BackfillSource;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.backfill.client.BlockNodeClient;
import org.hiero.block.node.backfill.client.GrpcWebClientTuning;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * Client for fetching blocks from block nodes using gRPC.
 * This client handles retries and uses a priority and health-based strategy to select nodes for fetching blocks.
 * <p>
 * The client fetches blocks in a specified range and retries fetching from different nodes
 * if the initial node does not have the required blocks or is unavailable.
 * It implements exponential backoff for retries to avoid overwhelming the nodes.
 * <p>
 * The client is initialized with a path to a block node preference file, which contains
 * a list of block nodes with their addresses, ports, and priorities.
 */
public class BackfillFetcher implements PriorityHealthBasedStrategy.NodeHealthProvider {
    private static final System.Logger LOGGER = System.getLogger(BackfillFetcher.class.getName());

    /** Metric for Number of retries during the backfill process. */
    private final Counter backfillRetries;
    /** Source of block node configurations. */
    private final BackfillSource blockNodeSource;
    /**
     * Maximum number of retries to fetch blocks from a block node.
     * This is used to avoid infinite loops in case of persistent failures.
     */
    private final int maxRetries;
    /**
     * Initial delay in milliseconds before retrying to fetch blocks from a block node.
     * This is used for exponential backoff in case of failures.
     */
    private final int initialRetryDelayMs;
    /** Global timeout in milliseconds for gRPC calls to block nodes (used as fallback). */
    private final int globalGrpcTimeoutMs;
    /** Enable TLS for secure connections to block nodes. */
    private final boolean enableTls;
    /** Maximum backoff duration in milliseconds (configurable). */
    private final long maxBackoffMs;
    /** Health penalty per failure for scoring (configurable). */
    private final double healthPenaltyPerFailure;
    /** Strategy for selecting nodes. */
    private final NodeSelectionStrategy selectionStrategy;
    /**
     * Map of BackfillSourceConfig to BlockNodeClient instances.
     * This allows us to reuse clients for the same node configuration.
     * Package-private for testing.
     */
    final ConcurrentHashMap<BackfillSourceConfig, BlockNodeClient> nodeClientMap = new ConcurrentHashMap<>();
    /** Per-source health for backoff and simple scoring. */
    private final ConcurrentHashMap<BackfillSourceConfig, SourceHealth> healthMap = new ConcurrentHashMap<>();

    /**
     * Constructor for the fetcher responsible for retrieving blocks from peer block nodes.
     *
     * @param backfillSource the backfill source configuration
     * @param config the backfill configuration containing retry, timeout, and other settings
     * @param metrics the metrics holder
     */
    public BackfillFetcher(
            BackfillSource backfillSource,
            BackfillConfiguration config,
            @NonNull BackfillPlugin.MetricsHolder metrics) {
        this.blockNodeSource = backfillSource;
        this.maxRetries = config.maxRetries();
        this.initialRetryDelayMs = config.initialRetryDelay();
        this.backfillRetries = metrics.backfillRetries();
        this.globalGrpcTimeoutMs = config.grpcOverallTimeout();
        this.enableTls = config.enableTLS();
        this.maxBackoffMs = config.maxBackoffMs();
        this.healthPenaltyPerFailure = config.healthPenaltyPerFailure();
        this.selectionStrategy = new PriorityHealthBasedStrategy(this);

        final String nodeConfigLogMsg = "Node: [{0}] ({1}) Address: [{2}], Port: [{3}], Priority: [{4}], Tuning: {5}";
        for (BackfillSourceConfig node : blockNodeSource.nodes()) {
            GrpcWebClientTuning tuning = node.grpcWebclientTuning();
            LOGGER.log(
                    INFO,
                    nodeConfigLogMsg,
                    node.nodeId() == 0 ? "n/a" : node.nodeId(),
                    node.name().isBlank() ? "unnamed" : node.name(),
                    node.address(),
                    node.port(),
                    node.priority(),
                    tuning != null ? "custom" : "defaults");
        }
    }

    /**
     * Determines the new available block range across all configured block nodes,
     * starting from the latest stored block number + 1 and ending at the maximum
     * last available block number reported by any of the nodes.
     *
     * @param latestStoredBlockNumber the latest stored block number
     * @return a LongRange representing the new available block range
     */
    public LongRange getNewAvailableRange(long latestStoredBlockNumber) {
        long earliestPeerBlock = Long.MAX_VALUE;
        long latestPeerBlock = Long.MIN_VALUE;

        for (BackfillSourceConfig node : blockNodeSource.nodes()) {
            BlockNodeClient currentNodeClient = getNodeClient(node);
            if (currentNodeClient == null || !currentNodeClient.isNodeReachable()) {
                final String unableToReachNodeMsg = "Unable to reach node [{0}], skipping";
                LOGGER.log(INFO, unableToReachNodeMsg, node);
                continue;
            }

            try {
                final ServerStatusResponse nodeStatus =
                        currentNodeClient.getBlockNodeServiceClient().serverStatus(new ServerStatusRequest());
                long firstAvailableBlock = nodeStatus.firstAvailableBlock();
                long lastAvailableBlock = nodeStatus.lastAvailableBlock();

                // update the earliestPeerBlock to the max lastAvailableBlock
                latestPeerBlock = Math.max(latestPeerBlock, lastAvailableBlock);
                earliestPeerBlock = Math.min(earliestPeerBlock, firstAvailableBlock);
            } catch (RuntimeException e) {
                final String failedToGetStatusMsg = "Failed to get status from node [%s:%d]: %s"
                        .formatted(node.address(), node.port(), e.getMessage());
                LOGGER.log(INFO, failedToGetStatusMsg, e);
                markFailure(node);
            }
        }

        // Determine the earliest block we can actually fetch from peers
        long startBlock = Math.max(latestStoredBlockNumber + 1, earliestPeerBlock);
        // confirm next block is available if not we still can't backfill
        if (startBlock > latestPeerBlock) {
            return null;
        }

        final String determinedAvailableRangeMsg =
                "Determined available range from peer blocks nodes start=[{0}] to end=[{1}]";
        LOGGER.log(TRACE, determinedAvailableRangeMsg, startBlock, latestPeerBlock);
        return new LongRange(startBlock, latestPeerBlock);
    }

    /**
     * Determine available ranges for a node. Once serverStatusDetail is available, this method could return multiple
     * ranges; today it returns a single contiguous range from serverStatus.
     */
    protected List<LongRange> resolveAvailableRanges(BlockNodeClient node) {
        final ServerStatusResponse nodeStatus =
                node.getBlockNodeServiceClient().serverStatus(new ServerStatusRequest());
        return List.of(new LongRange(nodeStatus.firstAvailableBlock(), nodeStatus.lastAvailableBlock()));
    }

    /**
     * Returns a BlockNodeClient for the given BackfillSourceConfig.
     * If a client for the node already exists, it returns that client.
     * Otherwise, it creates a new client and stores it in the map.
     * <p>
     * Per-node gRPC tuning (timeouts, HTTP/2 settings, buffer sizes) is passed
     * to the client. When tuning values are 0 or not specified, the global
     * timeout from BackfillConfiguration is used as fallback.
     *
     * @param node the BackfillSourceConfig to get the client for
     * @return a BlockNodeClient for the specified node
     */
    protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
        return nodeClientMap.computeIfAbsent(
                node, n -> new BlockNodeClient(n, globalGrpcTimeoutMs, enableTls, n.grpcWebclientTuning()));
    }

    /**
     * Perform a serverStatus call per configured node and compute the available ranges intersecting the target.
     *
     * @param targetRange overall gap we are trying to backfill
     * @return map of node -> available range overlapping the target
     */
    public Map<BackfillSourceConfig, List<LongRange>> getAvailabilityForRange(LongRange targetRange) {
        Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();

        for (BackfillSourceConfig node : blockNodeSource.nodes()) {
            if (isInBackoff(node)) {
                continue;
            }
            BlockNodeClient currentNodeClient = getNodeClient(node);
            if (currentNodeClient == null || !currentNodeClient.isNodeReachable()) {
                markFailure(node);
                continue;
            }

            List<LongRange> ranges;
            try {
                ranges = resolveAvailableRanges(currentNodeClient);
            } catch (RuntimeException e) {
                final String failedToResolveRangesMsg = "Failed to resolve available ranges from node [{0}]: {1}";
                LOGGER.log(INFO, failedToResolveRangesMsg, node, e.getMessage());
                markFailure(node);
                continue;
            }

            List<LongRange> intersections = new ArrayList<>();
            for (LongRange range : ranges) {
                long intersectionStart = Math.max(targetRange.start(), range.start());
                long intersectionEnd = Math.min(targetRange.end(), range.end());
                if (intersectionStart <= intersectionEnd) {
                    intersections.add(new LongRange(intersectionStart, intersectionEnd));
                }
            }

            if (!intersections.isEmpty()) {
                availability.put(node, LongRange.mergeContiguousRanges(intersections));
            } else {
                markFailure(node);
            }
        }

        return availability;
    }

    /**
     * Selects the best node and chunk to fetch next, based on pre-computed availability.
     * Delegates to the configured NodeSelectionStrategy.
     *
     * @param startBlock the next block number to fetch
     * @param gapEnd     inclusive end of the gap
     * @param availability map of node -> available range intersecting the gap
     * @return optional NodeSelection describing which node to hit and what range to request
     */
    public Optional<NodeSelectionStrategy.NodeSelection> selectNextChunk(
            long startBlock, long gapEnd, @NonNull Map<BackfillSourceConfig, List<LongRange>> availability) {
        return selectionStrategy.select(startBlock, gapEnd, availability);
    }

    /**
     * Fetch blocks for the provided range from the selected node using retries, without iterating other nodes.
     */
    public List<BlockUnparsed> fetchBlocksFromNode(BackfillSourceConfig nodeConfig, LongRange blockRange) {
        BlockNodeClient currentNodeClient = getNodeClient(nodeConfig);

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                long startNanos = System.nanoTime();
                List<BlockUnparsed> batch = currentNodeClient
                        .getBlockstreamSubscribeUnparsedClient()
                        .getBatchOfBlocks(blockRange.start(), blockRange.end());
                if (batch.size() != blockRange.size()) {
                    markFailure(nodeConfig);
                    return Collections.emptyList();
                }
                markSuccess(nodeConfig, System.nanoTime() - startNanos);
                return batch;
            } catch (RuntimeException e) {
                final String failedToFetchBlocksMsg =
                        "Failed to fetch blocks [%s->%s] from node [%s] (attempt %d/%d): %s-%s"
                                .formatted(
                                        blockRange.start(),
                                        blockRange.end(),
                                        nodeConfig.address(),
                                        attempt,
                                        maxRetries,
                                        e.getMessage(),
                                        e.getCause());
                LOGGER.log(INFO, failedToFetchBlocksMsg, e);
                if (attempt == maxRetries) {
                    markFailure(nodeConfig);
                    // Only log exception stack trace on final failure to prevent log spam
                    LOGGER.log(TRACE, "Final failure stack trace:", e);
                } else {
                    long delay = Math.multiplyExact(initialRetryDelayMs, attempt);
                    try {
                        TimeUnit.MILLISECONDS.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    backfillRetries.increment();
                }
            }
        }

        final String allAttemptsExhaustedMsg = "All {0} attempts exhausted for blocks [{1}->{2}] from node [{3}]";
        LOGGER.log(
                TRACE, allAttemptsExhaustedMsg, maxRetries, blockRange.start(), blockRange.end(), nodeConfig.address());
        return Collections.emptyList();
    }

    @Override
    public boolean isInBackoff(BackfillSourceConfig node) {
        SourceHealth health = healthMap.get(node);
        if (health == null) {
            return false;
        }
        return System.currentTimeMillis() < health.nextAllowedMillis;
    }

    private void markFailure(BackfillSourceConfig node) {
        // Evict cached client so a fresh one is created after backoff expires
        nodeClientMap.remove(node);
        healthMap.compute(node, (n, h) -> {
            if (h == null) {
                return new SourceHealth(1, System.currentTimeMillis() + initialRetryDelayMs, 0, 0);
            }
            int failures = h.failures + 1;
            long base = (long) initialRetryDelayMs * (1L << Math.min(failures - 1, 10));
            long backoff = Math.min(base, maxBackoffMs);
            long nextAllowed = System.currentTimeMillis() + backoff;
            return new SourceHealth(failures, nextAllowed, h.successes, h.totalLatencyNanos);
        });
    }

    private void markSuccess(BackfillSourceConfig node, long latencyNanos) {
        healthMap.compute(node, (n, h) -> {
            if (h == null) {
                return new SourceHealth(0, 0, 1, latencyNanos);
            }
            long successes = h.successes + 1;
            long totalLatency = h.totalLatencyNanos + latencyNanos;
            return new SourceHealth(0, 0, successes, totalLatency);
        });
    }

    @Override
    public double healthScore(BackfillSourceConfig node) {
        SourceHealth h = healthMap.get(node);
        if (h == null) {
            return 0.0;
        }
        double failurePenalty = h.failures * healthPenaltyPerFailure;
        double latencyPenaltyMs = h.successes > 0 ? (h.totalLatencyNanos / (double) h.successes) / 1_000_000.0 : 0;
        return failurePenalty + latencyPenaltyMs;
    }

    /**
     * Resets the health tracking for all nodes, clearing failure counts and backoff times.
     * Also clears cached clients so fresh connections are established on the next cycle.
     */
    public void resetHealth() {
        healthMap.clear();
        nodeClientMap.clear();
    }

    private record SourceHealth(int failures, long nextAllowedMillis, long successes, long totalLatencyNanos) {}
}
