// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.internal.BlockNodeSource;
import org.hiero.block.internal.BlockNodeSourceConfig;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.client.BlockNodeClient;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.metrics.LongCounter;

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
    private final LongCounter.Measurement backfillRetries;
    /** Source of block node configurations. */
    private final BlockNodeSource blockNodeSource;
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
    /** Maximum incoming buffer size in bytes for the gRPC client. */
    private final int maxIncomingBufferSize;
    /** Maximum protobuf message size in bytes accepted while parsing a fetched block. */
    private final int maxProtobufMessageSizeBytes;
    /** Maximum backoff duration in milliseconds (configurable). */
    private final long maxBackoffMs;
    /** Health penalty per failure for scoring (configurable). */
    private final double healthPenaltyPerFailure;
    /** Strategy for selecting nodes. */
    private final NodeSelectionStrategy selectionStrategy;
    /**
     * Map of BlockNodeSourceConfig to BlockNodeClient instances.
     * This allows us to reuse clients for the same node configuration.
     * Package-private for testing.
     */
    final ConcurrentHashMap<BlockNodeSourceConfig, BlockNodeClient> nodeClientMap = new ConcurrentHashMap<>();
    /**
     * Per-source health for backoff and simple scoring.
     * Package-private for testing.
     */
    final ConcurrentHashMap<BlockNodeSourceConfig, SourceHealth> healthMap = new ConcurrentHashMap<>();

    /**
     * Constructor for the fetcher responsible for retrieving blocks from peer block nodes.
     *
     * @param backfillSource the backfill source configuration
     * @param config the backfill configuration containing retry, timeout, and other settings
     * @param metrics the metrics holder
     */
    public BackfillFetcher(
            BlockNodeSource backfillSource,
            BackfillConfiguration config,
            @NonNull BackfillPlugin.MetricsHolder metrics) {
        this.blockNodeSource = backfillSource;
        this.maxRetries = config.maxRetries();
        this.initialRetryDelayMs = config.initialRetryDelay();
        this.backfillRetries = metrics.backfillRetries();
        this.globalGrpcTimeoutMs = config.grpcOverallTimeout();
        this.enableTls = config.enableTLS();
        this.maxIncomingBufferSize = config.maxIncomingBufferSize();
        this.maxProtobufMessageSizeBytes = config.maxProtobufMessageSizeBytes();
        this.maxBackoffMs = config.maxBackoffMs();
        this.healthPenaltyPerFailure = config.healthPenaltyPerFailure();
        this.selectionStrategy = new PriorityHealthBasedStrategy(this);

        for (BlockNodeSourceConfig node : blockNodeSource.nodes()) {
            LOGGER.log(INFO, "Loaded backfill source node: {0}", node);
        }
    }

    /**
     * Determines the block ranges available across all configured block nodes that extend beyond the
     * given baseline. Because peers can have non-contiguous history, this returns the union of their
     * exact available ranges (each clipped to blocks after the baseline) rather than a single span:
     * callers must not assume every block between the first and last range is available.
     *
     * @param latestStoredBlockNumber the highest block already known locally; only blocks beyond it are returned
     * @return the merged, ascending list of available peer ranges beyond the baseline; empty if none
     */
    public List<LongRange> getNewAvailableRanges(long latestStoredBlockNumber) {
        final List<LongRange> peerRanges = new ArrayList<>();

        for (BlockNodeSourceConfig node : blockNodeSource.nodes()) {
            if (isInBackoff(node)) {
                LOGGER.log(DEBUG, "Node [{0}] is in backoff, skipping range discovery", node.address());
                continue;
            }
            BlockNodeClient currentNodeClient = getNodeClient(node);
            if (currentNodeClient == null || !currentNodeClient.isNodeReachable()) {
                final String unableToReachNodeMsg = "Unable to reach node [{0}], skipping";
                LOGGER.log(INFO, unableToReachNodeMsg, node);
                continue;
            }

            try {
                for (final LongRange range : resolveAvailableRanges(currentNodeClient)) {
                    if (range.end() > latestStoredBlockNumber) {
                        final long start = Math.max(range.start(), latestStoredBlockNumber + 1);
                        peerRanges.add(new LongRange(start, range.end()));
                    }
                }
            } catch (RuntimeException e) {
                final String failedToGetStatusMsg = "Failed to get status from node [%s:%d]: %s"
                        .formatted(node.address(), node.port(), e.getMessage());
                LOGGER.log(INFO, failedToGetStatusMsg, e);
                markFailure(node);
            }
        }

        final List<LongRange> mergedRanges = LongRange.mergeContiguousRanges(peerRanges);
        final String determinedAvailableRangeMsg = "Determined available peer ranges beyond block [{0}]: [{1}]";
        LOGGER.log(DEBUG, determinedAvailableRangeMsg, latestStoredBlockNumber, mergedRanges);
        return mergedRanges;
    }

    /**
     * Determine the exact available block ranges for a node using the {@code serverStatusDetail} RPC.
     * Unlike the coarse {@code serverStatus} first/last pair, this reports each contiguous range the peer
     * has, so internal gaps in the peer's history are visible and never requested. Malformed ranges are
     * skipped defensively.
     *
     * @param node the peer client to query
     * @return the peer's available ranges, sorted ascending; empty if the peer reports none
     */
    protected List<LongRange> resolveAvailableRanges(BlockNodeClient node) {
        final ServerStatusDetailResponse nodeStatus =
                node.getBlockNodeServiceClient().serverStatusDetail(new ServerStatusRequest());
        final List<LongRange> ranges =
                new ArrayList<>(nodeStatus.availableRanges().size());
        for (final BlockRange range : nodeStatus.availableRanges()) {
            if (range.rangeStart() < 0 || range.rangeEnd() < range.rangeStart()) {
                continue;
            }
            ranges.add(new LongRange(range.rangeStart(), range.rangeEnd()));
        }
        return ranges;
    }

    /**
     * Returns a BlockNodeClient for the given BlockNodeSourceConfig.
     * If a client for the node already exists, it returns that client.
     * Otherwise, it creates a new client and stores it in the map.
     * <p>
     * Per-node gRPC tuning (timeouts, HTTP/2 settings, buffer sizes) is passed
     * to the client. When tuning values are 0 or not specified, the global
     * timeout from BackfillConfiguration is used as fallback.
     *
     * @param node the BlockNodeSourceConfig to get the client for
     * @return a BlockNodeClient for the specified node
     */
    protected BlockNodeClient getNodeClient(BlockNodeSourceConfig node) {
        // Check if existing client is unreachable and remove it to allow recreation
        BlockNodeClient existingClient = nodeClientMap.get(node);
        if (existingClient != null && !existingClient.isNodeReachable()) {
            nodeClientMap.remove(node);
            LOGGER.log(DEBUG, "Removed unreachable client for node [{0}], will attempt to recreate", node.address());
        }
        return nodeClientMap.computeIfAbsent(
                node,
                n -> new BlockNodeClient(
                        n,
                        globalGrpcTimeoutMs,
                        enableTls,
                        maxIncomingBufferSize,
                        maxProtobufMessageSizeBytes,
                        n.grpcWebclientTuning()));
    }

    /**
     * Perform a serverStatus call per configured node and compute the available ranges intersecting the target.
     *
     * @param targetRange overall gap we are trying to backfill
     * @return map of node -> available range overlapping the target
     */
    public Map<BlockNodeSourceConfig, List<LongRange>> getAvailabilityForRange(LongRange targetRange) {
        Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();

        for (BlockNodeSourceConfig node : blockNodeSource.nodes()) {
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

            // A peer whose exact ranges do not cover this chunk is not a failure — with serverStatusDetail
            // reporting real ranges, "I don't have those blocks" is a precise, truthful answer. Only omit
            // it from the availability map; unreachable/erroring peers are still penalised above.
            if (!intersections.isEmpty()) {
                availability.put(node, LongRange.mergeContiguousRanges(intersections));
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
            long startBlock, long gapEnd, @NonNull Map<BlockNodeSourceConfig, List<LongRange>> availability) {
        return selectionStrategy.select(startBlock, gapEnd, availability);
    }

    /**
     * Fetch blocks for the provided range from the selected node using retries, without iterating other nodes.
     */
    public List<BlockUnparsed> fetchBlocksFromNode(BlockNodeSourceConfig nodeConfig, LongRange blockRange) {
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
                        "Failed to fetch blocks [{0}->{1}] from node [{2}] (attempt {3}/{4}) due to {5} - {6}.";
                final String cause = e.getCause() != null ? ", " + e.getCause() : "";
                LOGGER.log(
                        DEBUG,
                        failedToFetchBlocksMsg,
                        blockRange.start(),
                        blockRange.end(),
                        nodeConfig.address(),
                        attempt,
                        maxRetries,
                        e.toString(),
                        cause);
                if (attempt == maxRetries) {
                    markFailure(nodeConfig);
                    // Log exception details on final failure for debugging
                    LOGGER.log(WARNING, "Final failure stack trace.\n", e);
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
                DEBUG, allAttemptsExhaustedMsg, maxRetries, blockRange.start(), blockRange.end(), nodeConfig.address());
        return Collections.emptyList();
    }

    @Override
    public boolean isInBackoff(BlockNodeSourceConfig node) {
        SourceHealth health = healthMap.get(node);
        if (health == null) {
            return false;
        }
        return System.currentTimeMillis() < health.nextAllowedMillis;
    }

    private void markFailure(BlockNodeSourceConfig node) {
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

    private void markSuccess(BlockNodeSourceConfig node, long latencyNanos) {
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
    public double healthScore(BlockNodeSourceConfig node) {
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

    /** Package-private for testing. */
    record SourceHealth(int failures, long nextAllowedMillis, long successes, long totalLatencyNanos) {}
}
