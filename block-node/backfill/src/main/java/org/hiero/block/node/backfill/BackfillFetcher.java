// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.hiero.block.node.base.client.BlockStreamSubscribeUnparsedClient.BatchResult;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
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
public class BackfillFetcher implements PriorityHealthBasedStrategy.NodeHealthProvider, AutoCloseable {
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
     * <p>
     * This only locates the peer network's frontier beyond the baseline (used to size the scan
     * window in greedy mode); it does not decide which gaps get backfilled. Actual chunk fetching,
     * including gaps below the baseline, is resolved independently via {@link #getAvailabilityForRange},
     * which is not clipped to any baseline.
     *
     * @param latestStoredBlockNumber the highest block already known locally; only blocks beyond it are returned
     * @return the merged, ascending list of available peer ranges beyond the baseline; empty if none
     */
    public List<BlockRange> getNewAvailableRanges(long latestStoredBlockNumber) {
        final List<BlockRange> peerRanges = new ArrayList<>();
        final int totalNodes = blockNodeSource.nodes().size();
        int reachedNodeCount = 0;

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
                for (final BlockRange range : resolveAvailableRanges(currentNodeClient)) {
                    if (range.rangeEnd() > latestStoredBlockNumber) {
                        final long start = Math.max(range.rangeStart(), latestStoredBlockNumber + 1);
                        peerRanges.add(BlockRangeSet.of(start, range.rangeEnd()));
                    }
                }
                reachedNodeCount++;
            } catch (RuntimeException e) {
                final String failedToGetStatusMsg = "Failed to get status from node [%s:%d]: %s"
                        .formatted(node.address(), node.port(), e.getMessage());
                LOGGER.log(DEBUG, failedToGetStatusMsg, e);
                markFailure(node);
            }
        }

        // Individual unreachable/failed nodes are DEBUG, but no reachable source at all means backfill
        // cannot progress: surface that aggregate condition once at WARNING for on-call visibility.
        if (totalNodes > 0 && reachedNodeCount == 0) {
            final String allSourcesUnreachableMsg =
                    "All [{0}] backfill source(s) are unreachable; cannot discover peer ranges";
            LOGGER.log(WARNING, allSourcesUnreachableMsg, totalNodes);
        }

        final List<BlockRange> mergedRanges = BlockRangeSet.mergeContiguousRanges(peerRanges);
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
    protected List<BlockRange> resolveAvailableRanges(BlockNodeClient node) {
        final ServerStatusDetailResponse nodeStatus =
                node.getBlockNodeServiceClient().serverStatusDetail(new ServerStatusRequest());
        final List<BlockRange> ranges =
                new ArrayList<>(nodeStatus.availableRanges().size());
        for (final BlockRange range : nodeStatus.availableRanges()) {
            if (range.rangeStart() < 0 || range.rangeEnd() < range.rangeStart()) {
                continue;
            }
            ranges.add(BlockRangeSet.of(range.rangeStart(), range.rangeEnd()));
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
        // Atomic check-close-construct: avoids the window a separate get()+remove()+
        // computeIfAbsent() would leave between evicting an unreachable client and
        // constructing its replacement.
        return nodeClientMap.compute(node, (key, current) -> {
            if (current != null) {
                if (current.isNodeReachable()) {
                    return current;
                }
                closeQuietly(key, current);
                LOGGER.log(DEBUG, "Removed unreachable client for node [{0}], will attempt to recreate", key.address());
            }
            return new BlockNodeClient(
                    key,
                    globalGrpcTimeoutMs,
                    enableTls,
                    maxIncomingBufferSize,
                    maxProtobufMessageSizeBytes,
                    key.grpcWebclientTuning());
        });
    }

    /**
     * Perform a serverStatus call per configured node and compute the available ranges intersecting the target.
     *
     * @param targetRange overall gap we are trying to backfill
     * @return map of node -> available range overlapping the target
     */
    public Map<BlockNodeSourceConfig, List<BlockRange>> getAvailabilityForRange(BlockRange targetRange) {
        Map<BlockNodeSourceConfig, List<BlockRange>> availability = new HashMap<>();

        for (BlockNodeSourceConfig node : blockNodeSource.nodes()) {
            if (isInBackoff(node)) {
                continue;
            }
            BlockNodeClient currentNodeClient = getNodeClient(node);
            if (currentNodeClient == null || !currentNodeClient.isNodeReachable()) {
                markFailure(node);
                continue;
            }

            List<BlockRange> ranges;
            try {
                ranges = resolveAvailableRanges(currentNodeClient);
            } catch (RuntimeException e) {
                final String failedToResolveRangesMsg = "Failed to resolve available ranges from node [{0}]: {1}";
                LOGGER.log(INFO, failedToResolveRangesMsg, node, e.getMessage());
                markFailure(node);
                continue;
            }

            List<BlockRange> intersections = new ArrayList<>();
            for (BlockRange range : ranges) {
                long intersectionStart = Math.max(targetRange.rangeStart(), range.rangeStart());
                long intersectionEnd = Math.min(targetRange.rangeEnd(), range.rangeEnd());
                if (intersectionStart <= intersectionEnd) {
                    intersections.add(BlockRangeSet.of(intersectionStart, intersectionEnd));
                }
            }

            // A peer whose exact ranges do not cover this chunk is not a failure — with serverStatusDetail
            // reporting real ranges, "I don't have those blocks" is a precise, truthful answer. Only omit
            // it from the availability map; unreachable/erroring peers are still penalised above.
            if (!intersections.isEmpty()) {
                availability.put(node, BlockRangeSet.mergeContiguousRanges(intersections));
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
            long startBlock, long gapEnd, @NonNull Map<BlockNodeSourceConfig, List<BlockRange>> availability) {
        return selectionStrategy.select(startBlock, gapEnd, availability);
    }

    /**
     * Fetch blocks for the provided range from the selected node using retries, without iterating other nodes.
     * Each retry starts after the blocks already received, so a block is never requested twice.
     *
     * @return the blocks received, in order from the start of the range. This can be only part of the range,
     *         when the attempts ran out or the peer ended the stream early, or empty if nothing was received
     */
    public List<BlockUnparsed> fetchBlocksFromNode(BlockNodeSourceConfig nodeConfig, BlockRange blockRange) {
        final List<BlockUnparsed> received = new ArrayList<>();
        long fetchNanos = 0;
        Throwable lastError = null;
        boolean done = false;

        for (int attempt = 1; attempt <= maxRetries && !done; attempt++) {
            final long firstBlock = blockRange.rangeStart() + received.size();
            final long startNanos = System.nanoTime();
            Throwable error;
            try {
                final BatchResult result = getNodeClient(nodeConfig)
                        .getBlockstreamSubscribeUnparsedClient()
                        .getBatchOfBlocks(firstBlock, blockRange.rangeEnd());
                received.addAll(result.blocks());
                error = result.error();
            } catch (RuntimeException e) {
                error = e;
            }
            final long attemptNanos = System.nanoTime() - startNanos;
            fetchNanos += attemptNanos;
            lastError = error;
            final String attemptFinishedMsg =
                    "Attempt {0}/{1} for blocks [{2}->{3}] from node [{4}] took [{5}] ms, [{6}] blocks received so far";
            LOGGER.log(
                    DEBUG,
                    attemptFinishedMsg,
                    attempt,
                    maxRetries,
                    firstBlock,
                    blockRange.rangeEnd(),
                    nodeConfig.address(),
                    TimeUnit.NANOSECONDS.toMillis(attemptNanos),
                    received.size());

            if (received.size() == BlockRangeSet.size(blockRange)
                    || error == null
                    || error instanceof InterruptedException) {
                done = true;
            } else {
                final String failedToFetchBlocksMsg =
                        "Failed to fetch blocks [{0}->{1}] from node [{2}] (attempt {3}/{4}) due to {5} - {6}.";
                final String cause = error.getCause() != null ? ", " + error.getCause() : "";
                LOGGER.log(
                        DEBUG,
                        failedToFetchBlocksMsg,
                        firstBlock,
                        blockRange.rangeEnd(),
                        nodeConfig.address(),
                        attempt,
                        maxRetries,
                        error.toString(),
                        cause);
                if (attempt < maxRetries) {
                    long delay = Math.multiplyExact(initialRetryDelayMs, attempt);
                    try {
                        TimeUnit.MILLISECONDS.sleep(delay);
                        backfillRetries.increment();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        done = true;
                    }
                }
            }
        }

        recordFetchOutcome(nodeConfig, blockRange, received, fetchNanos, lastError);
        return received;
    }

    /**
     * Updates the health of {@code nodeConfig} after a fetch and logs a fetch that ended early with an error.
     *
     * @param received the blocks the fetch returns
     * @param fetchNanos the time of the attempts, without the retry delays between them
     * @param lastError the error of the last attempt, or {@code null} if it ended without one
     */
    private void recordFetchOutcome(
            BlockNodeSourceConfig nodeConfig,
            BlockRange blockRange,
            List<BlockUnparsed> received,
            long fetchNanos,
            Throwable lastError) {
        // An interruption comes from our side, typically a shutdown, so it says nothing about the peer.
        final boolean interrupted = lastError instanceof InterruptedException
                || Thread.currentThread().isInterrupted();
        if (received.size() == BlockRangeSet.size(blockRange)) {
            recordBlocksReceived(nodeConfig, fetchNanos, received.size(), true);
        } else {
            if (!received.isEmpty()) {
                recordBlocksReceived(nodeConfig, fetchNanos, received.size(), false);
            } else if (!interrupted) {
                markFailure(nodeConfig);
            }
            if (lastError != null) {
                final String fetchEndedEarlyMsg = "Fetch of blocks [%d->%d] from node [%s] ended with %d of %d blocks."
                        .formatted(
                                blockRange.rangeStart(),
                                blockRange.rangeEnd(),
                                nodeConfig.address(),
                                received.size(),
                                BlockRangeSet.size(blockRange));
                LOGGER.log(interrupted ? DEBUG : WARNING, fetchEndedEarlyMsg, lastError);
            }
        }
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
        removeAndClose(node);
        healthMap.compute(node, (n, h) -> {
            if (h == null) {
                return new SourceHealth(1, System.currentTimeMillis() + initialRetryDelayMs, 0, 0);
            }
            int failures = h.failures + 1;
            long base = (long) initialRetryDelayMs * (1L << Math.min(failures - 1, 10));
            long backoff = Math.min(base, maxBackoffMs);
            long nextAllowed = System.currentTimeMillis() + backoff;
            return new SourceHealth(failures, nextAllowed, h.blocksReceived, h.totalLatencyNanos);
        });
    }

    /** Removes and closes the cached client for {@code node}, if present. */
    private void removeAndClose(BlockNodeSourceConfig node) {
        BlockNodeClient removed = nodeClientMap.remove(node);
        if (removed != null) {
            closeQuietly(node, removed);
        }
    }

    /** Closes {@code client}, logging (not throwing) if it fails. */
    private void closeQuietly(BlockNodeSourceConfig node, BlockNodeClient client) {
        try {
            client.close();
        } catch (IOException e) {
            LOGGER.log(WARNING, "Unable to close BlockNodeClient [{0}]: {1}", node.address(), e);
        }
    }

    /**
     * Adds the time and block count of a fetch that received blocks to the health of {@code node}.
     *
     * @param latencyNanos the time of the attempts, without the retry delays between them
     * @param blocks the number of blocks received
     * @param fullRange whether the whole requested range arrived. Only then the failures are reset, so a peer
     *                  that keeps failing part way through keeps its failure history.
     */
    private void recordBlocksReceived(BlockNodeSourceConfig node, long latencyNanos, long blocks, boolean fullRange) {
        healthMap.compute(node, (n, h) -> {
            if (h == null) {
                return new SourceHealth(0, 0, blocks, latencyNanos);
            }
            long blocksReceived = h.blocksReceived + blocks;
            long totalLatency = h.totalLatencyNanos + latencyNanos;
            int failures = fullRange ? 0 : h.failures;
            long nextAllowedMillis = fullRange ? 0 : h.nextAllowedMillis;
            return new SourceHealth(failures, nextAllowedMillis, blocksReceived, totalLatency);
        });
    }

    @Override
    public double healthScore(BlockNodeSourceConfig node) {
        SourceHealth h = healthMap.get(node);
        if (h == null) {
            return 0.0;
        }
        double failurePenalty = h.failures * healthPenaltyPerFailure;
        double latencyPenaltyMs =
                h.blocksReceived > 0 ? (h.totalLatencyNanos / (double) h.blocksReceived) / 1_000_000.0 : 0;
        return failurePenalty + latencyPenaltyMs;
    }

    /**
     * Resets the health tracking for all nodes, clearing failure counts and backoff times.
     * Also closes and clears cached clients so fresh connections are established on the next cycle.
     */
    public void resetHealth() {
        healthMap.clear();
        for (BlockNodeSourceConfig node : nodeClientMap.keySet()) {
            removeAndClose(node);
        }
    }

    /** Package-private for testing. */
    record SourceHealth(int failures, long nextAllowedMillis, long blocksReceived, long totalLatencyNanos) {}

    /** Closes every cached client. Safe to call multiple times. */
    @Override
    public void close() throws IOException {
        for (Entry<BlockNodeSourceConfig, BlockNodeClient> entry : nodeClientMap.entrySet()) {
            closeQuietly(entry.getKey(), entry.getValue());
        }
    }
}
