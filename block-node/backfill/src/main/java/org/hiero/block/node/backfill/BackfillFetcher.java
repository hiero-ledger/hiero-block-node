// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.backfill.client.BackfillSource;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.backfill.client.BlockNodeClient;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * Client for fetching blocks from block nodes using gRPC.
 * This client handles retries and manages the status of block nodes.
 * It uses a round-robin approach to try different block nodes based on their priority.
 * It also maintains a map of node statuses to avoid hitting unavailable nodes repeatedly.
 * <p>
 * The client fetches blocks in a specified range and retries fetching from different nodes
 * if the initial node does not have the required blocks or is unavailable.
 * It also implements exponential backoff for retries to avoid overwhelming the nodes.
 * <p>
 * The client is initialized with a path to a block node preference file, which contains
 * */
public class BackfillFetcher {
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
    /** Connection timeout in milliseconds for gRPC calls to block nodes. */
    private final int connectionTimeoutSeconds;
    /** Enable TLS for secure connections to block nodes. */
    private final boolean enableTls;
    /** Current status of the Block Node Clients */
    private ConcurrentHashMap<BackfillSourceConfig, Status> nodeStatusMap = new ConcurrentHashMap<>();
    /**
     * Map of BackfillSourceConfig to BlockNodeClient instances.
     * This allows us to reuse clients for the same node configuration.
     */
    private ConcurrentHashMap<BackfillSourceConfig, BlockNodeClient> nodeClientMap = new ConcurrentHashMap<>();
    /** Per-source health for backoff and simple scoring. */
    private final ConcurrentHashMap<BackfillSourceConfig, SourceHealth> healthMap = new ConcurrentHashMap<>();

    /**
     * Constructor for the fetcher responsible for retrieving blocks from peer block nodes.
     *
     * @param blockNodePreferenceFilePath the path to the block node preference file
     */
    public BackfillFetcher(
            Path blockNodePreferenceFilePath,
            int maxRetries,
            Counter backfillRetriesCounter,
            int retryInitialDelayMs,
            int connectionTimeoutSeconds,
            boolean enableTls)
            throws IOException, ParseException {
        this.blockNodeSource = BackfillSource.JSON.parse(Bytes.wrap(Files.readAllBytes(blockNodePreferenceFilePath)));
        this.maxRetries = maxRetries;
        this.initialRetryDelayMs = retryInitialDelayMs;
        this.backfillRetries = backfillRetriesCounter;
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        this.enableTls = enableTls;

        for (BackfillSourceConfig node : blockNodeSource.nodes()) {
            LOGGER.log(INFO, "Address: {0}, Port: {1}, Priority: {2}", node.address(), node.port(), node.priority());
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
                // to-do: add logic to retry node later to avoid marking it unavailable forever
                nodeStatusMap.put(node, Status.UNAVAILABLE);
                LOGGER.log(INFO, "Unable to reach node {0}, marked as unavailable", node);
                continue;
            }

            final ServerStatusResponse nodeStatus =
                    currentNodeClient.getBlockNodeServiceClient().serverStatus(new ServerStatusRequest());
            long firstAvailableBlock = nodeStatus.firstAvailableBlock();
            long lastAvailableBlock = nodeStatus.lastAvailableBlock();

            // update the earliestPeerBlock to the max lastAvailableBlock
            latestPeerBlock = Math.max(latestPeerBlock, lastAvailableBlock);
            earliestPeerBlock = Math.min(earliestPeerBlock, firstAvailableBlock);
        }

        LOGGER.log(
                TRACE,
                "Determined block range from peer blocks nodes earliestPeerBlock={0,number,#} to latestStoredBlockNumber={1,number,#}",
                earliestPeerBlock,
                latestPeerBlock);

        // Determine the earliest block we can actually fetch from peers
        long startBlock = Math.max(latestStoredBlockNumber + 1, earliestPeerBlock);
        // confirm next block is available if not we still can't backfill
        if (startBlock > latestPeerBlock) {
            return null;
        }

        LOGGER.log(
                INFO,
                "Determined available range from peer blocks nodes start={0,number,#} to end={1,number,#}",
                startBlock,
                latestPeerBlock);
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
     *
     * @param node the BackfillSourceConfig to get the client for
     * @return a BlockNodeClient for the specified node
     */
    protected BlockNodeClient getNodeClient(BackfillSourceConfig node) {
        return nodeClientMap.computeIfAbsent(
                node, BlockNodeClient -> new BlockNodeClient(node, connectionTimeoutSeconds, enableTls));
    }

    /**
     * Resets the status of all block nodes to UNKNOWN.
     * This is useful for scenarios where the status of nodes may change,
     * such as after a network outage or when nodes are restarted.
     */
    public void resetStatus() {
        for (BackfillSourceConfig node : blockNodeSource.nodes()) {
            nodeStatusMap.put(node, Status.UNKNOWN);
        }
        healthMap.clear();
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
                nodeStatusMap.put(node, Status.UNAVAILABLE);
                markFailure(node);
                continue;
            }

            List<LongRange> ranges = resolveAvailableRanges(currentNodeClient);

            List<LongRange> intersections = new ArrayList<>();
            for (LongRange range : ranges) {
                long intersectionStart = Math.max(targetRange.start(), range.start());
                long intersectionEnd = Math.min(targetRange.end(), range.end());
                if (intersectionStart <= intersectionEnd) {
                    intersections.add(new LongRange(intersectionStart, intersectionEnd));
                }
            }

            if (!intersections.isEmpty()) {
                availability.put(node, intersections);
                nodeStatusMap.put(node, Status.AVAILABLE);
            } else {
                nodeStatusMap.put(node, Status.UNAVAILABLE);
                markFailure(node);
            }
        }

        return availability;
    }

    /**
     * Selects the best node and chunk to fetch next, based on pre-computed availability.
     *
     * @param startBlock the next block number to fetch
     * @param gapEnd     inclusive end of the gap
     * @param availability map of node -> available range intersecting the gap
     * @return optional NodeSelection describing which node to hit and what range to request
     */
    public Optional<NodeSelection> selectNextChunk(
            long startBlock, long gapEnd, @NonNull Map<BackfillSourceConfig, List<LongRange>> availability) {
        if (startBlock > gapEnd) {
            return Optional.empty();
        }

        OptionalLong earliestAvailableStart = findEarliestAvailableStart(startBlock, gapEnd, availability);
        if (earliestAvailableStart.isEmpty()) {
            return Optional.empty();
        }

        List<NodeSelection> candidates =
                candidatesForEarliest(startBlock, gapEnd, earliestAvailableStart.getAsLong(), availability);
        return chooseBestCandidate(candidates);
    }

    /** Locate the earliest start index across all available ranges that can satisfy the request bounds. */
    private OptionalLong findEarliestAvailableStart(
            long startBlock, long gapEnd, @NonNull Map<BackfillSourceConfig, List<LongRange>> availability) {
        long earliestAvailableStart = Long.MAX_VALUE;
        for (Map.Entry<BackfillSourceConfig, List<LongRange>> entry : availability.entrySet()) {
            for (LongRange availableRange : entry.getValue()) {
                long candidateStart = Math.max(startBlock, availableRange.start());
                if (candidateStart > availableRange.end() || candidateStart > gapEnd) {
                    continue;
                }
                earliestAvailableStart = Math.min(earliestAvailableStart, candidateStart);
            }
        }

        if (earliestAvailableStart == Long.MAX_VALUE) {
            return OptionalLong.empty();
        }

        return OptionalLong.of(earliestAvailableStart);
    }

    /** Build the list of nodes that can serve the earliest available start. */
    private List<NodeSelection> candidatesForEarliest(
            long startBlock,
            long gapEnd,
            long earliestAvailableStart,
            @NonNull Map<BackfillSourceConfig, List<LongRange>> availability) {
        List<NodeSelection> candidates = new ArrayList<>();
        for (Map.Entry<BackfillSourceConfig, List<LongRange>> entry : availability.entrySet()) {
            for (LongRange availableRange : entry.getValue()) {
                long candidateStart = Math.max(startBlock, availableRange.start());
                if (candidateStart != earliestAvailableStart) {
                    continue;
                }
                if (candidateStart > availableRange.end() || candidateStart > gapEnd) {
                    continue;
                }
                candidates.add(new NodeSelection(entry.getKey(), candidateStart));
            }
        }
        return candidates;
    }

    /** Choose the lowest-priority candidate (tie-breaking randomly) from the supplied list. */
    private Optional<NodeSelection> chooseBestCandidate(@NonNull List<NodeSelection> candidates) {
        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        int bestPriority = candidates.stream()
                .mapToInt(selection -> selection.nodeConfig().priority())
                .min()
                .orElse(Integer.MAX_VALUE);
        List<NodeSelection> bestPriorityCandidates = candidates.stream()
                .filter(c -> c.nodeConfig().priority() == bestPriority)
                .filter(c -> !isInBackoff(c.nodeConfig()))
                .toList();

        if (bestPriorityCandidates.size() == 1) {
            return Optional.of(bestPriorityCandidates.getFirst());
        }

        if (bestPriorityCandidates.isEmpty()) {
            return Optional.empty();
        }

        double bestScore = bestPriorityCandidates.stream()
                .mapToDouble(c -> healthScore(c.nodeConfig()))
                .min()
                .orElse(Double.MAX_VALUE);
        List<NodeSelection> bestHealth = bestPriorityCandidates.stream()
                .filter(c -> Double.compare(healthScore(c.nodeConfig()), bestScore) == 0)
                .toList();

        if (bestHealth.size() == 1) {
            return Optional.of(bestHealth.getFirst());
        }

        int chosenIndex = ThreadLocalRandom.current().nextInt(bestHealth.size());
        return Optional.of(bestHealth.get(chosenIndex));
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
                    nodeStatusMap.put(nodeConfig, Status.UNAVAILABLE);
                    markFailure(nodeConfig);
                    return Collections.emptyList();
                }
                markSuccess(nodeConfig, System.nanoTime() - startNanos);
                return batch;
            } catch (Exception e) {
                if (attempt == maxRetries) {
                    nodeStatusMap.put(nodeConfig, Status.UNAVAILABLE);
                    markFailure(nodeConfig);
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

        return Collections.emptyList();
    }

    public record NodeSelection(BackfillSourceConfig nodeConfig, long startBlock) {}

    private boolean isInBackoff(BackfillSourceConfig node) {
        SourceHealth health = healthMap.get(node);
        if (health == null) {
            return false;
        }
        return System.currentTimeMillis() < health.nextAllowedMillis;
    }

    private void markFailure(BackfillSourceConfig node) {
        healthMap.compute(node, (n, h) -> {
            if (h == null) {
                return new SourceHealth(1, System.currentTimeMillis() + initialRetryDelayMs, 0, 0);
            }
            int failures = h.failures + 1;
            long backoff = (long) initialRetryDelayMs * Math.max(1, failures);
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

    private double healthScore(BackfillSourceConfig node) {
        SourceHealth h = healthMap.get(node);
        if (h == null) {
            return 0.0;
        }
        double failurePenalty = h.failures * 1000.0;
        double latencyPenaltyMs = h.successes > 0 ? (h.totalLatencyNanos / (double) h.successes) / 1_000_000.0 : 0;
        return failurePenalty + latencyPenaltyMs;
    }

    /**
     * Enum representing the status of a block node:
     * <ul>
     *     <li>UNKNOWN: The status of the node is unknown.</li>
     *     <li>AVAILABLE: The node is available and can serve requests.</li>
     *     <li>UNAVAILABLE: The node is not available, either due to an error or because it does not have the requested blocks.</li>
     * </ul>>
     */
    public enum Status {
        UNKNOWN,
        AVAILABLE,
        UNAVAILABLE
    }

    private record SourceHealth(int failures, long nextAllowedMillis, long successes, long totalLatencyNanos) {}
}
