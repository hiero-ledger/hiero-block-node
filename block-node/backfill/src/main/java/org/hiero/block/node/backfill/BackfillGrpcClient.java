// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
public class BackfillGrpcClient {
    private static final System.Logger LOGGER = System.getLogger(BackfillGrpcClient.class.getName());

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
    /** Cache of available ranges per node to avoid repeated serverStatus calls while chunking a gap. */
    private ConcurrentHashMap<BackfillSourceConfig, LongRange> nodeAvailableRangeCache = new ConcurrentHashMap<>();

    /**
     * Constructor for BackfillGrpcClient.
     *
     * @param blockNodePreferenceFilePath the path to the block node preference file
     */
    public BackfillGrpcClient(
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

        // confirm next block is available if not we still can't backfill
        if (latestStoredBlockNumber + 1 < earliestPeerBlock
                || latestStoredBlockNumber > latestPeerBlock
                || latestStoredBlockNumber + 1 > latestPeerBlock) {
            return null;
        }

        LOGGER.log(
                INFO,
                "Determined available range from peer blocks nodes start={0,number,#} to end={1,number,#}",
                latestStoredBlockNumber + 1,
                latestPeerBlock);
        return new LongRange(latestStoredBlockNumber + 1, latestPeerBlock);
    }

    /**
     * Returns a BlockNodeClient for the given BackfillSourceConfig.
     * If a client for the node already exists, it returns that client.
     * Otherwise, it creates a new client and stores it in the map.
     *
     * @param node the BackfillSourceConfig to get the client for
     * @return a BlockNodeClient for the specified node
     */
    private BlockNodeClient getNodeClient(BackfillSourceConfig node) {
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
            nodeAvailableRangeCache.remove(node);
        }
    }

    /**
     * Perform a single serverStatus call per configured node and cache the available ranges intersecting the target.
     *
     * @param targetRange overall gap we are trying to backfill
     * @return map of node -> available range overlapping the target
     */
    public Map<BackfillSourceConfig, LongRange> getAvailabilityForRange(LongRange targetRange) {
        Map<BackfillSourceConfig, LongRange> availability = new HashMap<>();

        for (BackfillSourceConfig node : blockNodeSource.nodes()) {
            BlockNodeClient currentNodeClient = getNodeClient(node);
            if (currentNodeClient == null || !currentNodeClient.isNodeReachable()) {
                nodeStatusMap.put(node, Status.UNAVAILABLE);
                continue;
            }

            final ServerStatusResponse nodeStatus =
                    currentNodeClient.getBlockNodeServiceClient().serverStatus(new ServerStatusRequest());
            long firstAvailableBlock = nodeStatus.firstAvailableBlock();
            long lastAvailableBlock = nodeStatus.lastAvailableBlock();
            final LongRange availableRange = new LongRange(firstAvailableBlock, lastAvailableBlock);
            nodeAvailableRangeCache.put(node, availableRange);

            long intersectionStart = Math.max(targetRange.start(), firstAvailableBlock);
            long intersectionEnd = Math.min(targetRange.end(), lastAvailableBlock);
            if (intersectionStart <= intersectionEnd) {
                availability.put(node, new LongRange(intersectionStart, intersectionEnd));
                nodeStatusMap.put(node, Status.AVAILABLE);
            } else {
                nodeStatusMap.put(node, Status.UNAVAILABLE);
            }
        }

        return availability;
    }

    /**
     * Selects the best node and chunk to fetch next, based on pre-computed availability.
     *
     * @param startBlock the next block number to fetch
     * @param gapEnd     inclusive end of the gap
     * @param batchSize  maximum number of blocks to request
     * @param availability map of node -> available range intersecting the gap
     * @param excludedNodes nodes temporarily marked as unavailable due to failures in this run
     * @return optional NodeSelection describing which node to hit and what range to request
     */
    public Optional<NodeSelection> selectNextChunk(
            long startBlock,
            long gapEnd,
            long batchSize,
            Map<BackfillSourceConfig, LongRange> availability,
            Set<BackfillSourceConfig> excludedNodes) {
        if (startBlock > gapEnd) {
            return Optional.empty();
        }

        long earliestAvailableStart = Long.MAX_VALUE;
        List<NodeSelection> candidates = new ArrayList<>();
        for (Map.Entry<BackfillSourceConfig, LongRange> entry : availability.entrySet()) {
            if (excludedNodes.contains(entry.getKey())) {
                continue;
            }
            LongRange availableRange = entry.getValue();
            long candidateStart = Math.max(startBlock, availableRange.start());
            if (candidateStart > availableRange.end() || candidateStart > gapEnd) {
                continue;
            }
            earliestAvailableStart = Math.min(earliestAvailableStart, candidateStart);
        }

        if (earliestAvailableStart == Long.MAX_VALUE) {
            return Optional.empty();
        }

        for (Map.Entry<BackfillSourceConfig, LongRange> entry : availability.entrySet()) {
            if (excludedNodes.contains(entry.getKey())) {
                continue;
            }
            LongRange availableRange = entry.getValue();
            long candidateStart = Math.max(startBlock, availableRange.start());
            if (candidateStart != earliestAvailableStart) {
                continue;
            }
            if (candidateStart > availableRange.end() || candidateStart > gapEnd) {
                continue;
            }
            long chunkEnd = Math.min(Math.min(candidateStart + batchSize - 1, availableRange.end()), gapEnd);
            candidates.add(new NodeSelection(entry.getKey(), new LongRange(candidateStart, chunkEnd)));
        }

        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        // Pick the lowest priority value (cost/bandwidth) among candidates covering the earliest available start
        int bestPriority = candidates.stream()
                .mapToInt(selection -> selection.nodeConfig().priority())
                .min()
                .orElse(Integer.MAX_VALUE);
        List<NodeSelection> bestPriorityCandidates = candidates.stream()
                .filter(c -> c.nodeConfig().priority() == bestPriority)
                .toList();

        if (bestPriorityCandidates.size() == 1) {
            return Optional.of(bestPriorityCandidates.getFirst());
        }

        // Tie on range + priority: pick randomly to distribute load.
        int chosenIndex = ThreadLocalRandom.current().nextInt(bestPriorityCandidates.size());
        return Optional.of(bestPriorityCandidates.get(chosenIndex));
    }

    /**
     * Fetch blocks for the provided range from the selected node using retries, without iterating other nodes.
     */
    public List<BlockUnparsed> fetchBlocksFromNode(BackfillSourceConfig nodeConfig, LongRange blockRange) {
        BlockNodeClient currentNodeClient = getNodeClient(nodeConfig);

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return currentNodeClient
                        .getBlockstreamSubscribeUnparsedClient()
                        .getBatchOfBlocks(blockRange.start(), blockRange.end());
            } catch (Exception e) {
                if (attempt == maxRetries) {
                    nodeStatusMap.put(nodeConfig, Status.UNAVAILABLE);
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

    public record NodeSelection(BackfillSourceConfig nodeConfig, LongRange chunkRange) {}

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
}
