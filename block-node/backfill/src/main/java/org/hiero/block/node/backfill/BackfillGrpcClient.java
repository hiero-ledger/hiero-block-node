// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.INFO;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.ServerStatusRequest;
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

    /** Current status of the Block Node Clients */
    private ConcurrentHashMap<BackfillSourceConfig, Status> nodeStatusMap = new ConcurrentHashMap<>();
    /**
     * Map of BackfillSourceConfig to BlockNodeClient instances.
     * This allows us to reuse clients for the same node configuration.
     */
    private ConcurrentHashMap<BackfillSourceConfig, BlockNodeClient> nodeClientMap = new ConcurrentHashMap<>();

    /**
     * Constructor for BackfillGrpcClient.
     *
     * @param blockNodePreferenceFilePath the path to the block node preference file
     */
    public BackfillGrpcClient(
            Path blockNodePreferenceFilePath, int maxRetries, Counter backfillRetriesCounter, int retryInitialDelayMs)
            throws IOException, ParseException {
        this.blockNodeSource = BackfillSource.JSON.parse(Bytes.wrap(Files.readAllBytes(blockNodePreferenceFilePath)));
        this.maxRetries = maxRetries;
        this.initialRetryDelayMs = retryInitialDelayMs;
        this.backfillRetries = backfillRetriesCounter;

        for (BackfillSourceConfig node : blockNodeSource.nodes()) {
            LOGGER.log(INFO, "Address: {0}, Port: {1}, Priority: {2}", node.address(), node.port(), node.priority());
        }
    }

    /**
     * Checks if the specified block range is available in the given block node.
     * @param node the block node to check
     * @param blockRange the block range to check
     * @return a LongRange representing the intersection of the block range and the available blocks in the node.
     */
    private LongRange getAvailableRangeInNode(BlockNodeClient node, LongRange blockRange) {
        long firstAvailableBlock = node.getBlockNodeServiceClient()
                .serverStatus(new ServerStatusRequest())
                .firstAvailableBlock();
        long lastAvailableBlock = node.getBlockNodeServiceClient()
                .serverStatus(new ServerStatusRequest())
                .lastAvailableBlock();

        long start = blockRange.start();
        long end = blockRange.end();

        // Compute the intersection
        long intersectionStart = Math.max(start, firstAvailableBlock);
        long intersectionEnd = Math.min(end, lastAvailableBlock);

        // If there's no overlap, return null
        if (intersectionStart > intersectionEnd) {
            return null;
        }

        return new LongRange(intersectionStart, intersectionEnd);
    }

    /**
     * Fetches missing blocks for the given block range.
     *
     * @param blockRange The block range to fetch
     * @return A list of blocks fetched from the block nodes, or an empty list if no blocks were found
     */
    public List<BlockUnparsed> fetchBlocks(LongRange blockRange) {
        LOGGER.log(INFO, "Requesting blocks for range: {0} to {1}", blockRange.start(), blockRange.end());

        // only use nodes that are ACTIVE or UNKNOWN
        List<BackfillSourceConfig> activeOrUnknownNodes = new ArrayList<>();
        for (BackfillSourceConfig node : blockNodeSource.nodes()) {
            Status currentStatus = nodeStatusMap.getOrDefault(node, Status.UNKNOWN);
            if (currentStatus == Status.AVAILABLE || currentStatus == Status.UNKNOWN) {
                activeOrUnknownNodes.add(node);
            }
        }
        // Randomize order to avoid always hitting the same node first
        Collections.shuffle(activeOrUnknownNodes);
        // Sort by priority
        activeOrUnknownNodes.sort(Comparator.comparingInt(BackfillSourceConfig::priority));

        // Try each node in priority order
        for (BackfillSourceConfig node : activeOrUnknownNodes) {
            LOGGER.log(INFO, "Trying Block Node: {0}:{1}", node.address(), node.port());

            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    BlockNodeClient currentNodeClient = getNodeClient(node);
                    // Check if the node has the blocks we need
                    LongRange actualRange = getAvailableRangeInNode(currentNodeClient, blockRange);
                    if (actualRange == null) {
                        LOGGER.log(
                                INFO,
                                "Block range not available in node {0}:{1}, trying next node",
                                node.address(),
                                node.port());
                        break;
                    }
                    // if we reach here, the node is available
                    nodeStatusMap.put(node, Status.AVAILABLE);

                    // Try to fetch blocks from this node
                    return currentNodeClient
                            .getBlockstreamSubscribeUnparsedClient()
                            .getBatchOfBlocks(actualRange.start(), actualRange.end());
                } catch (Exception e) {
                    if (attempt == maxRetries) {
                        // If we reach the max retries, mark the node as UNAVAILABLE
                        nodeStatusMap.put(node, Status.UNAVAILABLE);

                        LOGGER.log(
                                INFO,
                                "Failed to fetch blocks from node {0}:{1}, error: {2}",
                                node.address(),
                                node.port(),
                                e.getMessage());

                    } else {
                        long delay = Math.multiplyExact(initialRetryDelayMs, attempt);
                        LOGGER.log(INFO, "Attempt {0} failed. Retrying in {1} milliseconds...", attempt, delay);
                        try {
                            TimeUnit.MILLISECONDS.sleep(delay);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        // update metrics
                        backfillRetries.increment();
                    }
                }
            }
        }

        LOGGER.log(
                INFO,
                "No configured Block Node had the missing blocks for range: {0} to {1}",
                blockRange.start(),
                blockRange.end());
        return Collections.emptyList();
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
        return nodeClientMap.computeIfAbsent(node, BlockNodeClient::new);
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
}
