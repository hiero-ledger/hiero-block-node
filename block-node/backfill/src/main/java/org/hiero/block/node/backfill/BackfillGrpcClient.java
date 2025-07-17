// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.INFO;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.backfill.client.BlockNodeClient;
import org.hiero.block.node.backfill.client.proto.BlockNodeConfig;
import org.hiero.block.node.backfill.client.proto.BlockNodeSources;

public class BackfillGrpcClient {
    private static final System.Logger LOGGER = System.getLogger(BackfillGrpcClient.class.getName());

    // BN Sources List
    private final BlockNodeSources blockNodeSource;
    private final int maxRetries;

    // default initial delay of retry in seconds
    // first retry will be after INITIAL_RETRY_DELAY_SECONDS seconds
    // subsequent retries will double the delay each time
    private static final int INITIAL_RETRY_DELAY_SECONDS = 5;
    /** Current status of the Block Node Clients */
    private ConcurrentHashMap<BlockNodeConfig, status> nodeStatusMap = new ConcurrentHashMap<>();
    /**
     * Map of BlockNodeConfig to BlockNodeClient instances.
     * This allows us to reuse clients for the same node configuration.
     */
    private ConcurrentHashMap<BlockNodeConfig, BlockNodeClient> nodeClientMap = new ConcurrentHashMap<>();

    /**
     * Constructor for BackfillGrpcClient.
     *
     * @param blockNodePreferenceFilePath the path to the block node preference file
     */
    public BackfillGrpcClient(Path blockNodePreferenceFilePath, int maxRetries) throws IOException, ParseException {
        this.blockNodeSource = BlockNodeSources.JSON.parse(Bytes.wrap(Files.readAllBytes(blockNodePreferenceFilePath)));
        this.maxRetries = maxRetries;

        for (BlockNodeConfig node : blockNodeSource.nodes()) {
            LOGGER.log(INFO, "Address: {0}, Port: {1}, Priority: {2}", node.address(), node.port(), node.priority());
        }

        BlockNodeConfig firstNode = blockNodeSource.nodes().get(0);
        LOGGER.log(INFO, "Using block-node as default: {0}:{1}", firstNode.address(), firstNode.port());
    }

    /**
     * Checks if the specified block range is available in the given block node.
     * @param node the block node to check
     * @param blockRange the block range to check
     * @return true if the range is available in the node, false otherwise
     */
    private boolean isRangeAvailableInNode(BlockNodeClient node, BlockGap blockRange) {
        long firstAvailableBlock =
                node.getBlockNodeServerStatusClient().getServerStatus().firstAvailableBlock();
        long lastAvailableBlock =
                node.getBlockNodeServerStatusClient().getServerStatus().lastAvailableBlock();

        return blockRange.startBlockNumber() >= firstAvailableBlock
                && blockRange.endBlockNumber() <= lastAvailableBlock;

        // TODO: there might be some cases when BlockGap is available between more than 1 node.
        // ie: Gap from 0 to 100, 0-50 is available in node A, 51-100 is available in node B.
        // in this case we should return the available gap in the node instead and null when not a single block of given
        // gap is available.
    }

    /**
     * Fetches missing blocks for the given block range.
     *
     * @param blockRange The block range to fetch
     * @return A list of blocks fetched from the block nodes, or an empty list if no blocks were found
     */
    public List<BlockUnparsed> fetchBlocks(BlockGap blockRange) {
        LOGGER.log(
                INFO,
                "Requesting blocks for range: {0} to {1}",
                blockRange.startBlockNumber(),
                blockRange.endBlockNumber());

        // only use nodes that are ACTIVE or UNKNOWN
        List<BlockNodeConfig> activeOrUnknownNodes = new ArrayList<>();
        for (BlockNodeConfig node : blockNodeSource.nodes()) {
            status currentStatus = nodeStatusMap.getOrDefault(node, status.UNKNOWN);
            if (currentStatus == status.ACTIVE || currentStatus == status.UNKNOWN) {
                activeOrUnknownNodes.add(node);
            }
        }
        // Randomize order to avoid always hitting the same node first
        Collections.shuffle(activeOrUnknownNodes);
        // Sort by priority
        activeOrUnknownNodes.sort(Comparator.comparingInt(BlockNodeConfig::priority));

        // Try each node in priority order
        for (BlockNodeConfig node : activeOrUnknownNodes) {
            LOGGER.log(INFO, "Trying Block Node: {0}:{1}", node.address(), node.port());

            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    BlockNodeClient currentNodeClient = getNodeClient(node);
                    // Check if the node has the blocks we need
                    if (!isRangeAvailableInNode(currentNodeClient, blockRange)) {
                        LOGGER.log(
                                INFO,
                                "Block range not available in node {0}:{1}, trying next node",
                                node.address(),
                                node.port());
                        break;
                    }
                    // if we reach here, the node is available
                    nodeStatusMap.put(node, status.ACTIVE);

                    // Try to fetch blocks from this node
                    return currentNodeClient
                            .getBlockNodeSubscribeClient()
                            .getBatchOfBlocks(blockRange.startBlockNumber(), blockRange.endBlockNumber());
                } catch (Exception e) {
                    if (attempt == maxRetries) {
                        // If we reach the max retries, mark the node as UNAVAILABLE
                        nodeStatusMap.put(node, status.UNAVAILABLE);

                        LOGGER.log(
                                INFO,
                                "Failed to fetch blocks from node {0}:{1}, error: {2}",
                                node.address(),
                                node.port(),
                                e.getMessage());
                    } else {
                        long delay = (long) Math.pow(INITIAL_RETRY_DELAY_SECONDS, attempt);
                        LOGGER.log(INFO, "Attempt {0} failed. Retrying in {1} seconds...", attempt, delay);
                        try {
                            TimeUnit.SECONDS.sleep(delay);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
        }

        LOGGER.log(
                INFO,
                "No configured Block Node had the missing blocks for range: {0} to {1}",
                blockRange.startBlockNumber(),
                blockRange.endBlockNumber());
        return Collections.emptyList();
    }

    private BlockNodeClient getNodeClient(BlockNodeConfig node) {
        return nodeClientMap.computeIfAbsent(node, BlockNodeClient::new);
    }

    public void resetStatus() {
        for (BlockNodeConfig node : blockNodeSource.nodes()) {
            nodeStatusMap.put(node, status.UNKNOWN);
        }
    }

    public enum status {
        UNKNOWN,
        ACTIVE,
        UNAVAILABLE
    }
}
