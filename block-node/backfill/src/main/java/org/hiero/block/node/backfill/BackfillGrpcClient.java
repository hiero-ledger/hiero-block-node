// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.*;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
        LOGGER.log(INFO, "Using first node as default: {0}:{1}", firstNode.address(), firstNode.port());
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

        // Sort nodes by priority but randomize between nodes with same priority
        List<BlockNodeConfig> sortedNodes = new ArrayList<>(blockNodeSource.nodes());
        Collections.shuffle(sortedNodes); // Randomize order to avoid always hitting the same node first
        // Sort by priority
        sortedNodes.sort(Comparator.comparingInt(BlockNodeConfig::priority));

        // Try each node in priority order
        for (BlockNodeConfig node : sortedNodes) {
            LOGGER.log(INFO, "Trying Block Node: {0}:{1}", node.address(), node.port());

            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try (BlockNodeClient currentNodeClient = new BlockNodeClient(node)) {
                    // Check if the node has the blocks we need
                    if (!isRangeAvailableInNode(currentNodeClient, blockRange)) {
                        LOGGER.log(
                                INFO,
                                "Block range not available in node {0}:{1}, trying next node",
                                node.address(),
                                node.port());
                        break;
                    }

                    // Try to fetch blocks from this node
                    return currentNodeClient
                            .getBlockNodeSubscribeClient()
                            .getBatchOfBlocks(blockRange.startBlockNumber(), blockRange.endBlockNumber());
                } catch (Exception e) {
                    if (attempt == maxRetries) {
                        sortedNodes.remove(node);
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
                WARNING,
                "No configured Block Node had the missing blocks for range: {0} to {1}",
                blockRange.startBlockNumber(),
                blockRange.endBlockNumber());
        return Collections.emptyList();
    }
}
