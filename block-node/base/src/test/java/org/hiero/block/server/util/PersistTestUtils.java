// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.util;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.input.EventHeader;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.platform.event.EventCore;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.common.utils.ChunkUtils;
import org.hiero.hapi.block.node.BlockItemUnparsed;

public final class PersistTestUtils {
    public static final String PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY = "persistence.storage.liveRootPath";
    public static final String PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY = "persistence.storage.archiveRootPath";
    public static final String PERSISTENCE_STORAGE_UNVERIFIED_ROOT_PATH_KEY = "persistence.storage.unverifiedRootPath";
    public static final String PERSISTENCE_STORAGE_COMPRESSION_TYPE = "persistence.storage.compressionType";
    public static final String PERSISTENCE_STORAGE_COMPRESSION_LEVEL = "persistence.storage.compressionLevel";
    public static final String PERSISTENCE_STORAGE_ARCHIVE_GROUP_SIZE = "persistence.storage.archiveGroupSize";

    private PersistTestUtils() {}

    /**
     * This method generates a list of {@link BlockItemUnparsed} with the input
     * blockNumber used to generate the block items for. It generates 10 block
     * items starting with the block header, followed by 8 events and ending
     * with the block proof.
     *
     * @param blockNumber the block number to generate the block items for
     *
     * @return a list of {@link BlockItemUnparsed} with the input blockNumber
     * used to generate the block items for
     */
    public static List<BlockItemUnparsed> generateBlockItemsUnparsedForWithBlockNumber(final long blockNumber) {
        return generateBlockItemsUnparsedForWithBlockNumber(blockNumber, 10);
    }

    /**
     * This method generates a list of {@link BlockItemUnparsed} with the input
     * blockNumber used to generate the block items for. It generates 10 block
     * items starting with the block header, followed by 8 events and ending
     * with the block proof.
     *
     * @param blockNumber the block number to generate the block items for
     * @param numOfBlockItems the number of block items to generate per block
     *
     * @return a list of {@link BlockItemUnparsed} with the input blockNumber
     * used to generate the block items for
     */
    public static List<BlockItemUnparsed> generateBlockItemsUnparsedForWithBlockNumber(
            final long blockNumber, final int numOfBlockItems) {
        final List<BlockItemUnparsed> result = new ArrayList<>();
        for (int j = 1; j <= numOfBlockItems; j++) {
            switch (j) {
                case 1:
                    // First block is always the header
                    result.add(BlockItemUnparsed.newBuilder()
                            .blockHeader(BlockHeader.PROTOBUF.toBytes(BlockHeader.newBuilder()
                                    .number(blockNumber)
                                    .softwareVersion(SemanticVersion.newBuilder()
                                            .major(1)
                                            .minor(0)
                                            .build())
                                    .build()))
                            .build());
                    break;
                case 10:
                    // Last block is always the state proof
                    result.add(BlockItemUnparsed.newBuilder()
                            .blockProof(BlockProof.PROTOBUF.toBytes(
                                    BlockProof.newBuilder().block(blockNumber).build()))
                            .build());
                    break;
                default:
                    // Middle blocks are events
                    result.add(BlockItemUnparsed.newBuilder()
                            .eventHeader(EventHeader.PROTOBUF.toBytes(EventHeader.newBuilder()
                                    .eventCore(EventCore.newBuilder()
                                            .creatorNodeId(blockNumber)
                                            .build())
                                    .build()))
                            .build());
                    break;
            }
        }
        return result;
    }

    /**
     * This method generates a list of {@link BlockItemUnparsed} for as many
     * blocks as specified by the input parameter numOfBlocks. For each block
     * number from 1 to numOfBlocks, it generates 10 block items starting with
     * the block header, followed by 8 events and ending with the block proof.
     * In a way, this simulates a stream of block items. Each 10 items in the
     * list represent a block.
     *
     * @param numOfBlocks the number of blocks to generate block items for
     *
     * @return a list of {@link BlockItemUnparsed} for as many blocks as
     * specified by the input parameter numOfBlocks
     */
    public static List<BlockItemUnparsed> generateBlockItemsUnparsed(int numOfBlocks) {
        final List<BlockItemUnparsed> blockItems = new ArrayList<>();
        for (int i = 1; i <= numOfBlocks; i++) {
            blockItems.addAll(generateBlockItemsUnparsedForWithBlockNumber(i));
        }
        return blockItems;
    }

    /**
     * This method generates a list of {@link BlockItemUnparsed} for as many
     * blocks as specified by the input parameter numOfBlocks. For each block
     * number from 0 to numOfBlocks - 1, it generates 10 block items starting with
     * the block header, followed by 8 events and ending with the block proof.
     * In a way, this simulates a stream of block items. Each 10 items in the
     * list represent a block.
     *
     * @param numOfBlocks the number of blocks to generate block items for
     *
     * @return a list of {@link BlockItemUnparsed} for as many blocks as
     * specified by the input parameter numOfBlocks
     */
    public static List<BlockItemUnparsed> generateBlockItemsUnparsedStartFromBlockNumber0(final int numOfBlocks) {
        final List<BlockItemUnparsed> blockItems = new ArrayList<>();
        for (int i = 0; i < numOfBlocks; i++) {
            blockItems.addAll(generateBlockItemsUnparsedForWithBlockNumber(i));
        }
        return blockItems;
    }

    /**
     * This method generates a list of {@link BlockItemUnparsed} for as many
     * blocks as specified by the input parameter numOfBlocks. For each block
     * number from 0 to numOfBlocks - 1, it generates 10 block items starting with
     * the block header, followed by 8 events and ending with the block proof.
     * In a way, this simulates a stream of block items. Each 10 items in the
     * list represent a block.
     *
     * @param numOfBlocks the number of blocks to generate block items for
     *
     * @return a list of {@link BlockItemUnparsed} for as many blocks as
     * specified by the input parameter numOfBlocks
     */
    public static List<List<BlockItemUnparsed>> generateBlockItemsUnparsedStartFromBlockNumber0Chunked(
            final int numOfBlocks) {
        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsedStartFromBlockNumber0(numOfBlocks);
        return ChunkUtils.chunkify(blockItems, 10);
    }

    /**
     * This method generates a list of {@link BlockItemUnparsed} for as many
     * blocks as specified in a given range. For each block number from
     * startBlockNumber to endBlockNumber - 1, it generates 10 block items.
     *
     * @param startBlockNumber the start block number
     * @param endBlockNumber the end block number
     *
     * @return a list of {@link BlockItemUnparsed} for as many blocks as
     * specified in a given range
     */
    public static List<BlockItemUnparsed> generateBlockItemsUnparsedForBlocksInRange(
            final long startBlockNumber, final long endBlockNumber) {
        final List<BlockItemUnparsed> blockItems = new ArrayList<>();
        for (long i = startBlockNumber; i < endBlockNumber; i++) {
            blockItems.addAll(generateBlockItemsUnparsedForWithBlockNumber(i));
        }
        return blockItems;
    }

    /**
     * This method generates a list of chunks of {@link BlockItemUnparsed} for
     * as many blocks as specified in a given range. For each block number from
     * startBlockNumber to endBlockNumber - 1, it generates 10 block items.
     *
     * @param startBlockNumber the start block number
     * @param endBlockNumber the end block number
     *
     * @return a list of chunks {@link BlockItemUnparsed} for as many blocks as
     * specified in a given range
     */
    public static List<List<BlockItemUnparsed>> generateBlockItemsUnparsedForBlocksInRangeChunked(
            final long startBlockNumber, final long endBlockNumber) {
        final List<BlockItemUnparsed> blockItems =
                generateBlockItemsUnparsedForBlocksInRange(startBlockNumber, endBlockNumber);
        return ChunkUtils.chunkify(blockItems, 10);
    }
}
