// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.hiero.block.common.hasher.HashingUtilities.getBlockItemHash;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.NaiveStreamingTreeHasher;
import org.hiero.block.common.hasher.StreamingHasher;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.blocks.MinimalBlockAccessor;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.block.node.verification.session.HapiVersionSessionFactory;
import org.hiero.block.node.verification.session.VerificationSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AllBlocksHasherHandlerTest {

    @TempDir
    Path tempDir;

    @Test
    void initializesFromGenesisWhenStoreEmpty() throws Exception {
        final Path hasherFile = tempDir.resolve("hasher.bin");
        final VerificationConfig config = new VerificationConfig(hasherFile, true);
        final HistoricalBlockFacility facility = new HistoricalBlockFacility() {
            @Override
            public BlockAccessor block(long blockNumber) {
                return null;
            }

            @Override
            public BlockRangeSet availableBlocks() {
                return BlockRangeSet.EMPTY;
            }
        };

        final AllBlocksHasherHandler handler = new AllBlocksHasherHandler(config, buildContext(facility));

        assertTrue(handler.isAvailable());
        assertEquals(0, handler.getNumberOfBlocks());
        assertNull(handler.lastBlockHash());

        final StreamingHasher expectedHasher = new StreamingHasher();
        expectedHasher.addLeaf(AllBlocksHasherHandler.ZERO_BLOCK_HASH);
        assertArrayEquals(expectedHasher.computeRootHash(), handler.computeRootHash());
        assertEquals(AllBlocksHasherHandler.BLOCK_HASH_LENGTH, Files.size(hasherFile));
    }

    @Test
    void rebuildsFromStoreWhenFileMissing() throws Exception {
        final BlockChainData chain = buildBlockChain(6);
        final Path hasherFile = tempDir.resolve("rebuild.bin");
        final VerificationConfig config = new VerificationConfig(hasherFile, true);
        final AllBlocksHasherHandler handler =
                new AllBlocksHasherHandler(config, buildContext(new ChainHistoricalBlockFacility(chain)));

        assertTrue(handler.isAvailable());
        assertEquals(chain.blocks().size(), handler.getNumberOfBlocks());
        assertArrayEquals(chain.blockHashes().get(chain.blockHashes().size() - 1), handler.lastBlockHash());
        assertArrayEquals(chain.expectedRootHash(), handler.computeRootHash());
    }

    @Test
    void loadsFullyFromExistingHasherFile() throws Exception {
        final BlockChainData chain = buildBlockChain(5);
        final Path hasherFile = tempDir.resolve("existing.bin");
        persistHasher(hasherFile, chain.blockHashes());
        final long originalSize = Files.size(hasherFile);

        final VerificationConfig config = new VerificationConfig(hasherFile, true);
        final AllBlocksHasherHandler handler =
                new AllBlocksHasherHandler(config, buildContext(new ChainHistoricalBlockFacility(chain)));

        assertTrue(handler.isAvailable());
        assertEquals(chain.blocks().size(), handler.getNumberOfBlocks());
        assertArrayEquals(chain.blockHashes().getLast(), handler.lastBlockHash());
        assertArrayEquals(chain.expectedRootHash(), handler.computeRootHash());
        assertEquals(originalSize, Files.size(hasherFile));
    }

    @Test
    void loadPartiallyFromFileSyncWithStoreMissing5blocks() throws Exception {
        final BlockChainData chain = buildBlockChain(10);
        final Path hasherFile = tempDir.resolve("partial-five.bin");
        final List<byte[]> partialHashes = chain.blockHashes().subList(0, 5);
        persistHasher(hasherFile, partialHashes);

        final VerificationConfig config = new VerificationConfig(hasherFile, true);
        final AllBlocksHasherHandler handler =
                new AllBlocksHasherHandler(config, buildContext(new ChainHistoricalBlockFacility(chain)));

        assertTrue(handler.isAvailable());
        assertEquals(chain.blocks().size(), handler.getNumberOfBlocks());
        assertArrayEquals(chain.expectedRootHash(), handler.computeRootHash());
        assertArrayEquals(chain.blockHashes().getLast(), handler.lastBlockHash());
    }

    @Test
    void missingSingleBlockTest() throws Exception {
        final BlockChainData chain = buildBlockChain(4);
        final Path hasherFile = tempDir.resolve("partial-one.bin");
        final List<byte[]> partialHashes =
                chain.blockHashes().subList(0, chain.blockHashes().size() - 1);
        persistHasher(hasherFile, partialHashes);

        final VerificationConfig config = new VerificationConfig(hasherFile, true);
        final AllBlocksHasherHandler handler =
                new AllBlocksHasherHandler(config, buildContext(new ChainHistoricalBlockFacility(chain)));

        assertTrue(handler.isAvailable());
        assertEquals(chain.blocks().size(), handler.getNumberOfBlocks());
        assertArrayEquals(chain.expectedRootHash(), handler.computeRootHash());
        assertArrayEquals(chain.blockHashes().getLast(), handler.lastBlockHash());
    }

    private BlockChainData buildBlockChain(final int blockCount) throws ParseException, NoSuchAlgorithmException {
        final StreamingHasher hasher = new StreamingHasher();
        hasher.addLeaf(AllBlocksHasherHandler.ZERO_BLOCK_HASH);

        final List<Block> blocks = new ArrayList<>(blockCount);
        final List<byte[]> blockHashes = new ArrayList<>(blockCount);
        byte[] previousHash = AllBlocksHasherHandler.ZERO_BLOCK_HASH;

        for (int i = 0; i < blockCount; i++) {
            final Block block = buildBlock(i, previousHash, hasher.computeRootHash());
            final byte[] blockHash = calculateBlockHash(block, i, hasher.computeRootHash(), previousHash);
            blocks.add(block);
            blockHashes.add(blockHash);
            hasher.addLeaf(blockHash);
            previousHash = blockHash;
        }

        final SimpleBlockRangeSet rangeSet = new SimpleBlockRangeSet();
        if (blockCount > 0) {
            rangeSet.add(0, blockCount - 1);
        }
        return new BlockChainData(blocks, blockHashes, hasher.computeRootHash(), rangeSet);
    }

    private Block buildBlock(
            final long blockNumber, final byte[] previousBlockHash, final byte[] rootHashOfAllBlockHashesTree)
            throws ParseException {
        final BlockHeader header = buildBlockHeader(blockNumber);
        final BlockFooter footer = BlockFooter.newBuilder()
                .previousBlockRootHash(Bytes.wrap(Arrays.copyOf(previousBlockHash, previousBlockHash.length)))
                .rootHashOfAllBlockHashesTree(
                        Bytes.wrap(Arrays.copyOf(rootHashOfAllBlockHashesTree, rootHashOfAllBlockHashesTree.length)))
                .startOfBlockStateRootHash(Bytes.wrap(("state-" + blockNumber).getBytes()))
                .build();

        StreamingTreeHasher emptyHasher = new NaiveStreamingTreeHasher();
        StreamingTreeHasher outputTreeHasher = new NaiveStreamingTreeHasher();

        outputTreeHasher.addLeaf(
                getBlockItemHash(BlockItem.newBuilder().blockHeader(header).build()));
        Bytes blockHash = HashingUtilities.computeFinalBlockHash(
                header.blockTimestamp(),
                Bytes.wrap(previousBlockHash),
                Bytes.wrap(rootHashOfAllBlockHashesTree),
                Bytes.wrap("state-" + blockNumber),
                emptyHasher,
                emptyHasher,
                emptyHasher,
                emptyHasher,
                emptyHasher);
        Bytes blockProof = HashingUtilities.noThrowSha384HashOf(blockHash);
        final BlockProof proof = BlockProof.newBuilder()
                .block(blockNumber)
                .signedBlockProof(TssSignedBlockProof.newBuilder()
                        .blockSignature(blockProof)
                        .build())
                .build();
        return new Block(List.of(
                new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_HEADER, header)),
                new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_FOOTER, footer)),
                new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_PROOF, proof))));
    }

    private byte[] calculateBlockHash(
            final Block block, final long blockNumber, final byte[] rootHashOfAllPreviousBlocks, final byte[] prevHash)
            throws ParseException {
        final List<BlockItemUnparsed> items = block.items().stream()
                .map(item -> {
                    try {
                        return BlockItemUnparsed.PROTOBUF.parse(BlockItem.PROTOBUF.toBytes(item));
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();

        final VerificationSession session = HapiVersionSessionFactory.createSession(
                blockNumber,
                BlockSource.UNKNOWN,
                block.items().get(0).blockHeader().hapiProtoVersion());
        session.processBlockItems(items);
        final VerificationNotification notification =
                session.finalizeVerification(Bytes.wrap(rootHashOfAllPreviousBlocks), Bytes.wrap(prevHash));
        return notification.blockHash().toByteArray();
    }

    private BlockNodeContext buildContext(final HistoricalBlockFacility facility) {
        return new BlockNodeContext(
                mock(Configuration.class),
                mock(Metrics.class),
                mock(HealthFacility.class),
                mock(BlockMessagingFacility.class),
                facility,
                mock(ServiceLoaderFunction.class),
                mock(ThreadPoolManager.class));
    }

    private void persistHasher(final Path hasherPath, final List<byte[]> blockHashes) throws IOException {
        Files.createDirectories(hasherPath.getParent());
        try (BufferedOutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(hasherPath))) {
            outputStream.write(AllBlocksHasherHandler.ZERO_BLOCK_HASH);
            for (byte[] hash : blockHashes) {
                outputStream.write(hash);
            }
        }
    }

    private record BlockChainData(
            List<Block> blocks, List<byte[]> blockHashes, byte[] expectedRootHash, BlockRangeSet availableRange) {}

    private static class ChainHistoricalBlockFacility implements HistoricalBlockFacility {
        private final Map<Long, Block> blocks;
        private final BlockRangeSet blockRangeSet;

        ChainHistoricalBlockFacility(final BlockChainData chain) {
            this.blocks = new HashMap<>();
            for (int i = 0; i < chain.blocks().size(); i++) {
                blocks.put((long) i, chain.blocks().get(i));
            }
            this.blockRangeSet = chain.availableRange();
        }

        @Override
        public BlockAccessor block(final long blockNumber) {
            final Block block = blocks.get(blockNumber);
            return block == null ? null : new MinimalBlockAccessor(blockNumber, block);
        }

        @Override
        public BlockRangeSet availableBlocks() {
            return blockRangeSet;
        }
    }

    private BlockHeader buildBlockHeader(final long blockNumber) {
        final SemanticVersion hapiVersion = new SemanticVersion(0, 69, 0, "a", "b");
        final Timestamp timestamp = new Timestamp(123L, 456);
        return new BlockHeader(hapiVersion, hapiVersion, blockNumber, timestamp, BlockHashAlgorithm.SHA2_384);
    }
}
