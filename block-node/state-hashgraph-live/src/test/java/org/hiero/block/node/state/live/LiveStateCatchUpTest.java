// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.consensus.config.PathsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers {@link LiveStatePlugin#catchUpFromHistoricalBlocks()} (STORY-14): on start, the
 * plugin reads blocks from {@code context.historicalBlockProvider()} and replays them
 * through the apply loop until it is caught up to the latest block on hand. Only then
 * is {@code awaitReady} satisfied.
 */
class LiveStateCatchUpTest {

    @Test
    void catchesUpFromHistoricalProvider(@TempDir final Path tmp) throws Exception {
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        // Pre-seed three chained blocks: 0 (genesis), 1, 2.
        // No state changes in the blocks → root hash never changes after the first apply,
        // so all subsequent blocks carry the same footer hash.
        historic.put(0L, buildBlock(0L, 0L, Bytes.EMPTY));
        // The other two blocks need a startOfBlockStateRootHash equal to the live hash
        // after block 0. We can't know that until the plugin computes it, so the test
        // probes the plugin's mutable hash after manually applying block 0 — but that
        // would defeat the test. Instead, we use a two-phase fixture: catch-up applies
        // block 0, we then read the live hash, then push 1 and 2 with the right footer,
        // and call catch-up again.
        final LiveStatePlugin plugin = startPlugin(tmp, historic);
        LiveStatePluginTestSupport.awaitReady(plugin, 5_000L);
        assertThat(plugin.metadata().blockNumber()).isZero();

        // Pre-seed remaining blocks with chained hashes now that block 0 has applied.
        final Bytes liveAfter0 = plugin.metadata().stateRootHash();
        historic.put(1L, buildBlock(1L, 1L, liveAfter0));
        historic.put(2L, buildBlock(2L, 2L, liveAfter0));

        // Second catch-up pass — closes the new gap to 2.
        plugin.catchUpFromHistoricalBlocks();
        assertThat(plugin.metadata().blockNumber()).isEqualTo(2L);
        assertThat(plugin.metadata().roundNumber()).isEqualTo(2L);

        plugin.stop();
    }

    @Test
    void noHistoricalFacilityIsHandledGracefully(@TempDir final Path tmp) throws Exception {
        final LiveStatePlugin plugin = startPlugin(tmp, null);
        assertThat(LiveStatePluginTestSupport.awaitReady(plugin, 5_000L)).isTrue();
        assertThat(plugin.metadata().blockNumber()).isZero();
        plugin.stop();
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    private static BlockUnparsed buildBlock(final long blockNumber, final long roundNumber, final Bytes startHash) {
        return BlockUnparsed.newBuilder()
                .blockItems(
                        BlockItemUnparsed.newBuilder()
                                .blockHeader(BlockHeader.PROTOBUF.toBytes(BlockHeader.newBuilder()
                                        .number(blockNumber)
                                        .build()))
                                .build(),
                        BlockItemUnparsed.newBuilder()
                                .roundHeader(RoundHeader.PROTOBUF.toBytes(RoundHeader.newBuilder()
                                        .roundNumber(roundNumber)
                                        .build()))
                                .build(),
                        BlockItemUnparsed.newBuilder()
                                .blockFooter(BlockFooter.PROTOBUF.toBytes(BlockFooter.newBuilder()
                                        .startOfBlockStateRootHash(startHash)
                                        .build()))
                                .build())
                .build();
    }

    /**
     * Tiny in-tree {@link HistoricalBlockFacility} backed by a LinkedHashMap of
     * {@code blockNumber → BlockUnparsed}. The plugin's catch-up only reads
     * {@code block(n)} and {@code availableBlocks()}, so the other facility methods are
     * unimplemented.
     */
    private static final class InMemoryHistoricalBlockFacility implements HistoricalBlockFacility {
        private final Map<Long, BlockUnparsed> blocks = new LinkedHashMap<>();

        void put(final long blockNumber, @NonNull final BlockUnparsed block) {
            blocks.put(blockNumber, block);
        }

        @Override
        public BlockAccessor block(final long blockNumber) {
            final BlockUnparsed block = blocks.get(blockNumber);
            return block == null ? null : new InMemoryAccessor(blockNumber, block);
        }

        @Override
        public BlockRangeSet availableBlocks() {
            return new BlockRangeSet() {
                @Override
                public boolean contains(final long blockNumber) {
                    return blocks.containsKey(blockNumber);
                }

                @Override
                public boolean contains(final long start, final long end) {
                    return LongStream.rangeClosed(start, end).allMatch(blocks::containsKey);
                }

                @Override
                public long size() {
                    return blocks.size();
                }

                @Override
                public long min() {
                    return blocks.keySet().stream().mapToLong(Long::longValue).min().orElse(-1L);
                }

                @Override
                public long max() {
                    return blocks.keySet().stream().mapToLong(Long::longValue).max().orElse(-1L);
                }

                @Override
                public LongStream stream() {
                    return blocks.keySet().stream().mapToLong(Long::longValue).sorted();
                }

                @Override
                public Stream<LongRange> streamRanges() {
                    return blocks.keySet().stream().sorted().map(b -> new LongRange(b, b));
                }
            };
        }
    }

    private static final class InMemoryAccessor implements BlockAccessor {
        private final long blockNumber;
        private final BlockUnparsed block;
        private boolean closed = false;

        InMemoryAccessor(final long blockNumber, @NonNull final BlockUnparsed block) {
            this.blockNumber = blockNumber;
            this.block = block;
        }

        @Override
        public long blockNumber() {
            return blockNumber;
        }

        @Override
        public BlockUnparsed blockUnparsed() {
            return block; // override the default-method round-trip through PROTOBUF
        }

        @Override
        public Bytes blockBytes(final Format format) {
            return switch (format) {
                case PROTOBUF -> BlockUnparsed.PROTOBUF.toBytes(block);
                default -> throw new UnsupportedOperationException("Only PROTOBUF supported in this fixture");
            };
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }
    }

    private static LiveStatePlugin startPlugin(
            final Path tmp, final HistoricalBlockFacility historic) {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(LiveStateConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .withValue("state.live.stateMetadataPath", tmp.resolve("md.json").toString())
                .withValue("state.live.stateSnapshotRecentPath", tmp.resolve("recent").toString())
                .withValue("state.live.stateSnapshotHistoricPath", tmp.resolve("historic").toString())
                .withValue("state.live.snapshotIntervalMillis", "3600000")
                .withValue("state.live.stateChangesApplyIntervalMillis", "3600000")
                .withValue("state.live.historicCatchUpBatchSize", "2")
                .build();
        final BlockNodeContext context = new BlockNodeContext(
                configuration, null, null, facility, historic, null, null, null, null, null, null);
        final LiveStatePlugin plugin = new LiveStatePlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();
        return plugin;
    }

    private static final ServiceBuilder NOOP_SERVICE_BUILDER = new ServiceBuilder() {
        @Override
        public void registerHttpService(@NonNull final String path, final HttpService... service) {}

        @Override
        public void registerGrpcService(@NonNull final ServiceInterface service) {}
    };
}
