// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

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
import org.hiero.block.api.StateMetadata;
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
 * Covers {@link StateManagementPlugin#catchUpFromHistoricalBlocks()}: on start, the
 * plugin reads blocks from {@code context.historicalBlockProvider()} and replays them
 * through the apply loop until it is caught up to the latest block on hand. Only then
 * is {@code awaitReady} satisfied.
 */
class StateManagementCatchUpTest {

    @Test
    void catchesUpFromHistoricalProvider(@TempDir final Path tmp) throws Exception {
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        // Pre-seed three chained blocks: 0 (genesis), 1, 2.
        // No state changes in the blocks → root hash never changes after the first apply,
        // so all subsequent blocks carry the same footer hash.
        historic.put(0L, buildBlock(0L, 0L, Bytes.EMPTY));
        // The other blocks need a startOfBlockStateRootHash equal to the live hash after
        // block 0, which only the plugin can compute. Two-phase fixture: catch-up applies
        // block 0, we read the staged hash, then push the chained blocks and catch up again.
        final StateManagementPlugin plugin = startPlugin(tmp, historic);
        StateManagementPluginTestSupport.awaitReady(plugin, 5_000L);
        // Under lag-1 block 0 is applied (staged) but not yet exposed — nothing attests it.
        assertThat(plugin.metadata()).isEqualTo(StateMetadata.DEFAULT);

        // Chain the remaining blocks off the staged hash (empty blocks don't change state,
        // so every footer carries the same hash). Block 3 confirms block 2 so it is exposed.
        final Bytes stagedAfter0 = plugin.stagedStateRootHash();
        historic.put(1L, buildBlock(1L, 1L, stagedAfter0));
        historic.put(2L, buildBlock(2L, 2L, stagedAfter0));
        historic.put(3L, buildBlock(3L, 3L, stagedAfter0));

        // Second catch-up pass applies 1,2,3 → exposes up to block 2 (lag-1).
        plugin.catchUpFromHistoricalBlocks();
        assertThat(plugin.metadata().blockNumber()).isEqualTo(2L);
        assertThat(plugin.metadata().roundNumber()).isEqualTo(2L);

        plugin.stop();
    }

    @Test
    void noHistoricalFacilityIsHandledGracefully(@TempDir final Path tmp) throws Exception {
        final StateManagementPlugin plugin = startPlugin(tmp, null);
        assertThat(StateManagementPluginTestSupport.awaitReady(plugin, 5_000L)).isTrue();
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
                    return blocks.keySet().stream()
                            .mapToLong(Long::longValue)
                            .min()
                            .orElse(-1L);
                }

                @Override
                public long max() {
                    return blocks.keySet().stream()
                            .mapToLong(Long::longValue)
                            .max()
                            .orElse(-1L);
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

    private static StateManagementPlugin startPlugin(final Path tmp, final HistoricalBlockFacility historic) {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .withValue(
                        "state.management.stateMetadataPath",
                        tmp.resolve("md.json").toString())
                .withValue(
                        "state.management.stateSnapshotRecentPath",
                        tmp.resolve("recent").toString())
                .withValue("state.management.snapshotIntervalMillis", "3600000")
                .withValue("state.management.stateChangesApplyIntervalMillis", "3600000")
                .withValue("state.management.historicCatchUpBatchSize", "2")
                .build();
        final BlockNodeContext context =
                new BlockNodeContext(configuration, null, null, facility, historic, null, null, null, null, null, null);
        final StateManagementPlugin plugin = new StateManagementPlugin();
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
