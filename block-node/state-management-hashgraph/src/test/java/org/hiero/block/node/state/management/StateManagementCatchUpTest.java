// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.RecordingServiceBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.metrics.core.MetricRegistry;
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
        // Genesis state 2 is seeded synchronously in start(), before anything is in `historic` —
        // read the real (network-specific) empty-state hash rather than guessing empty/zero.
        final StateManagementPlugin plugin = startPlugin(tmp, historic);
        final Bytes genesisHash = plugin.stagedStateRootHash();
        // Pre-seed three chained blocks: 0 (genesis), 1, 2.
        // No state changes in the blocks → root hash never changes after the first apply,
        // so all subsequent blocks carry the same footer hash.
        historic.put(0L, buildBlock(0L, 0L, genesisHash));
        plugin.catchUpFromHistoricalBlocks();
        // Under lag-1 block 0 is applied (staged) but not yet exposed — nothing attests it.
        assertThat(plugin.attested().state()).isNull();

        // Chain the remaining blocks off the staged hash (empty blocks don't change state,
        // so every footer carries the same hash). Block 3 confirms block 2 so it is exposed.
        final Bytes stagedAfter0 = plugin.stagedStateRootHash();
        historic.put(1L, buildBlock(1L, 1L, stagedAfter0));
        historic.put(2L, buildBlock(2L, 2L, stagedAfter0));
        historic.put(3L, buildBlock(3L, 3L, stagedAfter0));

        // Second catch-up pass applies 1,2,3 → exposes up to block 2 (lag-1).
        plugin.catchUpFromHistoricalBlocks();
        assertThat(plugin.attested().blockNumber()).isEqualTo(2L);
        assertThat(plugin.attested().roundNumber()).isEqualTo(2L);

        plugin.stop();
    }

    @Test
    void gapRecoverySweepDrainsWholeBacklogInOnePassNotOnePerRetry(@TempDir final Path tmp) throws Exception {
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        // historicCatchUpBatchSize=2 (set by startPlugin) so this also proves the sweep keeps
        // looping across multiple batches within a single call, not just within one batch.
        final StateManagementPlugin plugin = startPlugin(tmp, historic);
        final Bytes genesisHash = plugin.stagedStateRootHash();
        historic.put(0L, buildBlock(0L, 0L, genesisHash));
        plugin.catchUpFromHistoricalBlocks();
        assertThat(plugin.isApplyHalted()).isFalse();

        // Simulate a backlog that landed only in historical storage, none staged in pendingBlocks.
        // Asserts the whole backlog is picked up in one sweep, not just the next block.
        final Bytes stagedAfter0 = plugin.stagedStateRootHash();
        for (long blockNumber = 1L; blockNumber <= 10L; blockNumber++) {
            historic.put(blockNumber, buildBlock(blockNumber, blockNumber, stagedAfter0));
        }

        // This is exactly what `retryGap` invokes on a gap-retry firing — call it directly
        // rather than waiting on the real `GAP_RETRY_DELAY_SECONDS` timer.
        plugin.sweepHistoricalBlocks();

        assertThat(plugin.attested().blockNumber())
                .as("one sweep call should drain the entire available backlog, not just block 1")
                .isEqualTo(9L); // lag-1: applying through block 10 exposes up to block 9
        assertThat(plugin.isApplyHalted()).isFalse();
        plugin.stop();
    }

    @Test
    void liveGapRecoveryDoesNotDeadlockOnBackgroundExecutor(@TempDir final Path tmp) throws Exception {
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp, historic, facility);
        final Bytes genesisHash = plugin.stagedStateRootHash();
        historic.put(0L, buildBlock(0L, 0L, genesisHash));
        plugin.catchUpFromHistoricalBlocks();
        assertThat(plugin.isApplyHalted()).isFalse();

        // Blocks 1 and 2 land only in historical storage, so recovering them makes
        // sweepHistoricalBlocks apply two blocks back to back in one pass: block 1's apply
        // queues its background hash, then block 2 needs to join that hash. That's the shape
        // that deadlocked when the sweep ran on backgroundExecutor instead of the apply worker
        // (see StateManagementPlugin's gapSweepRequested javadoc) — unlike
        // gapRecoverySweepDrainsWholeBacklogInOnePassNotOnePerRetry, which calls
        // sweepHistoricalBlocks() directly from the test thread and so can't see a
        // wrong-thread deadlock.
        final Bytes stagedAfter0 = plugin.stagedStateRootHash();
        historic.put(1L, buildBlock(1L, 1L, stagedAfter0));
        historic.put(2L, buildBlock(2L, 2L, stagedAfter0));

        // Deliver block 3 live. Blocks 1-2 are missing from pendingBlocks, so only the real
        // scheduleGapRetry/retryGap timer path recovers them — exercising the actual
        // production threading, not a direct call.
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 3L, Bytes.fromHex("aabb"), buildBlock(3L, 3L, stagedAfter0), BlockSource.PUBLISHER));

        final long deadline = System.currentTimeMillis() + 8_000L;
        while (plugin.attested().blockNumber() < 2L && System.currentTimeMillis() < deadline) {
            Thread.sleep(50L);
        }

        assertThat(plugin.isApplyHalted()).isFalse();
        assertThat(plugin.attested().blockNumber())
                .as("gap recovery must apply blocks 1-2 (exposing up to 2 under lag-1) within one "
                        + "GAP_RETRY_DELAY_SECONDS cycle, not hang")
                .isEqualTo(2L);
        plugin.stop();
    }

    @Test
    void sweepHistoricalBlocksStopsAtFirstStalledBatchInsteadOfBufferingPastIt(@TempDir final Path tmp)
            throws Exception {
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        // historicCatchUpBatchSize=2 (set by startPlugin) so block 1's hole falls in the first
        // batch and blocks 3-5 are only reachable from later batches.
        final StateManagementPlugin plugin = startPlugin(tmp, historic);
        final Bytes genesisHash = plugin.stagedStateRootHash();
        historic.put(0L, buildBlock(0L, 0L, genesisHash));
        plugin.catchUpFromHistoricalBlocks();
        assertThat(plugin.isApplyHalted()).isFalse();

        // Block 1 is a hole: present nowhere. Blocks 2-5 land only in historical storage, past
        // the hole — mirrors rockysingh's finding that later batches kept buffering blocks that
        // can never be applied until the gap closes.
        final Bytes stagedAfter0 = plugin.stagedStateRootHash();
        historic.put(2L, buildBlock(2L, 2L, stagedAfter0));
        historic.put(3L, buildBlock(3L, 3L, stagedAfter0));
        historic.put(4L, buildBlock(4L, 4L, stagedAfter0));
        historic.put(5L, buildBlock(5L, 5L, stagedAfter0));

        plugin.sweepHistoricalBlocks();

        assertThat(plugin.isApplyHalted()).isFalse();
        assertThat(plugin.stagedStateRootHash())
                .as("apply must stall at block 1, not skip past it")
                .isEqualTo(stagedAfter0);
        assertThat(plugin.pendingBlocksSize())
                .as("sweep must stop at the first stalled batch, not keep buffering blocks 3-5 "
                        + "from later batches it can never apply until block 1 resolves")
                .isEqualTo(1); // only block 2, fetched in the same batch as the hole

        // Filling the hole and sweeping again must drain the whole backlog in one pass, proving
        // the early stop doesn't strand recovery once the gap closes.
        historic.put(1L, buildBlock(1L, 1L, stagedAfter0));
        plugin.sweepHistoricalBlocks();

        assertThat(plugin.isApplyHalted()).isFalse();
        assertThat(plugin.attested().blockNumber()).isEqualTo(4L); // lag-1: applied through 5
        assertThat(plugin.pendingBlocksSize()).isZero();
        plugin.stop();
    }

    @Test
    void noHistoricalFacilityIsHandledGracefully(@TempDir final Path tmp) throws Exception {
        final StateManagementPlugin plugin = startPlugin(tmp, null);
        assertThat(StateManagementPluginTestSupport.awaitReady(plugin, 5_000L)).isTrue();
        assertThat(plugin.attested().state()).isNull();
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
        return startPlugin(tmp, historic, new TestBlockMessagingFacility());
    }

    private static StateManagementPlugin startPlugin(
            final Path tmp, final HistoricalBlockFacility historic, final TestBlockMessagingFacility facility) {
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withValue(
                        "state.management.snapshotRecentPath",
                        tmp.resolve("recent").toString())
                .withValue("state.management.historicCatchUpBatchSize", "2")
                .build();
        final BlockNodeContext context = new BlockNodeContext(
                configuration,
                MetricRegistry.builder().build(),
                null,
                facility,
                historic,
                null,
                null,
                StateManagementPluginTestSupport.newThreadPoolManager(),
                null);
        final StateManagementPlugin plugin = new StateManagementPlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();
        return plugin;
    }

    private static final RecordingServiceBuilder NOOP_SERVICE_BUILDER = new RecordingServiceBuilder();
}
