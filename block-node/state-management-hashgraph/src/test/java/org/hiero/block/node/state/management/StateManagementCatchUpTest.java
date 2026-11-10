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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hiero.block.api.BlockRange;
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
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers start-up catch-up and live gap recovery: on start, the
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
    void sweepDrainsWholeBacklogInOnePassHoldingOneBlockAtATime(@TempDir final Path tmp) throws Exception {
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        final StateManagementPlugin plugin = startPlugin(tmp, historic);
        final Bytes genesisHash = plugin.stagedStateRootHash();
        historic.put(0L, buildBlock(0L, 0L, genesisHash));
        plugin.catchUpFromHistoricalBlocks();
        assertThat(plugin.isApplyHalted()).isFalse();

        // A backlog that landed only in historical storage. One sweep picks up all of it.
        final Bytes stagedAfter0 = plugin.stagedStateRootHash();
        for (long blockNumber = 1L; blockNumber <= 10L; blockNumber++) {
            historic.put(blockNumber, buildBlock(blockNumber, blockNumber, stagedAfter0));
        }
        historic.resetAccessorStats();

        plugin.sweepHistoricalBlocks();

        assertThat(plugin.attested().blockNumber())
                .as("one sweep call should drain the entire available backlog, not just block 1")
                .isEqualTo(9L); // lag-1: applying through block 10 exposes up to block 9
        assertThat(plugin.isApplyHalted()).isFalse();
        assertThat(historic.opened()).isEqualTo(10);
        assertThat(historic.maxConcurrentlyOpen())
                .as("never more than one historical block held at a time")
                .isEqualTo(1);
        assertThat(historic.currentlyOpen()).as("every accessor is closed").isZero();
        plugin.stop();
    }

    @Test
    void aheadLiveBlockIsDroppedAndTheGapIsFilledFromHistoryOnTheCatchUpThread(@TempDir final Path tmp)
            throws Exception {
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp, historic, facility);
        final Bytes genesisHash = plugin.stagedStateRootHash();
        historic.put(0L, buildBlock(0L, 0L, genesisHash));
        plugin.catchUpFromHistoricalBlocks(); // the start-up sweep ran before block 0 existed
        assertThat(plugin.isApplyHalted()).isFalse();

        // Blocks 1 and 2 are only in historical storage. Live block 3 arrives ahead of them: it is
        // dropped, and the catch-up thread (never the delivering thread) reads 1 and 2 one at a time.
        final Bytes stagedAfter0 = plugin.stagedStateRootHash();
        historic.put(1L, buildBlock(1L, 1L, stagedAfter0));
        historic.put(2L, buildBlock(2L, 2L, stagedAfter0));
        historic.resetAccessorStats();
        final VerificationNotification block3 = new VerificationNotification(
                true, null, 3L, Bytes.fromHex("aabb"), buildBlock(3L, 3L, stagedAfter0), BlockSource.PUBLISHER);

        facility.sendBlockVerification(block3);
        awaitCatchUpFinished(plugin);

        assertThat(plugin.isApplyHalted()).isFalse();
        assertThat(plugin.attested().blockNumber())
                .as("blocks 1 and 2 applied, exposing up to 1 under lag-1; block 3 was not kept")
                .isEqualTo(1L);
        assertThat(historic.opened()).as("only the two missing blocks are read").isEqualTo(2);
        assertThat(historic.readerThreads()).doesNotContain(Thread.currentThread());
        assertThat(historic.maxConcurrentlyOpen()).isEqualTo(1);
        assertThat(historic.currentlyOpen()).isZero();

        // Block 3 is now the next expected one, so re-sending it applies it inline with no read.
        historic.resetAccessorStats();
        facility.sendBlockVerification(block3);

        assertThat(plugin.attested().blockNumber()).isEqualTo(2L);
        assertThat(historic.opened()).isZero();
        plugin.stop();
    }

    @Test
    void liveBlockAheadOfAGapHistoryCannotFillIsDroppedAndTheNextNotificationRetries(@TempDir final Path tmp)
            throws Exception {
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp, historic, facility);
        final Bytes genesisHash = plugin.stagedStateRootHash();
        historic.put(0L, buildBlock(0L, 0L, genesisHash));
        plugin.catchUpFromHistoricalBlocks(); // the start-up sweep ran before block 0 existed
        final Bytes stagedAfter0 = plugin.stagedStateRootHash();

        // History has block 0 only. Live block 3 is ahead of 1 and 2, which nobody can supply.
        final VerificationNotification block3 = new VerificationNotification(
                true, null, 3L, Bytes.fromHex("aabb"), buildBlock(3L, 3L, stagedAfter0), BlockSource.PUBLISHER);
        facility.sendBlockVerification(block3);
        awaitCatchUpFinished(plugin);

        assertThat(plugin.isApplyHalted()).isFalse();
        assertThat(plugin.attested().state()).as("nothing applied past block 0").isNull();
        assertThat(plugin.stagedStateRootHash()).isEqualTo(stagedAfter0);

        // Block 3 was not kept. Once history has 1 and 2, the next notification (block 3 re-sent,
        // standing in for block 4) makes the catch-up thread fill them in.
        historic.put(1L, buildBlock(1L, 1L, stagedAfter0));
        historic.put(2L, buildBlock(2L, 2L, stagedAfter0));
        facility.sendBlockVerification(block3);
        awaitCatchUpFinished(plugin);

        assertThat(plugin.attested().blockNumber()).isEqualTo(1L);
        assertThat(plugin.isApplyHalted()).isFalse();
        plugin.stop();
    }

    @Test
    void sweepStopsAtHoleAndResumesOnceItFills(@TempDir final Path tmp) throws Exception {
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        final StateManagementPlugin plugin = startPlugin(tmp, historic);
        final Bytes genesisHash = plugin.stagedStateRootHash();
        historic.put(0L, buildBlock(0L, 0L, genesisHash));
        plugin.catchUpFromHistoricalBlocks();
        assertThat(plugin.isApplyHalted()).isFalse();

        // Block 1 is a hole: present nowhere. Blocks 2-5 are in history, past the hole.
        final Bytes stagedAfter0 = plugin.stagedStateRootHash();
        historic.put(2L, buildBlock(2L, 2L, stagedAfter0));
        historic.put(3L, buildBlock(3L, 3L, stagedAfter0));
        historic.put(4L, buildBlock(4L, 4L, stagedAfter0));
        historic.put(5L, buildBlock(5L, 5L, stagedAfter0));
        historic.resetAccessorStats();

        assertThat(plugin.sweepHistoricalBlocks()).isFalse();

        assertThat(plugin.isApplyHalted()).isFalse();
        assertThat(plugin.stagedStateRootHash())
                .as("apply must stall at block 1, not skip past it")
                .isEqualTo(stagedAfter0);
        assertThat(historic.opened())
                .as("block 1 is absent and the sweep must not open blocks 2-5, which it cannot apply")
                .isZero();

        historic.put(1L, buildBlock(1L, 1L, stagedAfter0));
        assertThat(plugin.sweepHistoricalBlocks()).isTrue();

        assertThat(plugin.isApplyHalted()).isFalse();
        assertThat(plugin.attested().blockNumber()).isEqualTo(4L); // lag-1: applied through 5
        plugin.stop();
    }

    @Test
    void staleVerifiedBlocksAreDroppedWithoutReadsAndDoNotBlockLiveBlocks(@TempDir final Path tmp) throws Exception {
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp, historic, facility);
        final Bytes genesisHash = plugin.stagedStateRootHash();
        historic.put(0L, buildBlock(0L, 0L, genesisHash));
        plugin.catchUpFromHistoricalBlocks();
        final Bytes stagedAfter0 = plugin.stagedStateRootHash();
        historic.put(1L, buildBlock(1L, 1L, stagedAfter0));
        historic.put(2L, buildBlock(2L, 2L, stagedAfter0));
        plugin.catchUpFromHistoricalBlocks();
        assertThat(plugin.isApplyHalted()).isFalse();
        final Bytes stagedAfter2 = plugin.stagedStateRootHash();
        historic.resetAccessorStats();

        // A backfill-sourced verification for an already-applied block (1), and a resend of the
        // last applied block (2): both are dropped, nothing is read and the state is unchanged.
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 1L, Bytes.fromHex("aabb"), buildBlock(1L, 1L, stagedAfter0), BlockSource.BACKFILL));
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 2L, Bytes.fromHex("aabb"), buildBlock(2L, 2L, stagedAfter0), BlockSource.PUBLISHER));
        assertThat(historic.opened()).isZero();
        assertThat(plugin.stagedStateRootHash()).isEqualTo(stagedAfter2);
        assertThat(plugin.attested().blockNumber()).isEqualTo(1L);

        // The next live block still applies immediately.
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 3L, Bytes.fromHex("aabb"), buildBlock(3L, 3L, stagedAfter0), BlockSource.PUBLISHER));
        assertThat(plugin.attested().blockNumber()).isEqualTo(2L); // lag-1: applying 3 exposes 2
        assertThat(plugin.isApplyHalted()).isFalse();
        plugin.stop();
    }

    @Test
    void notReadyUntilCatchUpReachesTipThenReadyOnceANotificationRetriesIt(@TempDir final Path tmp) throws Exception {
        final ChainHashes hashes = chainHashes(tmp);
        // Block 1 is missing from history, so start-up catch-up applies block 0 and stalls.
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        historic.put(0L, buildBlock(0L, 0L, hashes.genesis()));
        historic.put(2L, buildBlock(2L, 2L, hashes.afterBlock0()));
        historic.put(3L, buildBlock(3L, 3L, hashes.afterBlock0()));
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp.resolve("run"), historic, facility);

        awaitCatchUpFinished(plugin);
        assertThat(plugin.isReady()).as("sweep stopped at the hole in history").isFalse();
        assertThat(plugin.isApplyHalted()).isFalse();

        // With no timer, the hole filling in history is not noticed until the next notification.
        historic.put(1L, buildBlock(1L, 1L, hashes.afterBlock0()));
        Thread.sleep(200L);
        assertThat(plugin.isReady())
                .as("nothing re-drives catch-up without a notification")
                .isFalse();

        // Live block 4 arrives ahead of 1: it is dropped and requests a catch-up, which reads 1-3
        // from history and reaches the latest available block (3), so the plugin becomes ready.
        facility.sendBlockVerification(new VerificationNotification(
                true,
                null,
                4L,
                Bytes.fromHex("aabb"),
                buildBlock(4L, 4L, hashes.afterBlock0()),
                BlockSource.PUBLISHER));
        awaitCatchUpFinished(plugin);
        assertThat(plugin.isReady()).isTrue();
        assertThat(plugin.attested().blockNumber()).isEqualTo(2L); // lag-1: applied through 3
        plugin.stop();
    }

    @Test
    void aLiveBlockFillingTheHoleAppliesInlineThenCatchUpAppliesTheRestOfHistory(@TempDir final Path tmp)
            throws Exception {
        final ChainHashes hashes = chainHashes(tmp);
        // Block 1 is a hole, with blocks 2 and 3 already in history past it.
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        historic.put(0L, buildBlock(0L, 0L, hashes.genesis()));
        historic.put(2L, buildBlock(2L, 2L, hashes.afterBlock0()));
        historic.put(3L, buildBlock(3L, 3L, hashes.afterBlock0()));
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp.resolve("run"), historic, facility);
        awaitCatchUpFinished(plugin);
        assertThat(plugin.isReady()).as("stalled at the hole").isFalse();
        historic.resetAccessorStats();

        // Block 1 arrives from any source (here a backfill) and is the next expected block, so it
        // applies inline on the delivering thread. Nothing else is notified, yet catch-up then
        // applies 2 and 3 from history.
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 1L, Bytes.fromHex("aabb"), buildBlock(1L, 1L, hashes.afterBlock0()), BlockSource.BACKFILL));
        awaitCatchUpFinished(plugin);

        assertThat(plugin.isReady()).isTrue();
        assertThat(plugin.attested().blockNumber()).isEqualTo(2L); // lag-1: applied through 3
        assertThat(historic.readerThreads()).doesNotContain(Thread.currentThread());
        plugin.stop();
    }

    @Test
    void sweepFailureLeavesPluginNotReadyThenALiveBlockTriggersARecoveringCatchUp(@TempDir final Path tmp)
            throws Exception {
        final ChainHashes hashes = chainHashes(tmp);
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        historic.put(0L, buildBlock(0L, 0L, hashes.genesis()));
        historic.failNextBlockCalls(1);
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp.resolve("run"), historic, facility);

        // The start-up sweep throws on its first block fetch; the plugin must not claim readiness.
        awaitCatchUpFinished(plugin);
        assertThat(plugin.isReady()).isFalse();
        assertThat(plugin.isApplyHalted()).isFalse();

        // Live block 1 is ahead of the never-applied block 0: it is dropped and requests a
        // catch-up, which now reads block 0 from history and reaches the latest block.
        facility.sendBlockVerification(new VerificationNotification(
                true,
                null,
                1L,
                Bytes.fromHex("aabb"),
                buildBlock(1L, 1L, hashes.afterBlock0()),
                BlockSource.PUBLISHER));
        awaitCatchUpFinished(plugin);
        assertThat(plugin.isReady()).isTrue();
        assertThat(plugin.isApplyHalted()).isFalse();
        plugin.stop();
    }

    @Test
    void noHistoricalFacilityIsHandledGracefully(@TempDir final Path tmp) throws Exception {
        final StateManagementPlugin plugin = startPlugin(tmp, null);
        assertThat(StateManagementPluginTestSupport.awaitReady(plugin, 5_000L)).isTrue();
        assertThat(plugin.attested().state()).isNull();
        plugin.stop();
    }

    private record ChainHashes(Bytes genesis, Bytes afterBlock0) {}

    /// Genesis and post-block-0 staged hashes, read from a throwaway plugin so tests can
    /// pre-populate history before the plugin under test starts.
    private static ChainHashes chainHashes(final Path tmp) throws Exception {
        final InMemoryHistoricalBlockFacility historic = new InMemoryHistoricalBlockFacility();
        final StateManagementPlugin probe = startPlugin(tmp.resolve("probe"), historic);
        final Bytes genesis = probe.stagedStateRootHash();
        historic.put(0L, buildBlock(0L, 0L, genesis));
        probe.catchUpFromHistoricalBlocks();
        final Bytes afterBlock0 = probe.stagedStateRootHash();
        probe.stop();
        return new ChainHashes(genesis, afterBlock0);
    }

    private static void awaitCatchUpFinished(final StateManagementPlugin plugin) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 5_000L;
        while (plugin.isCatchUpRunning() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertThat(plugin.isCatchUpRunning()).isFalse();
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
     * Tiny in-tree {@link HistoricalBlockFacility} backed by a map of
     * {@code blockNumber → BlockUnparsed}. The plugin's catch-up only reads
     * {@code block(n)} and {@code availableBlocks()}, so the other facility methods are
     * unimplemented.
     */
    private static final class InMemoryHistoricalBlockFacility implements HistoricalBlockFacility {
        private final Map<Long, BlockUnparsed> blocks = new ConcurrentHashMap<>();
        private final AtomicInteger blockFailures = new AtomicInteger();
        private final AtomicInteger opened = new AtomicInteger();
        private final AtomicInteger currentlyOpen = new AtomicInteger();
        private final AtomicInteger maxConcurrentlyOpen = new AtomicInteger();
        private final java.util.Set<Thread> readerThreads = ConcurrentHashMap.newKeySet();

        void resetAccessorStats() {
            opened.set(0);
            maxConcurrentlyOpen.set(currentlyOpen.get());
            readerThreads.clear();
        }

        /// The threads that opened a block since the last `resetAccessorStats`.
        java.util.Set<Thread> readerThreads() {
            return readerThreads;
        }

        int opened() {
            return opened.get();
        }

        int currentlyOpen() {
            return currentlyOpen.get();
        }

        int maxConcurrentlyOpen() {
            return maxConcurrentlyOpen.get();
        }

        /// Make the next `count` calls to `block(n)` throw, to simulate a failing sweep.
        void failNextBlockCalls(final int count) {
            blockFailures.set(count);
        }

        void put(final long blockNumber, @NonNull final BlockUnparsed block) {
            blocks.put(blockNumber, block);
        }

        @Override
        public BlockAccessor block(final long blockNumber) {
            if (blockFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new IllegalStateException("simulated block fetch failure");
            }
            final BlockUnparsed block = blocks.get(blockNumber);
            if (block == null) {
                return null;
            }
            opened.incrementAndGet();
            readerThreads.add(Thread.currentThread());
            maxConcurrentlyOpen.accumulateAndGet(currentlyOpen.incrementAndGet(), Math::max);
            return new InMemoryAccessor(blockNumber, block, currentlyOpen);
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
                public Stream<BlockRange> streamRanges() {
                    return blocks.keySet().stream().sorted().map(b -> new BlockRange(b, b));
                }
            };
        }
    }

    private static final class InMemoryAccessor implements BlockAccessor {
        private final long blockNumber;
        private final BlockUnparsed block;
        private final AtomicInteger currentlyOpen;
        private boolean closed = false;

        InMemoryAccessor(
                final long blockNumber, @NonNull final BlockUnparsed block, final AtomicInteger currentlyOpen) {
            this.blockNumber = blockNumber;
            this.block = block;
            this.currentlyOpen = currentlyOpen;
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
            if (!closed) {
                currentlyOpen.decrementAndGet();
            }
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
