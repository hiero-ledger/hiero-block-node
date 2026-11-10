// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.block.stream.output.StateIdentifier;
import com.hedera.hapi.node.state.blockstream.BlockStreamInfo;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.hiero.base.crypto.Hash;
import org.hiero.base.file.FileSystemManager;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// State plugin that maintains a live, queryable copy of Hashgraph network state inside
/// the Block Node by replaying verified block-stream state changes onto a
/// `VirtualMapStateLifecycleManager`-managed state.
///
/// See `docs/design/state/live-state.md` for the full design.
public final class StateManagementPlugin implements BlockNodePlugin, BlockNotificationHandler {

    private static final System.Logger LOGGER = System.getLogger(StateManagementPlugin.class.getName());

    /// `blocknode:state_applied_block` — latest committed (reader-visible) block number.
    static final MetricKey<ObservableGauge> METRIC_APPLIED_BLOCK =
            MetricKey.of("state_applied_block", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:state_size` — node count of the latest network-attested state.
    static final MetricKey<ObservableGauge> METRIC_STATE_SIZE =
            MetricKey.of("state_size", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:state_ready` — `1` once start-up catch-up is complete, else `0`.
    static final MetricKey<ObservableGauge> METRIC_READY =
            MetricKey.of("state_ready", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:state_apply_halted` — `1` when block apply has halted (e.g. footer
    /// hash mismatch), else `0`.
    static final MetricKey<ObservableGauge> METRIC_APPLY_HALTED =
            MetricKey.of("state_apply_halted", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:state_apply_latency_ms` — wall-clock duration of the most recent
    /// block apply, in milliseconds.
    static final MetricKey<ObservableGauge> METRIC_APPLY_LATENCY_MS =
            MetricKey.of("state_apply_latency_ms", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:state_hash_mismatch_total` — cumulative footer hash mismatches.
    static final MetricKey<LongCounter> METRIC_HASH_MISMATCH_TOTAL =
            MetricKey.of("state_hash_mismatch_total", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:state_block_lag` — blocks between the latest available block and the
    /// currently attested one. `0` once caught up; a persistent value signals a stall,
    /// independent of `state_apply_halted`.
    static final MetricKey<ObservableGauge> METRIC_BLOCK_LAG =
            MetricKey.of("state_block_lag", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    private BlockNodeContext context;
    private StateManagementConfig config;
    private VirtualMapStateLifecycleManager lifecycleManager;
    private StateChangeApplier applier;

    /// State (1) and the block/round number it corresponds to, published as one unit so a
    /// reader never pairs a new state with a stale number, see `publishAttested`. `state` is
    /// `null` until first attestation. `blockNumber`/`roundNumber` are read directly from the
    /// state itself (or the snapshot directory name on restore) rather than tracked as an
    /// independent copy — see `docs/design/state/live-state.md` §5.1. Readers must not use `state`
    /// directly: the apply thread releases a superseded state, so they go through
    /// `reserveAttested()` and read from the returned `ReservedState`.
    record AttestedSnapshot(@Nullable VirtualMapState state, long blockNumber, long roundNumber) {}

    /// A reader's reservation on the attested state and the block/round it corresponds to. While
    /// open, the apply thread cannot destroy `state`, even after a newer state is attested.
    /// Close it as soon as the read is done (`try`-with-resources); `close()` is idempotent.
    public static final class ReservedState implements AutoCloseable {
        private final VirtualMapState state;
        private final long blockNumber;
        private final long roundNumber;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private ReservedState(@NonNull final AttestedSnapshot snapshot) {
            this.state = java.util.Objects.requireNonNull(snapshot.state());
            this.blockNumber = snapshot.blockNumber();
            this.roundNumber = snapshot.roundNumber();
        }

        /// @return the reserved state, valid until `close()`
        @NonNull
        public VirtualMapState state() {
            return state;
        }

        /// @return the block number `state()` corresponds to
        public long blockNumber() {
            return blockNumber;
        }

        /// @return the round number `state()` corresponds to, or `-1` if unknown
        public long roundNumber() {
            return roundNumber;
        }

        /// Release the reservation. Safe to call more than once.
        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                state.getRoot().release();
            }
        }
    }

    /// Placeholder until the first block is attested: no state, no block, no round.
    private static final AttestedSnapshot GENESIS_ATTESTED = new AttestedSnapshot(null, -1L, -1L);

    /// Volatile because reads happen on query threads.
    private volatile AttestedSnapshot attested = GENESIS_ATTESTED;

    /// Backs `state_ready`: set once a sweep reaches the latest block, cleared on stop or halt.
    private final AtomicBoolean stateIsCaughtUp = new AtomicBoolean(false);
    /// Set by `stop()` so an in-flight apply or sweep gives up instead of starting new work.
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    /// Terminal: once set no further block is applied, and recovery is a restart from a snapshot.
    private final AtomicBoolean applyHalted = new AtomicBoolean(false);
    private final AtomicLong hashMismatchTotal = new AtomicLong();

    /// Wall-clock duration of the most recent block apply, exported via
    /// `state_apply_latency_ms`. Volatile: written by the apply thread, read by the
    /// metrics-scrape thread.
    private volatile long lastApplyDurationMs = 0L;

    /// Exported counter measurement for footer hash mismatches, registered in
    /// `init` (always before any apply runs in `start`).
    private LongCounter.Measurement hashMismatchMetric;

    /// The catch-up thread: the plugin's only thread of its own, and the only one that reads
    /// history. It replays missing blocks at start-up and whenever a live block arrives ahead of
    /// the next expected one. Live blocks that are next in line are applied inline on the
    /// messaging thread that delivers them, which never reads history. `null` until `start()`.
    private ExecutorService catchUpExecutor;

    /// `true` while a catch-up sweep is queued or running, so at most one runs at a time.
    private final AtomicBoolean catchUpRunning = new AtomicBoolean(false);

    /// Serialises every apply (live inline and catch-up) so only one thread mutates the
    /// lifecycle-managed state at a time. Taken per block, so a live block waits for at most one
    /// block's apply. Fair, so the catch-up thread cannot starve a live block waiting on it.
    private final ReentrantLock applyLock = new ReentrantLock(true);

    /// Block number of the most recent block whose state_changes have been applied to the
    /// mutable state. Distinct from `metadata.blockNumber`: that's the most recently *attested*
    /// (reader-visible) block, this is the staging block committed once the next block's footer
    /// confirms it (the lag-1 commit). `-1` until the first apply.
    private volatile long lastAppliedBlock = -1L;

    /// Round number of `lastAppliedBlock`, mirrored for the same lag-1 reason.
    private volatile long lastAppliedRound = -1L;

    // ── Three concurrent state versions ────────────────────────────────────────
    // (3) MUTABLE — `lifecycleManager.getMutableState()`; receives the current block's
    //     state_changes (library-managed, no field here).
    // (2) HASHING — `hashingImmutable`; sealed by copyMutableState, awaiting
    //     attestation by the next block's footer.
    // (1) ATTESTED — `attested.state()`; the network-confirmed, query-serving copy.
    // A state is hashed exactly once, never while mutable (3); the hash is computed inline
    // when the next block's footer needs it (`stagedStateRootHash`).

    /// **State (2) — sealed, awaiting attestation.** `null` until the first apply.
    private volatile VirtualMapState hashingImmutable;

    /// {@inheritDoc}
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = context;
        this.config = context.configuration().getConfigData(StateManagementConfig.class);
        this.applier = new StateChangeApplier();
        try {
            // Anchor FileSystemManager at the configured snapshot path so the scratchpad lives
            // next to snapshots; first filesystem touch, fails here if the dir isn't writable.
            final Path fsmRoot = Path.of(config.snapshotRecentPath());
            this.lifecycleManager = new VirtualMapStateLifecycleManager(
                    new NoOpSwirldsMetrics(),
                    Time.getCurrent(),
                    context.configuration(),
                    new FileSystemManager(fsmRoot));
        } catch (final RuntimeException e) {
            // Non-fatal: plugin stays inert (lifecycleManager == null, start() no-ops);
            // the rest of the Block Node is unaffected.
            LOGGER.log(
                    System.Logger.Level.WARNING,
                    "Failed to prepare state directory {0}.",
                    config.snapshotRecentPath(),
                    e);
        }
        registerMetrics(context.metricRegistry());
    }

    /// Register the plugin's metrics with the block node's registry.
    ///
    /// @param metrics the block node metric registry
    private void registerMetrics(@NonNull final MetricRegistry metrics) {
        metrics.register(ObservableGauge.builder(METRIC_APPLIED_BLOCK)
                .setDescription("Latest committed (reader-visible) block number applied to live state")
                .observe(() -> attested.blockNumber()));
        metrics.register(ObservableGauge.builder(METRIC_STATE_SIZE)
                .setDescription("Node count of the latest network-attested state")
                .observe(() -> sizeOf(attested.state())));
        metrics.register(ObservableGauge.builder(METRIC_READY)
                .setDescription("1 once the plugin has reached the latest available block without halting, else 0")
                .observe(() -> isReady() ? 1L : 0L));
        metrics.register(ObservableGauge.builder(METRIC_APPLY_HALTED)
                .setDescription("1 when block apply has halted (e.g. footer hash mismatch), else 0")
                .observe(() -> applyHalted.get() ? 1L : 0L));
        metrics.register(ObservableGauge.builder(METRIC_APPLY_LATENCY_MS)
                .setDescription("Wall-clock duration of the most recent block apply in ms")
                .observe(() -> lastApplyDurationMs));
        this.hashMismatchMetric = metrics.register(LongCounter.builder(METRIC_HASH_MISMATCH_TOTAL)
                        .setDescription("Cumulative footer hash mismatches observed"))
                .getOrCreateNotLabeled();
        metrics.register(ObservableGauge.builder(METRIC_BLOCK_LAG)
                .setDescription("Blocks between the latest available block and the currently attested block")
                .observe(this::currentBlockLag));
    }

    /// Blocks between the latest block `historicalBlockProvider` has on hand and the currently
    /// attested block. `0` if there's no historical facility, nothing available, or we're caught
    /// up; never negative.
    ///
    /// @return the current block lag
    private long currentBlockLag() {
        final HistoricalBlockFacility historic = context == null ? null : context.historicalBlockProvider();
        if (historic == null) {
            return 0L;
        }
        final BlockRangeSet available = historic.availableBlocks();
        if (available == null || available.size() == 0L) {
            return 0L;
        }
        return Math.max(0L, available.max() - attested.blockNumber());
    }

    /// {@inheritDoc}
    @Override
    public void start() {
        if (lifecycleManager == null) {
            // init() could not prepare the state directory; there is nothing to start.
            return;
        }
        loadPersistedState();
        context.blockMessaging().registerBlockNotificationHandler(this, true, name());

        // Catch up from history. Needed because an idle node (no live blocks) would otherwise
        // never become ready. The thread stays up for later catch-ups, see `requestCatchUp`.
        catchUpExecutor = context.threadPoolManager().createSingleThreadExecutor("StateManagement-catchup");
        requestCatchUp();
    }

    /// {@inheritDoc}
    @Override
    public void stop() {
        stopping.set(true);
        stateIsCaughtUp.set(false);
        if (context != null) {
            try {
                context.blockMessaging().unregisterBlockNotificationHandler(this);
            } catch (final RuntimeException ignored) {
                // facility may already be torn down — non-fatal.
            }
        }
        if (catchUpExecutor != null) {
            catchUpExecutor.shutdownNow();
            try {
                catchUpExecutor.awaitTermination(2L, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// Applies a verified block inline on the notification thread when it is the next expected
    /// one. Holds no block data between notifications: a stale block is dropped, and a block ahead
    /// of the next expected one is dropped too and triggers a catch-up on the catch-up thread, so
    /// this thread never reads history. The next notification retries if history was not ready.
    ///
    /// {@inheritDoc}
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        // A halt is terminal: restart re-derives state from the snapshot and history.
        if (!notification.success() || notification.block() == null || applyHalted.get()) {
            return;
        }
        final long blockNumber = notification.blockNumber();
        final long lastApplied = lastAppliedBlock;
        if (lastApplied >= 0L && blockNumber <= lastApplied) {
            return; // a resend or backfill-sourced verification of an already applied block
        }
        // The inline apply and hash below run on this messaging thread; their cost on a full-size
        // state is not yet measured (#4031).
        applyLock.lock();
        try {
            if (applyHalted.get() || stopping.get()) {
                return;
            }
            final HistoricalBlockFacility historic = historicOrNull();
            final long expectedNext = nextExpectedBlock(historic, blockNumber);
            if (blockNumber < expectedNext) {
                return; // applied by another thread while we waited on the lock
            }
            if (blockNumber > expectedNext) {
                LOGGER.log(
                        System.Logger.Level.DEBUG,
                        "Block {0} arrived ahead of block {1}; dropping it and catching up from history",
                        blockNumber,
                        expectedNext);
                requestCatchUp();
                return;
            }
            if (applyGuarded(notification.block(), blockNumber) && !refreshReady(historic)) {
                // History holds blocks past this one (for example, this block filled a hole that
                // had stalled catch-up): let the catch-up thread apply them.
                requestCatchUp();
            }
        } finally {
            applyLock.unlock();
        }
    }

    // ── Package-private observers ─────────────────────────────────────────────
    // Tests drive the plugin through its real entry points (block delivery, start) and
    // assert against these observers.

    /// Whether historical catch-up has completed.
    ///
    /// @return `true` once catch-up has finished
    boolean isReady() {
        return stateIsCaughtUp.get();
    }

    /// Whether the start-up catch-up thread is still walking history.
    ///
    /// @return `true` while catch-up runs
    boolean isCatchUpRunning() {
        return catchUpRunning.get();
    }

    /// The current attested snapshot: the network-confirmed state and the block/round number
    /// it corresponds to, published together (§6.5). `state()` is `null` if nothing has been
    /// attested yet.
    ///
    /// @return the current `AttestedSnapshot`
    @NonNull
    AttestedSnapshot attested() {
        return attested;
    }

    /// Reserve the attested state for reading, together with the block and round number it
    /// corresponds to. The reservation keeps the state alive while the caller reads, even if the
    /// apply thread attests a newer state meanwhile. If it loses that race (the state was
    /// released between loading the field and reserving it) it retries on the newer state.
    ///
    /// @return the reservation, or `null` if nothing has been attested yet. The caller must close it.
    @Nullable
    public ReservedState reserveAttested() {
        while (true) {
            final AttestedSnapshot snapshot = attested;
            if (snapshot.state() == null) {
                return null;
            }
            if (snapshot.state().getRoot().tryReserve()) {
                return new ReservedState(snapshot);
            }
            // tryReserve fails only if the apply thread already released this state, which means
            // a newer one is published: reload and reserve that instead.
        }
    }

    /// The `startOfBlockStateRootHash` the next block's footer must carry to be accepted — the
    /// root hash of the most recently applied (staged) block (state 2), one ahead of `metadata`
    /// under lag-1. Empty before the first apply.
    ///
    /// @return the staged state root hash, or empty before the first apply
    @NonNull
    Bytes stagedStateRootHash() {
        return rootHashOf(hashingImmutable);
    }

    /// Total count of footer hash mismatches observed.
    ///
    /// @return the cumulative hash-mismatch count
    long hashMismatchTotal() {
        return hashMismatchTotal.get();
    }

    /// Whether block apply has halted (stopped-applying state).
    ///
    /// @return `true` if apply is halted
    boolean isApplyHalted() {
        return applyHalted.get();
    }

    // ── Internals ───────────────────────────────────────────────────────────

    /// Restore from the highest-numbered recent-snapshot directory, if any exist, seeding the
    /// lag-1 bookkeeping so the loaded state is exposed on boot. A fresh node (no directories at
    /// all) falls back to genesis; a directory that fails to load or whose own
    /// `BlockStreamInfo.block_number` disagrees with its directory name halts instead, since
    /// silently falling back to genesis would discard the operator's recorded intent — see
    /// `docs/design/state/live-state.md` §6.2.
    private void loadPersistedState() {
        long restoreCandidate = -1L;
        try {
            restoreCandidate = highestNumberedSnapshotDirectory();
        } catch (final IOException e) {
            // Snapshots may exist but cannot be listed: same policy as an unreadable snapshot,
            // halt rather than start from genesis and later report a hash mismatch that looks
            // like divergence.
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Unable to scan {0} for snapshot directories; halting rather than silently starting from genesis",
                    config.snapshotRecentPath(),
                    e);
            applyHalted.set(true);
        }
        boolean snapshotLoaded = false;
        if (restoreCandidate >= 0L) {
            final Path snapshotDir = recentSnapshotDirectoryFor(restoreCandidate);
            try {
                final Hash loadedHash = lifecycleManager.loadSnapshot(snapshotDir);
                final long loadedBlockNumber = blockNumberOf(lifecycleManager.getLatestImmutableState());
                if (loadedBlockNumber == restoreCandidate) {
                    snapshotLoaded = true;
                    lastAppliedBlock = restoreCandidate;
                    // Logged so operators can compare it with an external value: nothing in the
                    // chain attests this hash until the next block's footer arrives.
                    LOGGER.log(
                            System.Logger.Level.INFO,
                            "Loaded state snapshot from {0} (root hash {1})",
                            snapshotDir,
                            loadedHash.toHex());
                } else {
                    // The loaded state disagrees with the name of the directory it came from —
                    // unrecoverable in place, same as a block-apply hash mismatch: halt rather
                    // than serve wrong data.
                    LOGGER.log(
                            System.Logger.Level.ERROR,
                            "Snapshot at {0} reports block {1}, but its directory name is {2}; halting",
                            snapshotDir,
                            loadedBlockNumber,
                            restoreCandidate);
                    hashMismatchTotal.incrementAndGet();
                    hashMismatchMetric.increment();
                    applyHalted.set(true);
                }
            } catch (final IOException | RuntimeException e) {
                // loadSnapshot wraps most on-disk problems (missing/corrupt MerkleDb files) in an
                // unchecked exception rather than a plain IOException — catch both rather than
                // let a bad snapshot directory crash start() outright.
                LOGGER.log(
                        System.Logger.Level.ERROR,
                        "Snapshot at {0} unreadable; halting rather than silently starting from genesis",
                        snapshotDir,
                        e);
                applyHalted.set(true);
            }
        }
        // Force the initial copyMutableState so getLatestImmutableState() is non-null from boot
        // onwards (and so the live mutable is a writable copy).
        lifecycleManager.copyMutableState();
        if (snapshotLoaded) {
            // The loaded (already-attested) block is simultaneously state 1 (exposed to
            // readers) and state 2 (the base the next block's footer validates against).
            // roundNumber is left unknown (-1): it isn't persisted (see §5.1) and is repopulated
            // by the first applied block's RoundHeader, exactly like the existing fallback for a
            // block that doesn't carry one of its own.
            final VirtualMapState loaded = lifecycleManager.getLatestImmutableState();
            publishAttested(loaded, restoreCandidate, -1L);
            hashingImmutable = loaded;
        } else if (!applyHalted.get()) {
            // True genesis: seed state 2 with the sealed empty VirtualMap so block 0 validates
            // through the same path as every other block. Skipped if we just halted above, so
            // hashingImmutable stays null rather than surfacing an untrustworthy hash.
            hashingImmutable = lifecycleManager.getLatestImmutableState();
        }
    }

    /// Scan `${config.snapshotRecentPath()}` for numerically-named directories and return the
    /// highest one.
    ///
    /// @return the highest-numbered snapshot directory's block number, or `-1` if none exist
    /// @throws IOException if the snapshot root exists but cannot be listed
    private long highestNumberedSnapshotDirectory() throws IOException {
        final Path root = Path.of(config.snapshotRecentPath());
        if (!Files.isDirectory(root)) {
            return -1L;
        }
        try (final var entries = Files.list(root)) {
            return entries.filter(Files::isDirectory)
                    .map(p -> p.getFileName().toString())
                    .filter(StateManagementPlugin::isDigits)
                    .mapToLong(Long::parseLong)
                    .max()
                    .orElse(-1L);
        }
    }

    /// @param name a directory's file name
    /// @return `true` if `name` is non-empty and every character is an ASCII digit
    private static boolean isDigits(@NonNull final String name) {
        return !name.isEmpty() && name.chars().allMatch(Character::isDigit);
    }

    /// Read `BlockStreamInfo.block_number` from `state`'s own singleton — the self-consistency
    /// check that replaces cross-referencing a separate metadata file (§5.1/§6.2).
    ///
    /// @param state the state to read
    /// @return the block number recorded in `state`'s `BlockStreamInfo` singleton, or `-1` if
    ///     absent or unparseable
    private static long blockNumberOf(@NonNull final VirtualMapState state) {
        final Bytes raw = state.getSingleton(StateIdentifier.STATE_ID_BLOCK_STREAM_INFO.protoOrdinal());
        if (raw == null || raw.length() == 0L) {
            return -1L;
        }
        try {
            return BlockStreamInfo.PROTOBUF
                    .parse(raw.toReadableSequentialData())
                    .blockNumber();
        } catch (final ParseException e) {
            return -1L;
        }
    }

    /// The historical facility, or `null` if there is none.
    @Nullable
    private HistoricalBlockFacility historicOrNull() {
        return context == null ? null : context.historicalBlockProvider();
    }

    /// The block number apply expects next. Before the first apply (no snapshot restored) this
    /// is the oldest block history has, or `fallback` if history is empty or starts later.
    ///
    /// @param historic the historical facility, may be `null`
    /// @param fallback the block to start from when history offers no earlier one
    /// @return the next block number to apply
    private long nextExpectedBlock(@Nullable final HistoricalBlockFacility historic, final long fallback) {
        if (lastAppliedBlock >= 0L) {
            return Math.max(lastAppliedBlock, attested.blockNumber()) + 1L;
        }
        if (historic != null) {
            final BlockRangeSet available = historic.availableBlocks();
            if (available != null && available.size() > 0L) {
                return Math.min(available.min(), fallback);
            }
        }
        return fallback;
    }

    /// `applyBlockStateChanges` with the halt-on-throw policy. A halt is terminal (the mutable
    /// state may be partially mutated; recovery is a restart from the last snapshot).
    /// Caller holds `applyLock`.
    ///
    /// @param block the block to apply
    /// @param blockNumber the block's number, for diagnostics
    /// @return `true` if applied, `false` if rejected or halted
    private boolean applyGuarded(@NonNull final BlockUnparsed block, final long blockNumber) {
        try {
            return applyBlockStateChanges(block);
        } catch (final RuntimeException e) {
            LOGGER.log(
                    System.Logger.Level.WARNING,
                    "applyBlockStateChanges threw for block {0}; halting apply (restart required to recover)",
                    blockNumber,
                    e);
            applyHalted.set(true);
            return false;
        }
    }

    /// Fetch blocks `from..to` from `historic` and apply them in order, holding at most one block
    /// at a time (open accessor, parse, apply, close). Stops at the first block history cannot
    /// supply, at a halt, or when stopping. Runs on the catch-up thread. The lock is taken per block,
    /// so a live block can interleave; blocks another thread already applied are skipped.
    ///
    /// @param historic the historical facility to read from
    /// @param from first block number to apply
    /// @param to last block number to apply, inclusive
    /// @return `true` if the plugin has applied through `to`
    private boolean applyFromHistory(@NonNull final HistoricalBlockFacility historic, final long from, final long to) {
        for (long i = from; i <= to; i++) {
            applyLock.lock();
            try {
                if (stopping.get() || applyHalted.get()) {
                    return false;
                }
                if (lastAppliedBlock >= i) {
                    continue; // applied meanwhile by another thread
                }
                final BlockAccessor accessor = historic.block(i);
                if (accessor == null) {
                    return false;
                }
                final BlockUnparsed block;
                try {
                    block = accessor.blockUnparsed();
                } finally {
                    try {
                        accessor.close();
                    } catch (final Exception ignored) {
                        // close failures are non-fatal.
                    }
                }
                if (block == null || !applyGuarded(block, i)) {
                    return false;
                }
            } finally {
                applyLock.unlock();
            }
        }
        return lastAppliedBlock >= to;
    }

    /// Latch readiness once the plugin is not halted and has applied through the latest block
    /// history has on hand (or there is no history to catch up to). Caller holds `applyLock`.
    ///
    /// @param historic the historical facility, may be `null`
    /// @return `false` only if the plugin is running and history holds blocks it has not applied
    private boolean refreshReady(@Nullable final HistoricalBlockFacility historic) {
        if (applyHalted.get() || stopping.get()) {
            return true;
        }
        final BlockRangeSet available = historic == null ? null : historic.availableBlocks();
        if (available == null || available.size() == 0L || lastAppliedBlock >= available.max()) {
            stateIsCaughtUp.set(true);
            return true;
        }
        return false;
    }

    /// Queue a catch-up sweep on the catch-up thread unless one is already queued or running.
    /// Does nothing once stopping.
    private void requestCatchUp() {
        if (catchUpExecutor == null || stopping.get() || !catchUpRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            catchUpExecutor.execute(() -> {
                try {
                    catchUpFromHistoricalBlocks();
                } finally {
                    catchUpRunning.set(false);
                }
            });
        } catch (final RejectedExecutionException e) {
            catchUpRunning.set(false); // executor shut down by stop()
        }
    }

    /// Bring the live state up to the latest block on hand by replaying history one block at a
    /// time. Runs on the catch-up thread, at start-up and whenever a live block arrives ahead of the
    /// next expected one. Ready only if the sweep reached the latest available block and is not
    /// halted. A sweep that stops short leaves the plugin not ready: the next live block ahead of
    /// the gap requests another one, see `handleVerification`.
    void catchUpFromHistoricalBlocks() {
        boolean reachedTip = false;
        try {
            reachedTip = sweepHistoricalBlocks();
        } catch (final RuntimeException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Catch-up failed; plugin remains not ready", e);
        }
        if (applyHalted.get()) {
            LOGGER.log(System.Logger.Level.WARNING, "Catch-up halted; plugin remains not ready");
        } else if (reachedTip) {
            stateIsCaughtUp.set(!stopping.get());
        } else {
            LOGGER.log(System.Logger.Level.WARNING, "Catch-up stopped short of the latest block; plugin not ready yet");
        }
    }

    /// Apply, block by block, everything `historicalBlockProvider` has from the next expected
    /// block to its latest, re-reading the latest each round so blocks persisted meanwhile are
    /// included. Package-private so tests can drive it directly.
    ///
    /// @return `true` if the plugin reached the latest available block (or there was nothing to
    ///     catch up to), `false` if it stopped short (hole, failed fetch or halt)
    boolean sweepHistoricalBlocks() {
        final HistoricalBlockFacility historic = historicOrNull();
        if (historic == null) {
            return true;
        }
        while (!stopping.get() && !applyHalted.get()) {
            final BlockRangeSet available = historic.availableBlocks();
            if (available == null || available.size() == 0L) {
                return true;
            }
            final long latest = available.max();
            final long start = nextExpectedBlock(historic, available.min());
            if (start > latest) {
                return true;
            }
            if (!applyFromHistory(historic, start, latest)) {
                return false;
            }
        }
        return false;
    }

    /// Apply a single block's `state_changes` to the live state under the **lag-1 commit**
    /// model: a block is applied into the live mutable, but only exposed once the *next*
    /// block's footer attests its root hash.
    ///
    /// Flow for block N (given block N-1 was the last applied, post-(N-1) sealed):
    ///
    /// 1. Pull `BlockFooter.startOfBlockStateRootHash` from N.
    /// 2. Compute state 2's hash (`hashingImmutable`, post-(N-1)) inline and validate N's
    ///    footer against it. Mismatch ⇒ halt without mutating.
    /// 3. Promote state 2 → state 1 (`attested`), publishing its state and metadata together.
    /// 4. Apply N's `state_changes` to the live mutable, then seal it as the new state 2 —
    ///    left un-hashed and un-exposed until block N+1 attests it.
    ///
    /// @param block the block to apply
    /// @return `true` on successful apply, `false` if rejected
    private boolean applyBlockStateChanges(@NonNull final BlockUnparsed block) {
        final long applyStartNanos = System.nanoTime();
        // Block number is pulled up-front purely for diagnostics — the apply path below
        // re-parses it from the header as the authoritative value.
        final long incomingBlock = incomingBlockNumberOf(block);
        final Bytes startHash = StateChangeApplier.extractStartOfBlockStateRootHash(block);
        if (startHash == null) {
            LOGGER.log(
                    System.Logger.Level.WARNING,
                    "Refusing to apply block {0}: missing a parseable BlockFooter",
                    incomingBlock);
            applyHalted.set(true);
            return false;
        }
        // State 2's root hash, computed here on first use (hashingImmutable is seeded at boot).
        final Bytes attestedHash = rootHashOf(hashingImmutable);
        if (!validateStartHash(startHash, attestedHash)) {
            if (lastAppliedBlock < 0L && incomingBlock > 0L) {
                // No snapshot was restored and the first block is mid-chain: its footer hashes
                // pre-block state this node never had. A missing seed, not divergence.
                applyHalted.set(true);
                LOGGER.log(
                        System.Logger.Level.ERROR,
                        "No seed state for block {0}: no snapshot was restored and this is not block 0, so its "
                                + "footer.startOfBlockStateRootHash cannot match the empty starting state; "
                                + "apply-halted. Provide a snapshot at or before block {1} with the following "
                                + "blocks available",
                        incomingBlock,
                        incomingBlock - 1L);
                return false;
            }
            hashMismatchTotal.incrementAndGet();
            hashMismatchMetric.increment();
            applyHalted.set(true);
            if (startHash.length() != attestedHash.length()) {
                // Different lengths mean a digest algorithm mismatch (e.g. CN on SHA-256, this
                // build still on SHA-384), not state divergence.
                LOGGER.log(
                        System.Logger.Level.ERROR,
                        "State hash mismatch applying block {0}: footer hash is {1} bytes, this build computes "
                                + "{2}-byte hashes — likely a digest algorithm mismatch, not divergence; "
                                + "apply-halted",
                        incomingBlock,
                        startHash.length(),
                        attestedHash.length());
            } else {
                LOGGER.log(
                        System.Logger.Level.ERROR,
                        "State hash mismatch applying block {0}: footer.startOfBlockStateRootHash ({1}) diverges "
                                + "from the last applied state's root hash ({2}, post-block {3}); plugin marked "
                                + "apply-halted (state not exposed)",
                        incomingBlock,
                        startHash.toHex(),
                        attestedHash.toHex(),
                        lastAppliedBlock);
            }
            return false;
        }

        // N's footer just attested state 2. Promote HASHING → ATTESTED now (lag-1: readers
        // only see attested state). Skipped at genesis and when the last applied block is
        // already exposed, e.g. after a snapshot reload.
        if (lastAppliedBlock >= 0L && (attested.state() == null || lastAppliedBlock != attested.blockNumber())) {
            publishAttested(
                    hashingImmutable,
                    lastAppliedBlock,
                    lastAppliedRound < 0L ? attested.roundNumber() : lastAppliedRound);
        }

        // Apply this block into the live mutable (state 3) and fast-copy to seal it as the new
        // state 2, but DO NOT hash or expose it: it stays staged until block N+1 attests it.
        final VirtualMapState mutable = lifecycleManager.getMutableState();
        final StateChangeApplier.ApplyResult result = applier.applyBlock(mutable, block);
        if (!result.applied()) {
            // The mutable state may be partly changed, so a failed apply is final (see the
            // class-level note on halting); recovery is a restart from the last snapshot.
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Refusing to apply block {0}: {1} ({2}); apply-halted",
                    incomingBlock,
                    result.detail(),
                    result.status());
            applyHalted.set(true);
            return false;
        }
        lifecycleManager.copyMutableState();
        hashingImmutable = lifecycleManager.getLatestImmutableState();
        lastAppliedBlock = result.blockNumber();
        if (result.roundNumber() >= 0L) {
            lastAppliedRound = result.roundNumber();
        }
        lastApplyDurationMs = (System.nanoTime() - applyStartNanos) / 1_000_000L;
        return true;
    }

    /// Validate block N's `BlockFooter.startOfBlockStateRootHash` against `attestedHash` — the
    /// root hash of state 2 (post-(N-1)), computed off the *sealed* `hashingImmutable` (never
    /// the live mutable). At genesis, state 2 is the sealed empty `VirtualMap`, so block 0
    /// validates through this same path with no special case.
    ///
    /// @param startHash the incoming block's footer start-of-block state root hash
    /// @param attestedHash the root hash of state 2 (post-(N-1))
    /// @return `true` if the hashes match
    private boolean validateStartHash(@NonNull final Bytes startHash, @NonNull final Bytes attestedHash) {
        return attestedHash.equals(startHash);
    }

    /// Publish `newState` and its block/round number as one atomic unit (a single volatile
    /// write to `attested`) so a reader never pairs a new state with a stale number. Also
    /// `reserve()`s the new state's root and `release()`s the previous one, since
    /// `copyMutableState()` releases the lifecycle manager's own reference — without this the
    /// held immutable is destroyed and reads/snapshots throw `ReferenceCountException`.
    ///
    /// @param newState the newly-attested state to publish to readers
    /// @param newBlockNumber the block number `newState` corresponds to
    /// @param newRoundNumber the round number `newState` corresponds to, or `-1` if unknown
    private void publishAttested(
            @NonNull final VirtualMapState newState, final long newBlockNumber, final long newRoundNumber) {
        final AttestedSnapshot previous = attested;
        newState.getRoot().reserve();
        attested = new AttestedSnapshot(newState, newBlockNumber, newRoundNumber);
        if (previous.state() != null && previous.state() != newState) {
            previous.state().getRoot().release();
        }
    }

    /// The block number from `block`'s `BlockHeader`, for diagnostics only. A verified block
    /// always starts with a `BlockHeader` (see `VerificationServicePlugin.validateStartOfBlock`).
    ///
    /// @param block the block to read
    /// @return the block number, or `-1` if the first item isn't a parseable `BlockHeader`
    private static long incomingBlockNumberOf(@NonNull final BlockUnparsed block) {
        final List<BlockItemUnparsed> items = block.blockItems();
        final BlockItemUnparsed first = items.isEmpty() ? null : items.getFirst();
        if (first == null || !first.hasBlockHeader()) {
            return -1L;
        }
        try {
            return BlockHeader.PROTOBUF.parse(first.blockHeaderOrThrow()).number();
        } catch (final ParseException e) {
            return -1L;
        }
    }

    /// The recent-snapshot directory path for a given block number.
    ///
    /// @param blockNumber the block number
    /// @return the directory path under `snapshotRecentPath` for that block
    @NonNull
    private Path recentSnapshotDirectoryFor(final long blockNumber) {
        return Path.of(config.snapshotRecentPath(), Long.toString(blockNumber));
    }

    /// The root hash of a sealed state, or empty if the state/root is absent or not yet hashed.
    ///
    /// @param state the state to hash (may be `null`)
    /// @return the root hash bytes, or empty
    @NonNull
    private static Bytes rootHashOf(@Nullable final VirtualMapState state) {
        if (state == null || state.getRoot() == null) {
            return Bytes.EMPTY;
        }
        try {
            return Bytes.wrap(state.getRoot().getHash().copyToByteArray());
        } catch (final RuntimeException e) {
            // Returning empty is fail-safe — the next block's footer validation will not match
            // an empty hash, so we halt apply rather than expose a wrong root.
            LOGGER.log(System.Logger.Level.WARNING, "Failed to read state root hash; treating as empty", e);
            return Bytes.EMPTY;
        }
    }

    /// The node count of a state's root, or `0` if the state/root is absent.
    ///
    /// @param state the state to measure (may be `null`)
    /// @return the root size, or `0`
    private static long sizeOf(@Nullable final VirtualMapState state) {
        if (state == null || state.getRoot() == null) {
            return 0L;
        }
        return state.getRoot().size();
    }
}
