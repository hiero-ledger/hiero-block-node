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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
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

    /// `blocknode:state_pending_blocks` — blocks buffered awaiting apply.
    static final MetricKey<ObservableGauge> METRIC_PENDING_BLOCKS =
            MetricKey.of("state_pending_blocks", ObservableGauge.class).addCategory(METRICS_CATEGORY);

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
    /// reader never pairs a new state with a stale number — see `publishAttested`. `state` is
    /// `null` until first attestation. `blockNumber`/`roundNumber` are read directly from the
    /// state itself (or the snapshot directory name on restore) rather than tracked as an
    /// independent copy — see `docs/design/state/live-state.md` §5.1. No reservation on `state`
    /// for readers yet; a future query API must add one before reading `state` off the
    /// apply-worker thread.
    record AttestedSnapshot(@Nullable VirtualMapState state, long blockNumber, long roundNumber) {}

    private static final AttestedSnapshot GENESIS_ATTESTED = new AttestedSnapshot(null, -1L, -1L);

    /// Volatile because reads happen on query threads.
    private volatile AttestedSnapshot attested = GENESIS_ATTESTED;

    private final ConcurrentSkipListMap<Long, BlockUnparsed> pendingBlocks = new ConcurrentSkipListMap<>();
    private final AtomicBoolean stateIsCaughtUp = new AtomicBoolean(false);
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean applyHalted = new AtomicBoolean(false);
    private final AtomicLong hashMismatchTotal = new AtomicLong();

    /// Wall-clock duration of the most recent block apply, exported via
    /// `state_apply_latency_ms`. Volatile: written by the apply thread, read by the
    /// metrics-scrape thread.
    private volatile long lastApplyDurationMs = 0L;

    /// Exported counter measurement for footer hash mismatches, registered in
    /// `init` (always before any apply runs in `start`).
    private LongCounter.Measurement hashMismatchMetric;

    /// Dedicated apply worker. Runs one-shot historical catch-up, then blocks on `arrivals`
    /// waiting for verified blocks and applies them immediately. `null` until `start()`.
    private ExecutorService applyWorker;

    /// Background executor for work that must not block the apply worker: gap retries today.
    /// `null` until `start()`.
    private ScheduledExecutorService backgroundExecutor;

    /// Guards against piling up duplicate gap-retry tasks while one is already scheduled.
    private final AtomicBoolean gapRetryScheduled = new AtomicBoolean(false);

    /// Set by `retryGap` (on `backgroundExecutor`), consumed by `runApplyWorker` (on
    /// `StateManagement-apply`), which is where `sweepHistoricalBlocks` actually runs. Keeps the
    /// sweep off `backgroundExecutor`: that thread also runs background state hashing
    /// (`startBackgroundHash`), and a sweep applying several blocks in a row can queue a hash
    /// task behind itself there, then block waiting on it — a self-deadlock.
    private final AtomicBoolean gapSweepRequested = new AtomicBoolean(false);

    /// Wake-up channel for the apply worker: `handleVerification` stages the block in
    /// `pendingBlocks` and offers its number here so the worker wakes immediately instead of
    /// waiting for a timer. Unbounded so the messaging dispatch thread never blocks handing off.
    private final BlockingQueue<Long> arrivals = new LinkedBlockingQueue<>();

    /// Serialises `applyPending` so the apply worker and direct (test-driven) callers never
    /// mutate the lifecycle-managed state concurrently.
    private final ReentrantLock applyLock = new ReentrantLock();

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
    // A state is hashed exactly once, never while mutable (3); hashing starts in the
    // background right after seal (hashingFuture) — see `startBackgroundHash`.

    /// **State (2) — sealed, awaiting attestation.** `null` until the first apply.
    private volatile VirtualMapState hashingImmutable;

    /// Background hash of `hashingImmutable`, started right after it is sealed — see
    /// `startBackgroundHash`/`currentStagedHash`. `null` only before the first seal.
    private volatile Future<Bytes> hashingFuture;

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
        metrics.register(ObservableGauge.builder(METRIC_PENDING_BLOCKS)
                .setDescription("Blocks buffered awaiting apply")
                .observe(pendingBlocks::size));
        metrics.register(ObservableGauge.builder(METRIC_READY)
                .setDescription("1 once start-up catch-up is complete, else 0")
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
        backgroundExecutor = context.threadPoolManager()
                .createSingleThreadScheduledExecutor(
                        "StateManagement-background",
                        (t, e) -> LOGGER.log(System.Logger.Level.WARNING, "Uncaught exception in " + t.getName(), e));
        loadPersistedState();
        context.blockMessaging().registerBlockNotificationHandler(this, true, name());

        // Single apply worker: runs one-shot historical catch-up, then blocks on `arrivals`
        // and drains `pendingBlocks` the instant a verified block shows up — no polling.
        applyWorker = context.threadPoolManager().createSingleThreadExecutor("StateManagement-apply");
        applyWorker.submit(this::runApplyWorker);
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
        stopApplyWorker();
        if (backgroundExecutor != null) {
            backgroundExecutor.shutdownNow();
        }
    }

    /// {@inheritDoc}
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        // Halted apply means pendingBlocks is frozen — restart re-derives it from historical
        // storage, so there is nothing to lose by not staging more here.
        if (!notification.success() || notification.block() == null || applyHalted.get()) {
            return;
        }
        // At or below the last applied block (a resend, or a backfill-sourced verification):
        // applyPending only ever removes expectedNext, so staging it would leak it forever and
        // eventually fill pendingBlocks to the cap.
        final long lastApplied = lastAppliedBlock;
        if (lastApplied >= 0L && notification.blockNumber() <= lastApplied) {
            return;
        }
        if (pendingBlocks.size() >= config.pendingBlocksMax()
                && !pendingBlocks.containsKey(notification.blockNumber())) {
            // Over the cap: drop rather than buffer unboundedly — backfill (scheduleGapRetry)
            // will eventually re-fetch this block from history anyway.
            LOGGER.log(
                    System.Logger.Level.WARNING,
                    "pendingBlocks at cap ({0}); dropping live block {1}",
                    config.pendingBlocksMax(),
                    notification.blockNumber());
            return;
        }
        pendingBlocks.put(notification.blockNumber(), notification.block());
        arrivals.offer(notification.blockNumber());
    }

    // ── Package-private observers ─────────────────────────────────────────────
    // Tests drive the plugin through its real entry points (applyPending, block delivery) and
    // assert against these observers.

    /// Whether historical catch-up has completed.
    ///
    /// @return `true` once catch-up has finished
    boolean isReady() {
        return stateIsCaughtUp.get();
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

    /// The `startOfBlockStateRootHash` the next block's footer must carry to be accepted — the
    /// root hash of the most recently applied (staged) block (state 2), one ahead of `metadata`
    /// under lag-1. Empty before the first apply.
    ///
    /// @return the staged state root hash, or empty before the first apply
    @NonNull
    Bytes stagedStateRootHash() {
        return currentStagedHash();
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

    /// Number of blocks buffered awaiting apply.
    ///
    /// @return the current `pendingBlocks` size
    int pendingBlocksSize() {
        return pendingBlocks.size();
    }

    // ── Internals ───────────────────────────────────────────────────────────

    /// Restore from the highest-numbered recent-snapshot directory, if any exist, seeding the
    /// lag-1 bookkeeping so the loaded state is exposed on boot. A fresh node (no directories at
    /// all) falls back to genesis; a directory that fails to load or whose own
    /// `BlockStreamInfo.block_number` disagrees with its directory name halts instead, since
    /// silently falling back to genesis would discard the operator's recorded intent — see
    /// `docs/design/state/live-state.md` §6.2.
    private void loadPersistedState() {
        final long restoreCandidate = highestNumberedSnapshotDirectory();
        boolean snapshotLoaded = false;
        if (restoreCandidate >= 0L) {
            final Path snapshotDir = recentSnapshotDirectoryFor(restoreCandidate);
            try {
                lifecycleManager.loadSnapshot(snapshotDir);
                final long loadedBlockNumber = blockNumberOf(lifecycleManager.getLatestImmutableState());
                if (loadedBlockNumber == restoreCandidate) {
                    snapshotLoaded = true;
                    lastAppliedBlock = restoreCandidate;
                    LOGGER.log(System.Logger.Level.INFO, "Loaded state snapshot from {0}", snapshotDir);
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
            startBackgroundHash(loaded);
        } else if (!applyHalted.get()) {
            // True genesis: seed state 2 with the sealed empty VirtualMap so block 0 validates
            // through the same path as every other block. Skipped if we just halted above, so
            // hashingImmutable stays null rather than surfacing an untrustworthy hash.
            hashingImmutable = lifecycleManager.getLatestImmutableState();
            startBackgroundHash(hashingImmutable);
        }
    }

    /// Scan `${config.snapshotRecentPath()}` for numerically-named directories and return the
    /// highest one.
    ///
    /// @return the highest-numbered snapshot directory's block number, or `-1` if none exist
    private long highestNumberedSnapshotDirectory() {
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
        } catch (final IOException e) {
            LOGGER.log(
                    System.Logger.Level.WARNING,
                    "Unable to scan {0} for snapshot directories; treating as genesis",
                    root,
                    e);
            return -1L;
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

    /// Drain the pending-blocks queue in strict block-number order via `applyBlockStateChanges`.
    /// Stops on the first gap, when stopping, or when apply halts. A halt is terminal (the
    /// mutable state may be partially mutated; recovery is a restart from the last snapshot),
    /// so a failed block stays queued for inspection rather than retried.
    void applyPending() {
        applyLock.lock();
        try {
            while (!pendingBlocks.isEmpty() && !stopping.get() && !applyHalted.get()) {
                final boolean atGenesis = lastAppliedBlock < 0L;
                final long expectedNext =
                        atGenesis ? pendingBlocks.firstKey() : Math.max(lastAppliedBlock, attested.blockNumber()) + 1L;
                if (!atGenesis) {
                    // Backstop for handleVerification's stale check (it can race an apply): drop
                    // anything below expectedNext so it can't keep pendingBlocks non-empty and
                    // trigger spurious gap retries.
                    pendingBlocks.headMap(expectedNext).clear();
                    if (pendingBlocks.isEmpty()) {
                        return;
                    }
                }
                // Peek (not remove) so a failure leaves the block for inspection, not retry.
                final BlockUnparsed block = pendingBlocks.get(expectedNext);
                if (block == null) {
                    scheduleGapRetry(expectedNext);
                    return;
                }
                final boolean applied;
                try {
                    applied = applyBlockStateChanges(block);
                } catch (final RuntimeException e) {
                    LOGGER.log(
                            System.Logger.Level.WARNING,
                            "applyBlockStateChanges threw for block {0}; halting apply (restart required to recover)",
                            expectedNext,
                            e);
                    applyHalted.set(true);
                    return;
                }
                if (!applied) {
                    // Block-specific failure already logged + applyHalted set (e.g. hash
                    // mismatch); leave the block in the queue and stop draining.
                    return;
                }
                pendingBlocks.remove(expectedNext);
            }
        } finally {
            applyLock.unlock();
        }
    }

    /// Body of the apply worker thread. Runs historical catch-up once, then blocks on
    /// `arrivals`, applying each newly-arrived block via `applyPending`. A burst of arrivals
    /// is coalesced into a single drain. Exits cleanly on interrupt/stop.
    private void runApplyWorker() {
        // One-shot catch-up first (it manages its own failures and flips readiness in a
        // finally, so it never throws out here).
        catchUpFromHistoricalBlocks();
        while (!stopping.get()) {
            try {
                arrivals.take(); // park (no CPU) until a block arrives
                arrivals.clear(); // coalesce any burst — pendingBlocks already holds them
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            // A gap-retry request runs the sweep here, not on backgroundExecutor — see
            // gapSweepRequested's javadoc.
            if (gapSweepRequested.compareAndSet(true, false) && !applyHalted.get()) {
                try {
                    sweepHistoricalBlocks();
                } catch (final RuntimeException e) {
                    LOGGER.log(System.Logger.Level.WARNING, "Gap-recovery sweep failed", e);
                }
            }
            applyPending();
        }
    }

    /// Stop the apply worker: interrupt it and wait briefly. Best-effort; no-ops if never
    /// started.
    private void stopApplyWorker() {
        if (applyWorker == null) {
            return;
        }
        applyWorker.shutdownNow();
        try {
            applyWorker.awaitTermination(2L, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /// Bring the live state up to the latest block on hand, reading from `HistoricalBlockFacility`
    /// through the same `applyPending` loop that handles verification-delivered blocks. Runs on
    /// the apply-worker thread; sets `stateIsCaughtUp` when complete (or nothing to catch up to).
    void catchUpFromHistoricalBlocks() {
        try {
            sweepHistoricalBlocks();
        } catch (final RuntimeException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Catch-up failed; plugin remains not ready", e);
        } finally {
            // Ready means caught up AND not halted, so state_ready can't be confused with a
            // caught-up-then-halted state.
            if (applyHalted.get()) {
                LOGGER.log(System.Logger.Level.WARNING, "Catch-up halted; plugin remains not ready");
            } else {
                stateIsCaughtUp.set(true);
            }
        }
    }

    /// Walk forward from the next expected block to whatever `historicalBlockProvider` currently
    /// has, applying in `historicCatchUpBatchSize` batches via `applyPending`. Shared by startup
    /// catch-up and live gap recovery, so a deep gap converges in one sweep instead of trickling
    /// in one block per `gapRetryDelaySeconds`. Gap recovery runs this from `runApplyWorker`
    /// (via `gapSweepRequested`), never directly from `retryGap` on `backgroundExecutor` — see
    /// `gapSweepRequested`'s javadoc for why.
    ///
    /// Thread-safe regardless of caller (`applyPending` serialises via `applyLock`).
    /// Stops at the first batch `applyPending` doesn't fully clear (a hole in `availableBlocks`,
    /// a fetch failure, or a hash-mismatch halt) instead of walking the cursor past it — otherwise
    /// later batches keep fetching and buffering blocks into `pendingBlocks` that can never be
    /// applied until the stalled block resolves.
    /// Package-private so tests can drive gap recovery directly.
    void sweepHistoricalBlocks() {
        final HistoricalBlockFacility historic = context == null ? null : context.historicalBlockProvider();
        if (historic == null) {
            return;
        }
        final BlockRangeSet available = historic.availableBlocks();
        if (available == null || available.size() == 0L) {
            return;
        }
        final long latest = available.max();
        final boolean atGenesis = lastAppliedBlock < 0L;
        final long start = atGenesis ? available.min() : Math.max(lastAppliedBlock, attested.blockNumber()) + 1L;
        if (start > latest) {
            return;
        }
        final int batchSize = Math.max(1, config.historicCatchUpBatchSize());
        long cursor = start;
        while (cursor <= latest && !stopping.get() && !applyHalted.get()) {
            final long batchEnd = Math.min(cursor + batchSize - 1L, latest);
            for (long i = cursor; i <= batchEnd; i++) {
                if (pendingBlocks.containsKey(i)) {
                    continue; // already supplied by a verification notification
                }
                enqueueHistoricalBlock(historic, i);
            }
            applyPending();
            if (lastAppliedBlock < batchEnd) {
                break;
            }
            cursor = batchEnd + 1L;
        }
    }

    /// A gap in `pendingBlocks` is expected, not an error — backfill fills it eventually. Retries
    /// after a delay rather than waiting forever on a notification that may never arrive for
    /// this exact block number. Coalesced: only one retry is ever in flight.
    ///
    /// @param expectedNext the missing block number to retry
    private void scheduleGapRetry(final long expectedNext) {
        if (backgroundExecutor == null || stopping.get() || !gapRetryScheduled.compareAndSet(false, true)) {
            return;
        }
        LOGGER.log(
                System.Logger.Level.WARNING,
                "Block {0} missing; retrying from historical storage in {1}s",
                expectedNext,
                config.gapRetryDelaySeconds());
        backgroundExecutor.schedule(() -> retryGap(expectedNext), config.gapRetryDelaySeconds(), TimeUnit.SECONDS);
    }

    /// Request a gap-recovery sweep and wake the apply worker. Runs on `backgroundExecutor`
    /// (the gap-retry timer thread), so it only flags the request — `runApplyWorker` runs the
    /// actual sweep, pulling everything available from the next expected block onward (not just
    /// the block that was missing), so blocks that arrived mid-delay are also picked up in one
    /// pass.
    ///
    /// @param expectedNext the block number that was missing when this retry was scheduled
    ///     (used only to wake the apply worker — the sweep recomputes the live starting point
    ///     itself)
    private void retryGap(final long expectedNext) {
        gapRetryScheduled.set(false);
        gapSweepRequested.set(true);
        arrivals.offer(expectedNext);
    }

    /// Fetch a single historical block via its `BlockAccessor` and enqueue it in
    /// `pendingBlocks`. No-ops if the block is unavailable or cannot be unparsed.
    ///
    /// @param historic the historical block facility to read from
    /// @param blockNumber the block number to fetch and enqueue
    private void enqueueHistoricalBlock(@NonNull final HistoricalBlockFacility historic, final long blockNumber) {
        final BlockAccessor accessor = historic.block(blockNumber);
        if (accessor == null) {
            return;
        }
        try {
            final BlockUnparsed block = accessor.blockUnparsed();
            if (block != null) {
                pendingBlocks.put(blockNumber, block);
            }
        } finally {
            try {
                accessor.close();
            } catch (final Exception ignored) {
                // close failures during catch-up are non-fatal.
            }
        }
    }

    /// Apply a single block's `state_changes` to the live state under the **lag-1 commit**
    /// model: a block is applied into the live mutable, but only exposed once the *next*
    /// block's footer attests its root hash.
    ///
    /// Flow for block N (given block N-1 was the last applied, post-(N-1) sealed):
    ///
    /// 1. Pull `BlockFooter.startOfBlockStateRootHash` from N.
    /// 2. Join state 2's hash (`hashingImmutable`, post-(N-1)), computed in the background since
    ///    it was sealed, and validate N's footer against it. Mismatch ⇒ halt without mutating.
    /// 3. Promote state 2 → state 1 (`attested`), publishing its state and metadata together.
    /// 4. Apply N's `state_changes` to the live mutable, then seal it as the new state 2 —
    ///    left un-hashed and un-exposed until block N+1 attests it.
    ///
    /// @param block the block to apply
    /// @return `true` on successful apply, `false` if rejected (caller leaves it queued)
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
        // Join state 2's hash, started in the background after it was sealed; hashingImmutable
        // is seeded at boot, so this is never null here.
        final Bytes attestedHash = currentStagedHash();
        if (!validateStartHash(startHash, attestedHash)) {
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
        final StateChangeApplier.ApplyResult result;
        try {
            result = applier.applyBlock(mutable, block);
        } catch (final StateChangeApplier.UnsupportedLegacyEncodingException e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Refusing to apply block {0}: {1}; apply-halted",
                    incomingBlock,
                    e.getMessage());
            applyHalted.set(true);
            return false;
        }
        if (result.blockNumber() < 0L) {
            LOGGER.log(System.Logger.Level.WARNING, "Block had unparseable header; treating as failed apply");
            applyHalted.set(true);
            return false;
        }
        lifecycleManager.copyMutableState();
        hashingImmutable = lifecycleManager.getLatestImmutableState();
        startBackgroundHash(hashingImmutable);
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

    /// Start hashing `state` on `backgroundExecutor`, right after it is sealed, so the hash
    /// overlaps with whatever happens next instead of blocking the apply path. `VirtualMap`
    /// caches its hash, so this computes it exactly once either way.
    ///
    /// @param state the newly-sealed state to hash
    private void startBackgroundHash(@NonNull final VirtualMapState state) {
        hashingFuture = backgroundExecutor == null
                ? CompletableFuture.completedFuture(rootHashOf(state))
                : backgroundExecutor.submit(() -> rootHashOf(state));
    }

    /// Join `hashingFuture` for `hashingImmutable`'s root hash. Falls back to hashing
    /// synchronously if no background computation was ever started.
    ///
    /// @return the root hash of `hashingImmutable`, or empty before the first apply
    @NonNull
    private Bytes currentStagedHash() {
        final Future<Bytes> future = hashingFuture;
        if (future == null) {
            return rootHashOf(hashingImmutable);
        }
        try {
            return future.get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return rootHashOf(hashingImmutable);
        } catch (final ExecutionException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Background state hash failed; hashing synchronously", e);
            return rootHashOf(hashingImmutable);
        }
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
