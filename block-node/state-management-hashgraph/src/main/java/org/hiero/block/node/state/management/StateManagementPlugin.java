// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.hedera.hapi.block.stream.output.BlockHeader;
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
import org.hiero.block.api.StateMetadata;
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
import org.hiero.consensus.metrics.noop.NoOpMetrics;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// State plugin that maintains a live, queryable copy of Hashgraph network state inside
/// the Block Node by replaying verified block-stream state changes onto a
/// `VirtualMapStateLifecycleManager`-managed state.
///
/// This slice covers lifecycle, the apply pipeline, and snapshot restore on startup.
/// Snapshot creation and the gRPC query surface land in follow-up PRs.
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

    private BlockNodeContext context;
    private StateManagementConfig config;
    private VirtualMapStateLifecycleManager lifecycleManager;
    private StateMetadataStore metadataStore;
    private StateChangeApplier applier;

    /// State (1) and its metadata, published as one unit so a reader can never pair a new state
    /// with stale metadata (or vice versa) — see `publishAttested`. `state` is `null` until the
    /// first attestation. Does not yet give readers their own reservation on `state`; no query
    /// path exists in this PR to need one, but the gRPC query API PR must add one before reading
    /// `state` from any thread other than the apply worker.
    private record AttestedSnapshot(
            @Nullable VirtualMapState state, @NonNull StateMetadata metadata) {}

    private static final AttestedSnapshot GENESIS_ATTESTED = new AttestedSnapshot(null, StateMetadata.DEFAULT);

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

    private static final long GAP_RETRY_DELAY_SECONDS = 5L;

    /// Wake-up channel for the apply worker. `handleVerification` stages the block in
    /// `pendingBlocks` (the ordered gap buffer `applyPending` drains) and offers the block
    /// number here so the worker wakes immediately instead of waiting for a timer. Unbounded so
    /// the messaging dispatch thread never blocks handing off.
    private final BlockingQueue<Long> arrivals = new LinkedBlockingQueue<>();

    /// Serialises `applyPending` so the apply worker and direct (test-driven) callers never
    /// mutate the lifecycle-managed state concurrently.
    private final ReentrantLock applyLock = new ReentrantLock();

    /// Block number of the most recent block whose state_changes have been applied to the
    /// mutable state. Distinct from `metadata.blockNumber` because of the lag-1 commit:
    /// `metadata.blockNumber` is the most recently *attested* (visible to readers) block;
    /// `lastAppliedBlock` is the staging block whose changes live in the mutable copy and will
    /// be committed when the next block's footer confirms them. `-1` until the first apply.
    private volatile long lastAppliedBlock = -1L;

    /// Round number of `lastAppliedBlock`, mirrored for the same lag-1 reason.
    private volatile long lastAppliedRound = -1L;

    // ── Three concurrent state versions ────────────────────────────────────────
    // (3) MUTABLE — `lifecycleManager.getMutableState()`; receives the current block's
    //     state_changes (library-managed, no field here).
    // (2) HASHING — `hashingImmutable`; sealed by copyMutableState, awaiting
    //     attestation by the next block's footer.
    // (1) ATTESTED — `attested.state()`; the network-confirmed, query-serving copy.
    // A state is hashed exactly once, never while mutable (3). VirtualMap.getHash() caches, so
    // hashing costs the same either way — the only choice is *when* to pay it. Hashing starts
    // in the background right after seal (hashingFuture), overlapping with the next block's
    // apply, instead of blocking the apply path behind a synchronous hash.

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
        this.metadataStore = new StateMetadataStore(Path.of(config.metadataPath()));
        this.applier = new StateChangeApplier();
        try {
            // Anchor FileSystemManager at the configured recent-snapshot path so the lifecycle
            // manager's scratchpad lives next to the snapshots. First filesystem touch — fails
            // here if the configured state directory is not writable.
            final Path fsmRoot = Path.of(config.snapshotRecentPath());
            this.lifecycleManager = new VirtualMapStateLifecycleManager(
                    new NoOpMetrics(), Time.getCurrent(), context.configuration(), new FileSystemManager(fsmRoot));
        } catch (final RuntimeException e) {
            // Non-fatal: this plugin stays inert (lifecycleManager == null, start() no-ops) but
            // the rest of the Block Node — block streaming included — is unaffected.
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
                .observe(() -> attested.metadata().blockNumber()));
        metrics.register(ObservableGauge.builder(METRIC_STATE_SIZE)
                .setDescription("Node count of the latest network-attested state")
                .observe(() -> attested.metadata().stateSize()));
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
        if (pendingBlocks.size() >= config.pendingBlocksMax()
                && !pendingBlocks.containsKey(notification.blockNumber())) {
            // Over the cap: drop rather than buffer unboundedly. Whatever gap is holding up
            // apply will eventually be filled by backfill (see scheduleGapRetry), at which
            // point this block number is re-fetched from historicalBlockProvider anyway.
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

    /// The latest applied state metadata.
    ///
    /// @return the current `StateMetadata`
    @NonNull
    StateMetadata metadata() {
        return attested.metadata();
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

    // ── Internals ───────────────────────────────────────────────────────────

    /// Load persisted metadata and, if present, the matching recent snapshot from disk,
    /// seeding the lag-1 bookkeeping so the loaded (already-attested) state is exposed on boot.
    /// Falls back to genesis when metadata is missing/unreadable or its snapshot cannot be
    /// loaded.
    private void loadPersistedState() {
        StateMetadata loadedMetadata;
        try {
            loadedMetadata = metadataStore.load().orElse(StateMetadata.DEFAULT);
        } catch (final IOException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Unable to load state metadata; starting from genesis", e);
            loadedMetadata = StateMetadata.DEFAULT;
        }
        boolean snapshotLoaded = false;
        final Path snapshotDir = recentSnapshotDirectoryFor(loadedMetadata.blockNumber());
        if (Files.isDirectory(snapshotDir)) {
            try {
                final Bytes onDiskHash =
                        lifecycleManager.loadSnapshot(snapshotDir).getBytes();
                if (onDiskHash.equals(loadedMetadata.stateRootHash())) {
                    snapshotLoaded = true;
                    LOGGER.log(System.Logger.Level.INFO, "Loaded state snapshot from {0}", snapshotDir);
                } else {
                    // Stale/corrupt snapshot paired with newer metadata. Unrecoverable in place,
                    // same as a block-apply hash mismatch: halt rather than serve wrong data.
                    LOGGER.log(
                            System.Logger.Level.ERROR,
                            "Snapshot at {0} has root hash {1}, metadata expects {2}; halting",
                            snapshotDir,
                            onDiskHash.toHex(),
                            loadedMetadata.stateRootHash().toHex());
                    hashMismatchTotal.incrementAndGet();
                    hashMismatchMetric.increment();
                    applyHalted.set(true);
                }
            } catch (final IOException e) {
                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "Snapshot at {0} unreadable; continuing with eager genesis state",
                        snapshotDir,
                        e);
            }
        }
        if (!snapshotLoaded) {
            // Metadata may point at a block we couldn't actually load — treat as genesis so the
            // lag-1 commit pipeline doesn't claim we already have state we don't.
            loadedMetadata = StateMetadata.DEFAULT;
        } else {
            // Trust the loaded snapshot represents post-(metadata.blockNumber), an
            // attested/committed block when snapshotted. Seed the lag-1 bookkeeping so it is
            // already the exposed state on boot and the next block validates against it.
            lastAppliedBlock = loadedMetadata.blockNumber();
            lastAppliedRound = loadedMetadata.roundNumber();
        }
        // Force the initial copyMutableState so getLatestImmutableState() is non-null from boot
        // onwards (and so the live mutable is a writable copy).
        lifecycleManager.copyMutableState();
        if (snapshotLoaded) {
            // The loaded (already-attested) block is simultaneously state 1 (exposed to
            // readers) and state 2 (the base the next block's footer validates against).
            final VirtualMapState loaded = lifecycleManager.getLatestImmutableState();
            publishAttested(loaded, loadedMetadata);
            hashingImmutable = loaded;
            startBackgroundHash(loaded);
        } else if (!applyHalted.get()) {
            // True genesis: seed state 2 with the sealed empty VirtualMap so block 0 validates
            // through the same path as every other block, no special-casing needed. Skipped if
            // we just halted on a corrupt snapshot above, so hashingImmutable stays null rather
            // than surfacing a hash we've declared untrustworthy.
            hashingImmutable = lifecycleManager.getLatestImmutableState();
            startBackgroundHash(hashingImmutable);
        }
    }

    /// Drain the pending-blocks queue in strict block-number order, applying each contiguous
    /// next block via `applyBlockStateChanges`. Stops on the first gap, when stopping, or when
    /// apply is halted. A failed block stays in `pendingBlocks` for post-mortem inspection, not
    /// retry — a halt is terminal, since the mutable state may already be partially mutated;
    /// recovery is a restart from the last snapshot.
    void applyPending() {
        applyLock.lock();
        try {
            while (!pendingBlocks.isEmpty() && !stopping.get() && !applyHalted.get()) {
                final StateMetadata metadata = attested.metadata();
                final boolean atGenesis = lastAppliedBlock < 0L
                        && metadata.blockNumber() == 0L
                        && metadata.stateRootHash().length() == 0L;
                final long expectedNext =
                        atGenesis ? pendingBlocks.firstKey() : Math.max(lastAppliedBlock, metadata.blockNumber()) + 1L;
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
    /// `arrivals` and applies each newly-arrived verified block immediately via
    /// `applyPending`. `arrivals.take()` parks the thread with zero CPU until
    /// `handleVerification` offers a block number; a burst of arrivals is coalesced into a
    /// single drain. Exits cleanly on interrupt/stop.
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

    /// Bring the live state up to the latest block the Block-Node has on hand by reading
    /// blocks from `HistoricalBlockFacility` and feeding them through the same `applyPending`
    /// loop that handles verification-delivered blocks.
    ///
    /// Runs on the apply-worker thread. When complete (or if there is nothing to catch up to)
    /// sets `stateIsCaughtUp`.
    void catchUpFromHistoricalBlocks() {
        try {
            final HistoricalBlockFacility historic = context == null ? null : context.historicalBlockProvider();
            if (historic == null) {
                return;
            }
            final BlockRangeSet available = historic.availableBlocks();
            if (available == null || available.size() == 0L) {
                return;
            }
            final long latest = available.max();
            final StateMetadata metadata = attested.metadata();
            final boolean atGenesis =
                    metadata.blockNumber() == 0L && metadata.stateRootHash().length() == 0L;
            final long start = atGenesis ? available.min() : metadata.blockNumber() + 1L;
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
                cursor = batchEnd + 1L;
            }
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

    /// A gap in `pendingBlocks` is expected, not an error: backfill fills it eventually. Log a
    /// warning and retry after a delay rather than waiting forever on a live notification that
    /// may never arrive for this exact block number. Coalesced — only one retry is ever in
    /// flight.
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
                GAP_RETRY_DELAY_SECONDS);
        backgroundExecutor.schedule(() -> retryGap(expectedNext), GAP_RETRY_DELAY_SECONDS, TimeUnit.SECONDS);
    }

    /// Re-check a previously missing block: pull it from `historicalBlockProvider` if backfill
    /// has it by now, then wake the apply worker either way so `applyPending` re-evaluates (and
    /// reschedules another retry if it is still missing).
    ///
    /// @param expectedNext the block number to re-check
    private void retryGap(final long expectedNext) {
        gapRetryScheduled.set(false);
        if (!stopping.get() && !applyHalted.get() && !pendingBlocks.containsKey(expectedNext)) {
            final HistoricalBlockFacility historic = context == null ? null : context.historicalBlockProvider();
            if (historic != null) {
                enqueueHistoricalBlock(historic, expectedNext);
            }
        }
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
        // Join the hash of state 2 (post-(lastAppliedBlock)), started in the background right
        // after it was sealed. hashingImmutable is seeded at boot (genesis or loaded snapshot),
        // so this is never null by the time a block reaches apply.
        final Bytes attestedHash = currentStagedHash();
        if (!validateStartHash(startHash, attestedHash)) {
            hashMismatchTotal.incrementAndGet();
            hashMismatchMetric.increment();
            applyHalted.set(true);
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "State hash mismatch applying block {0}: footer.startOfBlockStateRootHash ({1}) diverges "
                            + "from the last applied state's root hash ({2}, post-block {3}); plugin marked "
                            + "apply-halted (state not exposed)",
                    incomingBlock,
                    startHash.toHex(),
                    attestedHash.toHex(),
                    lastAppliedBlock);
            return false;
        }

        // N's footer just attested state 2 (post-(lastAppliedBlock)). Promote it from HASHING
        // → ATTESTED now — lag-1: readers only ever see attested state. (Skipped at genesis,
        // and when the last applied block is already the exposed one, e.g. after a snapshot
        // reload.)
        final StateMetadata currentMetadata = attested.metadata();
        if (lastAppliedBlock >= 0L && (attested.state() == null || lastAppliedBlock != currentMetadata.blockNumber())) {
            publishAttested(
                    hashingImmutable,
                    StateMetadata.newBuilder()
                            .blockNumber(lastAppliedBlock)
                            .roundNumber(lastAppliedRound < 0L ? currentMetadata.roundNumber() : lastAppliedRound)
                            .stateRootHash(attestedHash)
                            .stateSize(sizeOf(hashingImmutable))
                            .build());
        }

        // Apply this block into the live mutable (state 3) and fast-copy to seal it as the new
        // state 2, but DO NOT hash or expose it: it stays staged until block N+1 attests it.
        final VirtualMapState mutable = lifecycleManager.getMutableState();
        final StateChangeApplier.ApplyResult result = applier.applyBlock(mutable, block);
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

    /// Publish `newState` and `newMetadata` as one atomic unit — a single volatile write to
    /// `attested` — so a reader can never pair a new state with stale metadata or vice versa.
    /// Also manages reference counts so the state survives the next `copyMutableState()` (which
    /// releases the lifecycle manager's own reference to the superseded version): `reserve()`s
    /// the new state's root and `release()`s the previously-held one — without this the held
    /// immutable is destroyed and reads/snapshots throw `ReferenceCountException`.
    ///
    /// @param newState the newly-attested state to publish to readers
    /// @param newMetadata the metadata describing `newState`
    private void publishAttested(@NonNull final VirtualMapState newState, @NonNull final StateMetadata newMetadata) {
        final AttestedSnapshot previous = attested;
        newState.getRoot().reserve();
        attested = new AttestedSnapshot(newState, newMetadata);
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
