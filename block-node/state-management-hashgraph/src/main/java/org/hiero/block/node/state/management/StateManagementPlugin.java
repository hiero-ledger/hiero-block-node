// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.hiero.base.file.FileSystemManager;
import org.hiero.block.api.StateMetadata;
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

/// Beta plugin that maintains a live, queryable copy of Hashgraph network state inside
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

    /// Latest applied state metadata. Volatile because reads happen on query threads.
    ///
    /// Concurrency note (accepted for beta): `metadata` and `attestedImmutable` are updated as
    /// two separate volatile writes in the lag-1 commit (see `applyBlockStateChanges`), not one
    /// atomic swap. Both are only ever written from the single apply thread, so the window is
    /// tiny; worst case is a stale-but-attested read, never a crash.
    private volatile StateMetadata metadata = StateMetadata.DEFAULT;

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
    private Thread applyWorker;

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
    /// `metadata.blockNumber` is the most recently *committed* (visible to readers) block;
    /// `lastAppliedBlock` is the staging block whose changes live in the mutable copy and will
    /// be committed when the next block's footer confirms them. `-1` until the first apply.
    private volatile long lastAppliedBlock = -1L;

    /// Round number of `lastAppliedBlock`, mirrored for the same lag-1 reason.
    private volatile long lastAppliedRound = -1L;

    // ── Three concurrent state versions ────────────────────────────────────────
    // (3) MUTABLE — `lifecycleManager.getMutableState()`; receives the current block's
    //     state_changes (library-managed, no field here).
    // (2) HASHING — `hashingImmutable`; sealed by copyMutableState, awaiting hash +
    //     attestation by the next block's footer.
    // (1) ATTESTED — `attestedImmutable`; the network-confirmed, query-serving copy.
    // A state is hashed exactly once, when promoted from (2) to (1) — never while mutable (3)
    // and never eagerly at seal.

    /// **State (2) — sealed, awaiting hash/attestation.** Not hashed at seal time; its root
    /// hash is computed lazily, only when the next block's footer arrives to attest it. `null`
    /// until the first apply.
    private volatile VirtualMapState hashingImmutable;

    /// **State (1) — query-serving, network-attested.** Lags `lastAppliedBlock` by one block:
    /// a block is applied into the mutable (3), sealed into (2), and promoted here only once
    /// the *next* block's footer confirms its root hash. `null` until the first attestation.
    private volatile VirtualMapState attestedImmutable;

    /// {@inheritDoc}
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = context;
        this.config = context.configuration().getConfigData(StateManagementConfig.class);
        this.metadataStore = new StateMetadataStore(Path.of(config.stateMetadataPath()));
        this.applier = new StateChangeApplier();
        try {
            // Anchor FileSystemManager at the configured recent-snapshot path so the lifecycle
            // manager's scratchpad lives next to the snapshots. First filesystem touch — fails
            // here if the configured state directory is not writable.
            final Path fsmRoot = Path.of(config.stateSnapshotRecentPath());
            this.lifecycleManager = new VirtualMapStateLifecycleManager(
                    new NoOpMetrics(), Time.getCurrent(), context.configuration(), new FileSystemManager(fsmRoot));
        } catch (final RuntimeException e) {
            // Fatal misconfiguration — log and request a graceful node shutdown via the health
            // facility rather than throw out of init() (which would abort BlockNodeApp
            // construction with no plugin isolation).
            LOGGER.log(
                    System.Logger.Level.WARNING,
                    "Could not prepare state directory {0}; requesting Block Node shutdown.",
                    config.stateSnapshotRecentPath(),
                    e);
            context.serverHealth().shutdown(name(), "Could not prepare state directory");
        }
        registerMetrics(context.metricRegistry());
    }

    /// Register the plugin's metrics with the block node's registry.
    ///
    /// @param metrics the block node metric registry
    private void registerMetrics(@NonNull final MetricRegistry metrics) {
        metrics.register(ObservableGauge.builder(METRIC_APPLIED_BLOCK)
                .setDescription("Latest committed (reader-visible) block number applied to live state")
                .observe(() -> metadata.blockNumber()));
        metrics.register(ObservableGauge.builder(METRIC_STATE_SIZE)
                .setDescription("Node count of the latest network-attested state")
                .observe(() -> metadata.stateSize()));
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
            // init() could not prepare the state directory and requested a node shutdown;
            // there is nothing to start.
            return;
        }
        loadPersistedState();
        context.blockMessaging().registerBlockNotificationHandler(this, true, name());

        // Single apply worker: runs one-shot historical catch-up, then blocks on `arrivals`
        // and drains `pendingBlocks` the instant a verified block shows up — no polling.
        applyWorker = new Thread(this::runApplyWorker, "StateManagement-apply");
        applyWorker.setDaemon(true);
        applyWorker.start();
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
    }

    /// {@inheritDoc}
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        // Once apply is halted, applyPending() never drains pendingBlocks again until a restart
        // — staging more blocks in the meantime would only grow memory without bound, since
        // nothing durable is lost: catchUpFromHistoricalBlocks() re-derives pendingBlocks from
        // historical storage on the next start.
        if (!notification.success() || notification.block() == null || applyHalted.get()) {
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
        return metadata;
    }

    /// The `startOfBlockStateRootHash` the next block's footer must carry to be accepted — the
    /// root hash of the most recently applied (staged) block (state 2), one ahead of `metadata`
    /// under lag-1. Computed lazily from `hashingImmutable` (VirtualMap caches the hash after
    /// the first call). Empty before the first apply.
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

    /// Load persisted metadata and, if present, the matching recent snapshot from disk,
    /// seeding the lag-1 bookkeeping so the loaded (already-attested) state is exposed on boot.
    /// Falls back to genesis when metadata is missing/unreadable or its snapshot cannot be
    /// loaded.
    private void loadPersistedState() {
        try {
            metadata = metadataStore.load().orElse(StateMetadata.DEFAULT);
        } catch (final IOException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Unable to load state metadata; starting from genesis", e);
            metadata = StateMetadata.DEFAULT;
        }
        boolean snapshotLoaded = false;
        final Path snapshotDir = recentSnapshotDirectoryFor(metadata.blockNumber());
        if (Files.isDirectory(snapshotDir)) {
            try {
                lifecycleManager.loadSnapshot(snapshotDir);
                snapshotLoaded = true;
                LOGGER.log(System.Logger.Level.INFO, "Loaded state snapshot from {0}", snapshotDir);
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
            metadata = StateMetadata.DEFAULT;
        } else {
            // Trust the loaded snapshot represents post-(metadata.blockNumber), an
            // attested/committed block when snapshotted. Seed the lag-1 bookkeeping so it is
            // already the exposed state on boot and the next block validates against it.
            lastAppliedBlock = metadata.blockNumber();
            lastAppliedRound = metadata.roundNumber();
        }
        // Force the initial copyMutableState so getLatestImmutableState() is non-null from boot
        // onwards (and so the live mutable is a writable copy).
        lifecycleManager.copyMutableState();
        if (snapshotLoaded) {
            // The loaded (already-attested) block is simultaneously state 1 (exposed to
            // readers) and state 2 (the base the next block's footer validates against). Its
            // root hash is derived on demand from `hashingImmutable` when that next block
            // arrives — no eager hash here.
            final VirtualMapState loaded = lifecycleManager.getLatestImmutableState();
            setAttested(loaded);
            hashingImmutable = loaded;
        }
    }

    /// Drain the pending-blocks queue in strict block-number order, applying each contiguous
    /// next block via `applyBlockStateChanges`. Stops on the first gap, when stopping, or when
    /// apply is halted. Blocks are removed only after a successful apply so failures leave the
    /// block queued for inspection / retry.
    void applyPending() {
        applyLock.lock();
        try {
            while (!pendingBlocks.isEmpty() && !stopping.get() && !applyHalted.get()) {
                final boolean atGenesis = lastAppliedBlock < 0L
                        && metadata.blockNumber() == 0L
                        && metadata.stateRootHash().length() == 0L;
                final long expectedNext =
                        atGenesis ? pendingBlocks.firstKey() : Math.max(lastAppliedBlock, metadata.blockNumber()) + 1L;
                // Peek (not remove) so an applier failure leaves the block in the queue for
                // inspection / future retry rather than silently dropping it.
                final BlockUnparsed block = pendingBlocks.get(expectedNext);
                if (block == null) {
                    return;
                }
                final boolean applied;
                try {
                    applied = applyBlockStateChanges(block);
                } catch (final RuntimeException e) {
                    LOGGER.log(
                            System.Logger.Level.WARNING,
                            "applyBlockStateChanges threw for block {0}; halting apply (block retained in queue)",
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

    /// Stop the apply worker: interrupt it and join briefly. Best-effort; no-ops if never
    /// started.
    private void stopApplyWorker() {
        if (applyWorker == null) {
            return;
        }
        applyWorker.interrupt();
        try {
            applyWorker.join(TimeUnit.SECONDS.toMillis(2L));
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
                stateIsCaughtUp.set(true);
                return;
            }
            final BlockRangeSet available = historic.availableBlocks();
            if (available == null || available.size() == 0L) {
                stateIsCaughtUp.set(true);
                return;
            }
            final long latest = available.max();
            final boolean atGenesis =
                    metadata.blockNumber() == 0L && metadata.stateRootHash().length() == 0L;
            final long start = atGenesis ? available.min() : metadata.blockNumber() + 1L;
            if (start > latest) {
                stateIsCaughtUp.set(true);
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
            // Even if catch-up hit a gap or an apply-halted state, start serving queries
            // against whatever applied.
            stateIsCaughtUp.set(true);
        }
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
    /// 2. Hash state 2 (`hashingImmutable`, post-(N-1)) now and validate N's footer against it.
    ///    Mismatch ⇒ halt apply without mutating.
    /// 3. Promote state 2 → state 1 (`attestedImmutable`) and record its metadata.
    /// 4. Apply N's `state_changes` to the live mutable, then seal it as the new state 2 —
    ///    left un-hashed and un-exposed until block N+1 attests it.
    ///
    /// @param block the block to apply
    /// @return `true` on successful apply, `false` if rejected (caller leaves it queued)
    private boolean applyBlockStateChanges(@NonNull final BlockUnparsed block) {
        final long applyStartMs = System.currentTimeMillis();
        // Block number is pulled up-front purely for diagnostics — the apply path below
        // re-parses it from the header as the authoritative value.
        final long incomingBlock = StateChangeApplier.extractBlockNumber(block);
        final Bytes startHash = StateChangeApplier.extractStartOfBlockStateRootHash(block);
        if (startHash == null) {
            LOGGER.log(
                    System.Logger.Level.WARNING,
                    "Refusing to apply block {0}: missing a parseable BlockFooter",
                    incomingBlock);
            applyHalted.set(true);
            return false;
        }
        // Hash state 2 (post-(lastAppliedBlock)) exactly here, as we attempt to promote it —
        // never at seal, never on the mutable. Empty at genesis (no state 2 yet).
        final Bytes attestedHash = lastAppliedBlock < 0L ? Bytes.EMPTY : rootHashOf(hashingImmutable);
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
        if (lastAppliedBlock >= 0L && (attestedImmutable == null || lastAppliedBlock != metadata.blockNumber())) {
            setAttested(hashingImmutable);
            metadata = StateMetadata.newBuilder()
                    .blockNumber(lastAppliedBlock)
                    .roundNumber(lastAppliedRound < 0L ? metadata.roundNumber() : lastAppliedRound)
                    .stateRootHash(attestedHash)
                    .stateSize(sizeOf(attestedImmutable))
                    .build();
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
        lastAppliedBlock = result.blockNumber();
        if (result.roundNumber() >= 0L) {
            lastAppliedRound = result.roundNumber();
        }
        lastApplyDurationMs = System.currentTimeMillis() - applyStartMs;
        return true;
    }

    /// Validate block N's `BlockFooter.startOfBlockStateRootHash` against `attestedHash` — the
    /// root hash of state 2 (post-(N-1)), computed off the *sealed* `hashingImmutable` (never
    /// the live mutable). A match confirms our post-(N-1) equals the network's attested
    /// start-of-N hash.
    ///
    /// Genesis branch: when no block has been applied yet, there is no state 2 to hash and the
    /// expected start hash is empty / all-zeros; accept either.
    ///
    /// @param startHash the incoming block's footer start-of-block state root hash
    /// @param attestedHash the root hash of state 2 (post-(N-1)); empty at genesis
    /// @return `true` if the hashes match (or the genesis shape holds)
    private boolean validateStartHash(@NonNull final Bytes startHash, @NonNull final Bytes attestedHash) {
        if (lastAppliedBlock < 0L) {
            return startHash.length() == 0L || isAllZeros(startHash);
        }
        return attestedHash.equals(startHash);
    }

    /// Adopt `newAttested` as the state queries read, managing reference counts so it survives
    /// the next `copyMutableState()` (which releases the lifecycle manager's own reference to
    /// the superseded version). We `reserve()` the new state's root and `release()` the
    /// previously-held one — without this the held immutable is destroyed and reads/snapshots
    /// throw `ReferenceCountException`.
    ///
    /// @param newAttested the newly-attested state to publish to readers
    private void setAttested(@NonNull final VirtualMapState newAttested) {
        final VirtualMapState previous = attestedImmutable;
        newAttested.getRoot().reserve();
        attestedImmutable = newAttested;
        if (previous != null && previous != newAttested) {
            previous.getRoot().release();
        }
    }

    /// Whether every byte in `b` is zero. Used to accept an all-zeros genesis
    /// start-of-block state root hash.
    ///
    /// @param b the bytes to test
    /// @return `true` if all bytes are zero (vacuously true for empty)
    private static boolean isAllZeros(@NonNull final Bytes b) {
        final long len = b.length();
        for (long i = 0; i < len; i++) {
            if (b.getByte(i) != 0) {
                return false;
            }
        }
        return true;
    }

    /// The recent-snapshot directory path for a given block number.
    ///
    /// @param blockNumber the block number
    /// @return the directory path under `stateSnapshotRecentPath` for that block
    @NonNull
    private Path recentSnapshotDirectoryFor(final long blockNumber) {
        return Path.of(config.stateSnapshotRecentPath(), Long.toString(blockNumber));
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
