// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.state.BinaryState;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.base.file.FileSystemManager;
import org.hiero.block.api.BinaryStateQuery;
import org.hiero.block.api.BinaryStateQueryResponse;
import org.hiero.block.api.BinaryStateQueryResponse.Code;
import org.hiero.block.api.StateMetadata;
import org.hiero.block.api.StateServiceInterface;
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
/// The plugin owns:
///
/// - a `VirtualMapStateLifecycleManager` that wraps a `VirtualMapState`
///   holding the merkle-backed state on disk (per the consensus-node state-snapshot spec);
/// - a `StateMetadataStore` persisting the latest `StateMetadata`;
/// - a `StateChangeApplier` that translates `state_changes` block items
///   into `BinaryState` mutations;
/// - two single-thread scheduled executors — one for apply, one for snapshot.
///
/// See `docs/design/state/live-state.md` for the full design.
public final class StateManagementPlugin implements BlockNodePlugin, BlockNotificationHandler, StateServiceInterface {

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

    /// `blocknode:state_degraded` — `1` when the plugin has degraded (e.g. footer
    /// hash mismatch), else `0`.
    static final MetricKey<ObservableGauge> METRIC_DEGRADED =
            MetricKey.of("state_degraded", ObservableGauge.class).addCategory(METRICS_CATEGORY);

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
    /// Concurrency note (accepted for beta): `metadata` and `attestedImmutable` are
    /// updated as two separate volatile writes in the lag-1 commit (see
    /// `applyBlockStateChanges`), not as one atomic swap. A query thread can therefore
    /// briefly observe `metadata` one commit ahead of (or behind) `attestedImmutable`.
    /// Both are only ever written from the single apply thread, so the window is tiny
    /// and the worst case is a stale-but-attested read or a transient `NOT_FOUND` — never
    /// a crash. Tightening this by bundling `(attestedImmutable, metadata)` into a single
    /// immutable holder swapped with one volatile write is a deferred consideration.
    private volatile StateMetadata metadata = StateMetadata.DEFAULT;

    private final ConcurrentSkipListMap<Long, BlockUnparsed> pendingBlocks = new ConcurrentSkipListMap<>();
    private final AtomicBoolean stateIsCaughtUp = new AtomicBoolean(false);
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean degraded = new AtomicBoolean(false);
    private final AtomicLong hashMismatchTotal = new AtomicLong();

    /// Wall-clock duration of the most recent block apply, exported via
    /// `state_apply_latency_ms`. Volatile because the gauge supplier reads it
    /// from the metrics-scrape thread while the apply thread writes it.
    private volatile long lastApplyDurationMs = 0L;

    /// Exported counter measurement for footer hash mismatches, registered in
    /// `init` (always before any apply runs in `start`).
    private LongCounter.Measurement hashMismatchMetric;

    private ScheduledExecutorService snapshotExecutor;
    private ScheduledExecutorService stateChangesExecutor;
    private ScheduledExecutorService catchUpExecutor;
    private volatile long lastSnapshottedBlock = -1L;

    /// Block number of the most recent block whose state_changes have been applied
    /// to the mutable state. Distinct from `metadata.blockNumber` because of the
    /// lag-1 commit: `metadata.blockNumber` is the most recently *committed*
    /// (visible to readers) block; `lastAppliedBlock` is the staging block whose
    /// changes live in the mutable copy and will be committed when the next
    /// block's footer confirms them. Set to `-1` until the first apply.
    private volatile long lastAppliedBlock = -1L;

    /// Round number of `lastAppliedBlock`, mirrored for the same lag-1 reason.
    private volatile long lastAppliedRound = -1L;

    /// Root hash of the state produced by applying `lastAppliedBlock` (i.e.
    /// post-`lastAppliedBlock`). The next block's
    /// `BlockFooter.startOfBlockStateRootHash` must equal this for us to accept
    /// it — which is also the moment that next block's footer *attests*
    /// `lastAppliedBlock`, allowing us to commit/expose it (lag-1). Empty until
    /// the first apply.
    private volatile Bytes lastAppliedHash = Bytes.EMPTY;

    /// The latest network-attested immutable state — the only state the query API
    /// reads. Under the lag-1 commit model this lags `lastAppliedBlock` by one
    /// block: a block is applied into the live mutable first, and only promoted here
    /// once the *next* block's footer confirms its root hash. `null` until the
    /// first block has been attested (so queries report NOT_READY at pure genesis).
    private volatile VirtualMapState attestedImmutable;

    /// {@inheritDoc}
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        // TODO: a plugin shouldn't own config records defined by an external
        // library. Options under consideration:
        //   (A) Foundation: ship META-INF/services entries on the swirlds jars so
        //       BlockNodeApp.autoDiscoverExtensions picks them up automatically.
        //   (B) Block-node base helper: a shared SwirldsStateConfigs.types() list every
        //       state-consuming plugin includes, paired with idempotent registration so
        //       multiple plugins don't collide.
        //   (C) BlockNodeApp registers swirlds-state configs once at boot, invisible to
        //       plugins.
        // Until one of those lands, this list keeps the plugin functional.
        return List.of(
                StateManagementConfig.class,
                com.swirlds.merkledb.config.MerkleDbConfig.class,
                com.swirlds.virtualmap.config.VirtualMapConfig.class,
                org.hiero.consensus.config.PathsConfig.class);
    }

    /// {@inheritDoc}
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = context;
        this.config = context.configuration().getConfigData(StateManagementConfig.class);
        this.metadataStore = new StateMetadataStore(Path.of(config.stateMetadataPath()));
        // Anchor FileSystemManager at the configured recent-snapshot path so the lifecycle
        // manager's bookkeeping (its temp scratchpad in particular) lives next to the
        // snapshots rather than in the cwd.
        final Path fsmRoot = Path.of(config.stateSnapshotRecentPath());
        this.lifecycleManager = new VirtualMapStateLifecycleManager(
                new NoOpMetrics(), Time.getCurrent(), context.configuration(), new FileSystemManager(fsmRoot));
        this.applier = new StateChangeApplier();
        registerMetrics(context.metricRegistry());
        serviceBuilder.registerGrpcService(this);
    }

    /// Register the plugin's metrics with the block node's registry. Gauges observe
    /// the live plugin fields (committed block, state size, pending depth, readiness,
    /// degraded flag, last apply latency); the hash-mismatch counter is stored so the
    /// apply path can increment it.
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
        metrics.register(ObservableGauge.builder(METRIC_DEGRADED)
                .setDescription("1 when the plugin has degraded (e.g. footer hash mismatch), else 0")
                .observe(() -> degraded.get() ? 1L : 0L));
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
        loadPersistedState();
        context.blockMessaging().registerBlockNotificationHandler(this, true, name());

        stateChangesExecutor = newSingleThreadExecutor("StateManagement-apply");
        snapshotExecutor = newSingleThreadExecutor("StateManagement-snapshot");
        catchUpExecutor = newSingleThreadExecutor("StateManagement-catchup");

        // Snapshot timer can run from boot — it no-ops until metadata.blockNumber()
        // moves forward, so there is no race with catch-up.
        snapshotExecutor.scheduleWithFixedDelay(
                this::saveSnapshot,
                config.snapshotIntervalMillis(),
                config.snapshotIntervalMillis(),
                TimeUnit.MILLISECONDS);

        // IMPORTANT: do NOT schedule the recurring applyPending() task before
        // catchUpFromHistoricalBlocks finishes. Both call applyPending(), which
        // mutates lifecycleManager.getMutableState() / copyMutableState() and the
        // `metadata` field — none of which are safe to invoke concurrently from
        // two threads. The catch-up walk calls applyPending() inline as it
        // enqueues each batch; the timer-driven version starts only after the
        // catch-up walk has completed (or trivially decided there's nothing to
        // catch up to).
        catchUpExecutor.execute(() -> {
            try {
                catchUpFromHistoricalBlocks();
            } finally {
                stateChangesExecutor.scheduleWithFixedDelay(
                        this::applyPending, 0L, config.stateChangesApplyIntervalMillis(), TimeUnit.MILLISECONDS);
            }
        });
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
        shutdownExecutor(catchUpExecutor);
        shutdownExecutor(stateChangesExecutor);
        shutdownExecutor(snapshotExecutor);
        if (metadata.blockNumber() > lastSnapshottedBlock && metadata.blockNumber() > 0L) {
            try {
                saveSnapshot();
            } catch (final RuntimeException e) {
                LOGGER.log(System.Logger.Level.WARNING, "Final snapshot on stop() failed", e);
            }
        }
    }

    /// {@inheritDoc}
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        if (!notification.success() || notification.block() == null) {
            return;
        }
        pendingBlocks.put(notification.blockNumber(), notification.block());
    }

    // ── StateServiceInterface: getBinaryKV / Singleton / Queue ─────────────

    /// {@inheritDoc}
    @NonNull
    @Override
    public BinaryStateQueryResponse getBinaryKV(@NonNull final BinaryStateQuery request) {
        final BinaryStateQueryResponse.Builder out = baseResponse();
        if (notReadyForQueries()) {
            return out.status(Code.NOT_READY).build();
        }
        if (request.keyBytes() == null || request.keyBytes().length() == 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (request.queueIndex() != 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!hasBlockSpecifier(request)) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!matchesLatestBlock(request)) {
            return out.status(Code.NOT_FOUND).build();
        }
        // gRPC reads always go off the attested immutable state.
        final Bytes value = mapGet(StateSource.IMMUTABLE, (int) request.stateId(), request.keyBytes());
        if (value == null) {
            return out.status(Code.NOT_FOUND).build();
        }
        return out.status(Code.SUCCESS).kvBytes(value).build();
    }

    /// {@inheritDoc}
    @NonNull
    @Override
    public BinaryStateQueryResponse getBinarySingleton(@NonNull final BinaryStateQuery request) {
        final BinaryStateQueryResponse.Builder out = baseResponse();
        if (notReadyForQueries()) {
            return out.status(Code.NOT_READY).build();
        }
        if (request.keyBytes() != null && request.keyBytes().length() > 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (request.queueIndex() != 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!hasBlockSpecifier(request)) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!matchesLatestBlock(request)) {
            return out.status(Code.NOT_FOUND).build();
        }
        final Bytes value = singletonGet(StateSource.IMMUTABLE, (int) request.stateId());
        if (value == null) {
            return out.status(Code.NOT_FOUND).build();
        }
        return out.status(Code.SUCCESS).singletonBytes(value).build();
    }

    /// {@inheritDoc}
    @NonNull
    @Override
    public BinaryStateQueryResponse getBinaryQueue(@NonNull final BinaryStateQuery request) {
        final BinaryStateQueryResponse.Builder out = baseResponse();
        if (notReadyForQueries()) {
            return out.status(Code.NOT_READY).build();
        }
        if (request.keyBytes() != null && request.keyBytes().length() > 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!hasBlockSpecifier(request)) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!matchesLatestBlock(request)) {
            return out.status(Code.NOT_FOUND).build();
        }
        final int stateId = (int) request.stateId();
        try {
            final List<Bytes> elements = queueRead(StateSource.IMMUTABLE, stateId, request.queueIndex());
            if (elements == null || elements.isEmpty()) {
                return out.status(Code.NOT_FOUND).build();
            }
            return out.status(Code.SUCCESS).queueBytes(elements).build();
        } catch (final RuntimeException e) {
            LOGGER.log(System.Logger.Level.DEBUG, "Queue lookup for state {0} failed", stateId, e);
            return out.status(Code.NOT_FOUND).build();
        }
    }

    // ── Binary state reads (value-returning; no BinaryStateQuery) ──────────────
    // These back the gRPC handlers above and are the in-process read surface a
    // future plugin-to-plugin BinaryStateReader SPI will expose as desired.
    // The StateSource argument selects which version of state to read; the gRPC
    // API always passes IMMUTABLE (attested). Reading a non-latest *immutable*
    // (e.g. a snapshot) is future work

    /// Resolve the `BinaryState` for a read source. `StateSource.HISTORICAL`
    /// is not yet supported.
    ///
    /// @param source which version of state to read
    /// @return the backing `BinaryState` for that source
    /// @throws UnsupportedOperationException if `source` is `StateSource.HISTORICAL`
    @NonNull
    private BinaryState stateFor(@NonNull final StateSource source) {
        return switch (source) {
            // IMMUTABLE is the latest *attested* state (lag-1). gRPC callers reach
            // this only past the readiness gate, which guarantees it is non-null.
            case IMMUTABLE -> attestedImmutable;
            // MUTABLE is the latest applied-but-unattested state — the live mutable,
            // which after copyMutableState mirrors post-(lastAppliedBlock).
            case MUTABLE -> lifecycleManager.getMutableState();
            case HISTORICAL -> throw new UnsupportedOperationException("snapshot-backed reads are not yet supported");
        };
    }

    /// KV value for `stateId`/`key`, or `null` if absent.
    ///
    /// @param source which version of state to read
    /// @param stateId the state id to look up
    /// @param key the KV key
    /// @return the value bytes, or `null` if absent
    @Nullable
    Bytes mapGet(@NonNull final StateSource source, final int stateId, @NonNull final Bytes key) {
        return stateFor(source).getKv(stateId, key);
    }

    /// Singleton value for `stateId`, or `null` if absent.
    ///
    /// @param source which version of state to read
    /// @param stateId the singleton state id
    /// @return the singleton value bytes, or `null` if absent
    @Nullable
    Bytes singletonGet(@NonNull final StateSource source, final int stateId) {
        return stateFor(source).getSingleton(stateId);
    }

    /// Queue read for `stateId`: the whole queue when `index == 0`,
    /// otherwise the single element at `index`. Returns `null` when the
    /// queue state is unknown or empty. `VirtualMapStateImpl.getQueueState`
    /// returns null for unknown state ids (downstream calls then NPE), so callers
    /// should guard with a try/catch and map failures to NOT_FOUND.
    ///
    /// @param source which version of state to read
    /// @param stateId the queue state id
    /// @param index `0` for the whole queue, otherwise the 1-based element index to peek
    /// @return the queue elements (whole queue or single element), or `null` if unknown/empty
    @Nullable
    List<Bytes> queueRead(@NonNull final StateSource source, final int stateId, final long index) {
        final BinaryState binaryState = stateFor(source);
        if (binaryState.getQueueState(stateId) == null) {
            return null;
        }
        if (index == 0L) {
            final List<Bytes> all = binaryState.getQueueAsList(stateId);
            return (all == null || all.isEmpty()) ? null : all;
        }
        final Bytes element = binaryState.peekQueue(stateId, (int) index);
        return element == null ? null : List.of(element);
    }

    // ── Package-private observers ─────────────────────────────────────────────
    // Read-only accessors for the same state the gRPC surface already exposes.
    // Tests drive the plugin through its real entry points (applyPending,
    // saveSnapshot, block delivery) and assert against these observers; there
    // are no force/duplicate "*Now" hooks or internals accessors in production.

    /// Whether historical catch-up has completed.
    ///
    /// @return `true` once catch-up has finished
    boolean isReady() {
        return stateIsCaughtUp.get();
    }

    /// Queries are servable only once catch-up has finished AND at least one block
    /// has been attested (so `attestedImmutable` is non-null). Under lag-1 a
    /// freshly-started node that has applied only genesis (block 0) has nothing
    /// attested yet and must report NOT_READY until the next block confirms it.
    ///
    /// @return `true` if the plugin cannot yet serve queries
    private boolean notReadyForQueries() {
        return !stateIsCaughtUp.get() || attestedImmutable == null;
    }

    /// The latest applied state metadata.
    ///
    /// @return the current `StateMetadata`
    @NonNull
    StateMetadata metadata() {
        return metadata;
    }

    /// The `startOfBlockStateRootHash` the next block's footer must carry to be
    /// accepted — i.e. the root hash of the most recently applied (staged) block,
    /// which under lag-1 is one ahead of `metadata`. Empty before the first
    /// apply. Test-visible so tests can chain a confirming block.
    ///
    /// @return the staged state root hash, or empty before the first apply
    @NonNull
    Bytes stagedStateRootHash() {
        return lastAppliedHash;
    }

    // ── Internals ───────────────────────────────────────────────────────────

    /// Load persisted metadata and, if present, the matching recent snapshot from disk,
    /// seeding the lag-1 bookkeeping so the loaded (already-attested) state is exposed on
    /// boot. Falls back to genesis when metadata is missing/unreadable or its snapshot
    /// cannot be loaded.
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
            // Metadata may point at a block we couldn't actually load — treat as
            // genesis so the lag-1 commit pipeline doesn't claim we already have
            // state we don't.
            metadata = StateMetadata.DEFAULT;
        } else {
            // We trust the loaded snapshot represents post-(metadata.blockNumber),
            // which was an attested/committed block when snapshotted. Seed the
            // lag-1 bookkeeping so it is already the exposed state on boot and the
            // next block validates against it.
            lastAppliedBlock = metadata.blockNumber();
            lastAppliedRound = metadata.roundNumber();
        }
        // Force the initial copyMutableState so getLatestImmutableState() is non-null
        // from boot onwards (and so the live mutable is a writable copy).
        lifecycleManager.copyMutableState();
        if (snapshotLoaded) {
            // Expose the loaded (already-attested) state immediately and anchor the
            // hash the next block's footer must match.
            setAttested(lifecycleManager.getLatestImmutableState());
            lastAppliedHash = rootHashOf(attestedImmutable);
        }
    }

    /// Drain the pending-blocks queue in strict block-number order, applying each
    /// contiguous next block via `applyBlockStateChanges`. Stops on the first gap
    /// (next expected block not yet present), when stopping, or when degraded.
    /// Blocks are removed only after a successful apply so failures leave the block
    /// queued for inspection / retry.
    void applyPending() {
        while (!pendingBlocks.isEmpty() && !stopping.get() && !degraded.get()) {
            final boolean atGenesis = lastAppliedBlock < 0L
                    && metadata.blockNumber() == 0L
                    && metadata.stateRootHash().length() == 0L;
            final long expectedNext =
                    atGenesis ? pendingBlocks.firstKey() : Math.max(lastAppliedBlock, metadata.blockNumber()) + 1L;
            // Peek (not remove) so an applier failure leaves the block in the queue
            // for inspection / future retry rather than silently dropping it.
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
                        "applyBlockStateChanges threw for block {0}; degrading plugin (block retained in queue)",
                        expectedNext,
                        e);
                degraded.set(true);
                return;
            }
            if (!applied) {
                // Block-specific failure already logged + degraded set (e.g. hash
                // mismatch); leave the block in the queue and stop draining.
                return;
            }
            pendingBlocks.remove(expectedNext);
        }
    }

    /// Bring the live state up to the latest block the Block-Node has on hand by reading
    /// blocks from `HistoricalBlockFacility` and feeding them through the same
    /// `applyPending` loop that handles verification-delivered blocks.
    ///
    /// Runs on the `StateManagement-catchup` thread. When complete (or if there is
    /// nothing to catch up to) sets `stateIsCaughtUp` so query traffic stops returning
    /// `NOT_READY`.
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
            while (cursor <= latest && !stopping.get() && !degraded.get()) {
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
            // Even if catch-up hit a gap or a degraded state, the plugin is in the best
            // shape it can be — start serving queries against whatever applied.
            stateIsCaughtUp.set(true);
        }
    }

    /// Fetch a single historical block via its `BlockAccessor` and enqueue it in
    /// `pendingBlocks`. No-ops if the block is unavailable or cannot be unparsed.
    /// The accessor is always closed; close failures during catch-up are non-fatal.
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

    /// Apply a single block's `state_changes` to the live state under the **lag-1
    /// commit** model: a block is applied into the live mutable, but only exposed to
    /// readers once the *next* block's footer attests its root hash. Readers
    /// therefore never see state the network has not yet confirmed.
    ///
    /// Flow for block N (given block N-1 was the last applied, post-(N-1) sealed):
    ///
    /// 1. Pull `BlockFooter.startOfBlockStateRootHash` from N (cheap reverse scan).
    /// 2. Validate it against `lastAppliedHash` (the hash of post-(N-1)).
    ///    A match means N's footer attests post-(N-1). Mismatch ⇒ degrade without
    ///    mutating; we never expose post-(N-1).
    /// 3. Now that post-(N-1) is attested, promote it to `attestedImmutable`
    ///    (visible to queries), record its metadata, and emit `VERIFIED(N-1)`.
    /// 4. Apply N's `state_changes` to the live mutable, then `copyMutableState()`
    ///    to seal post-N. Post-N stays *un-exposed* until block N+1 attests it.
    ///
    /// @param block the block to apply
    /// @return `true` on successful apply, `false` if the block was rejected
    ///     (caller should leave the block in the pending queue and stop draining).
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
            degraded.set(true);
            return false;
        }
        if (!validateStartHash(startHash)) {
            hashMismatchTotal.incrementAndGet();
            hashMismatchMetric.increment();
            degraded.set(true);
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "State hash mismatch applying block {0}: footer.startOfBlockStateRootHash ({1}) diverges "
                            + "from the last applied state's root hash ({2}, post-block {3}); plugin marked "
                            + "degraded (state not exposed)",
                    incomingBlock,
                    startHash.toHex(),
                    lastAppliedHash.toHex(),
                    lastAppliedBlock);
            return false;
        }

        // This block's footer just attested post-(lastAppliedBlock). Commit/expose
        // that previously-applied block now — lag-1: readers only ever see attested
        // state. (Skipped at genesis, and when the last applied block is already the
        // exposed one, e.g. immediately after a snapshot reload.)
        if (lastAppliedBlock >= 0L && (attestedImmutable == null || lastAppliedBlock != metadata.blockNumber())) {
            final VirtualMapState attested = lifecycleManager.getLatestImmutableState();
            setAttested(attested);
            metadata = StateMetadata.newBuilder()
                    .blockNumber(lastAppliedBlock)
                    .roundNumber(lastAppliedRound < 0L ? metadata.roundNumber() : lastAppliedRound)
                    .stateRootHash(lastAppliedHash)
                    .stateSize(sizeOf(attested))
                    .build();
        }

        // Apply this block into the live mutable and seal it, but DO NOT expose it:
        // it stays staged until the next block's footer attests it (above, next call).
        final VirtualMapState mutable = lifecycleManager.getMutableState();
        final StateChangeApplier.ApplyResult result = applier.applyBlock(mutable, block);
        if (result.blockNumber() < 0L) {
            LOGGER.log(System.Logger.Level.WARNING, "Block had unparseable header; treating as failed apply");
            degraded.set(true);
            return false;
        }
        lifecycleManager.copyMutableState();
        final VirtualMapState sealed = lifecycleManager.getLatestImmutableState();
        lastAppliedBlock = result.blockNumber();
        if (result.roundNumber() >= 0L) {
            lastAppliedRound = result.roundNumber();
        }
        lastAppliedHash = rootHashOf(sealed);
        lastApplyDurationMs = System.currentTimeMillis() - applyStartMs;
        return true;
    }

    /// Validate block N's `BlockFooter.startOfBlockStateRootHash` against the
    /// hash of the last state we applied (post-(N-1), held in `lastAppliedHash`).
    /// A match confirms our post-(N-1) matches the network's attested start-of-N hash
    /// — which is exactly the attestation that lets us expose post-(N-1).
    ///
    /// We compare against the recorded `lastAppliedHash` rather than
    /// recomputing from the live mutable: hashing a `VirtualMap` that has been
    /// written to since its last hash seals it and breaks the next
    /// `applier.applyBlock` with "Cannot modify already hashed node". The hash was
    /// recorded at the right lifecycle point (right after the previous
    /// `copyMutableState`, off the sealed immutable).
    ///
    /// Genesis branch: when no block has been applied yet (`lastAppliedBlock < 0`),
    /// the expected start hash is empty / all-zeros and we accept either shape.
    ///
    /// @param startHash the incoming block's footer start-of-block state root hash
    /// @return `true` if the hash matches the last applied state (or genesis shape)
    private boolean validateStartHash(@NonNull final Bytes startHash) {
        if (lastAppliedBlock < 0L) {
            return startHash.length() == 0L || isAllZeros(startHash);
        }
        return lastAppliedHash.equals(startHash);
    }

    /// Adopt `newAttested` as the state queries read, managing reference counts
    /// so it survives the next `copyMutableState()` (which releases the lifecycle
    /// manager's own reference to the superseded version). We `reserve()` the new
    /// state's root and `release()` the previously-held one — without this the
    /// held immutable is destroyed and reads/snapshots throw `ReferenceCountException`.
    ///
    /// Reserve-then-swap-then-release keeps the new state pinned before it is
    /// published. A read that is already in flight on the prior state holds only a Java
    /// reference, not a reservation; tightening that race (reserve per-read, or a grace
    /// window) is deferred as a follow-up. The apply path is
    /// single-threaded, so only one rotation is ever in flight.
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

    /// Total count of footer hash mismatches observed. Visible to tests + future
    /// server-status metrics integration.
    ///
    /// @return the cumulative hash-mismatch count
    long hashMismatchTotal() {
        return hashMismatchTotal.get();
    }

    /// Whether the plugin has entered the degraded (stopped-applying) state.
    /// Visible to tests.
    ///
    /// @return `true` if the plugin is degraded
    boolean isDegraded() {
        return degraded.get();
    }

    /// Write a snapshot of the *attested* state (the committed block named by
    /// `metadata`) to its recent-snapshot directory, prune snapshots beyond the
    /// retention window, and persist the metadata. No-ops when nothing has been
    /// attested yet or the attested block was already snapshotted.
    void saveSnapshot() {
        final StateMetadata snapshot = metadata;
        final VirtualMapState attested = attestedImmutable;
        // Snapshot the *attested* state (the committed block named by metadata), not
        // lifecycleManager.getLatestImmutableState() — under lag-1 the latter is one
        // block ahead and unattested. Nothing attested yet ⇒ nothing to snapshot.
        if (attested == null || snapshot.blockNumber() <= lastSnapshottedBlock) {
            return;
        }
        final Path target = recentSnapshotDirectoryFor(snapshot.blockNumber());
        try {
            Files.createDirectories(target.getParent());
            if (!Files.exists(target)) {
                lifecycleManager.createSnapshot(attested, target);
            }
            pruneOldRecentSnapshots(snapshot.blockNumber());
            metadataStore.save(snapshot);
            lastSnapshottedBlock = snapshot.blockNumber();
        } catch (final IOException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Snapshot write failed for block " + snapshot.blockNumber(), e);
        }
    }

    /// Delete recent snapshot directories beyond `stateSnapshotRecentRetentionCount`
    /// after a snapshot is written. Snapshots live only under
    /// `stateSnapshotRecentPath` as hot, ready-to-load directories — each
    /// hard-links into the live MerkleDb files, so a snapshot is cheap in both time
    /// and disk. Long-term archival (compaction, off-box transfer, random-read
    /// indexes) is intentionally out of scope for this plugin and is left to a
    /// future archiving plugin; we keep only the last N good snapshots, which is all
    /// that seeding / reconnecting another BN or CN needs.
    ///
    /// The just-written `currentBlock` dir is always preserved (it's the
    /// freshest); remaining dirs sorted by block number ascending are deleted from
    /// the oldest until the count is at or under the retention window. A retention
    /// of `1` keeps only the current dir.
    ///
    /// @param currentBlock the block number of the just-written snapshot (always retained)
    /// @throws IOException if listing or deleting snapshot directories fails
    private void pruneOldRecentSnapshots(final long currentBlock) throws IOException {
        final Path recentRoot = Path.of(config.stateSnapshotRecentPath());
        if (!Files.isDirectory(recentRoot)) {
            return;
        }
        final int retain = Math.max(0, config.stateSnapshotRecentRetentionCount());
        final List<Path> entries = listRecentDirs(recentRoot);
        // Build a list of (block, path) excluding the current block — those are
        // candidates for deletion.
        final List<Map.Entry<Long, Path>> candidates = new ArrayList<>();
        for (final Path dir : entries) {
            final long blockNumber = parseBlockDirName(dir);
            if (blockNumber < 0L) {
                // Not a block-number dir — treat as garbage, delete unconditionally.
                deleteDirectoryRecursively(dir);
                continue;
            }
            if (blockNumber == currentBlock) {
                continue;
            }
            candidates.add(Map.entry(blockNumber, dir));
        }
        candidates.sort(Map.Entry.comparingByKey());
        // The current block counts as 1 against the retention budget; older dirs
        // beyond that get deleted from oldest to newest.
        final int allowedOlder = Math.max(0, retain - 1);
        final int excess = candidates.size() - allowedOlder;
        for (int i = 0; i < excess; i++) {
            deleteDirectoryRecursively(candidates.get(i).getValue());
        }
    }

    /// List the immediate subdirectories of the recent-snapshot root.
    ///
    /// @param recentRoot the recent-snapshot root directory
    /// @return the directory children of `recentRoot`
    /// @throws IOException if the directory cannot be listed
    @NonNull
    private static List<Path> listRecentDirs(@NonNull final Path recentRoot) throws IOException {
        try (var entries = Files.list(recentRoot)) {
            return entries.filter(Files::isDirectory).toList();
        }
    }

    /// Parse a snapshot directory's name as a block number.
    ///
    /// @param dir the directory whose name is parsed
    /// @return the parsed block number, or `-1` if the name is not a number
    private static long parseBlockDirName(@NonNull final Path dir) {
        try {
            return Long.parseLong(dir.getFileName().toString());
        } catch (final NumberFormatException ignored) {
            return -1L;
        }
    }

    /// The recent-snapshot directory path for a given block number.
    ///
    /// @param blockNumber the block number
    /// @return the directory path under `stateSnapshotRecentPath` for that block
    @NonNull
    private Path recentSnapshotDirectoryFor(final long blockNumber) {
        return Path.of(config.stateSnapshotRecentPath(), Long.toString(blockNumber));
    }

    /// A response builder pre-seeded with the current `metadata`, shared by all query handlers.
    ///
    /// @return a `BinaryStateQueryResponse.Builder` carrying the current metadata
    @NonNull
    private BinaryStateQueryResponse.Builder baseResponse() {
        return BinaryStateQueryResponse.newBuilder().stateMetadata(metadata);
    }

    /// Returns `true` when the request carries a `block_specifier` — exactly
    /// one of `retrieve_latest` or `block_number`, per the
    /// `BlockRequest.block_specifier` convention in `block_access_service.proto`.
    /// A request that sets neither is malformed and must be rejected with
    /// `Code.INVALID_REQUEST` (distinct from a well-formed request for a block
    /// we do not hold, which is `Code.NOT_FOUND`).
    ///
    /// @param request the query to inspect
    /// @return `true` if the request carries a block specifier
    private static boolean hasBlockSpecifier(@NonNull final BinaryStateQuery request) {
        return request.hasRetrieveLatest() || request.hasBlockNumber();
    }

    /// Returns `true` when the request targets the currently applied state. Assumes
    /// the request has already passed `hasBlockSpecifier`. The plugin only
    /// ever serves the latest immutable state, so a `block_number` other than the
    /// latest applied block does not match — callers get `Code.NOT_FOUND`,
    /// which affirms the latest-only API rather than rejecting the request shape.
    /// Block 0 (genesis) is a valid `block_number`.
    ///
    /// @param request the query to inspect (already known to carry a block specifier)
    /// @return `true` if the request targets the latest applied block
    private boolean matchesLatestBlock(@NonNull final BinaryStateQuery request) {
        if (request.hasRetrieveLatest() && Boolean.TRUE.equals(request.retrieveLatest())) {
            return true;
        }
        if (request.hasBlockNumber()) {
            return request.blockNumber() == metadata.blockNumber();
        }
        return false;
    }

    /// The root hash of a sealed state, or empty if the state/root is absent or
    /// not yet hashed.
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
            // Returning empty is fail-safe — the next block's footer validation will not
            // match an empty hash, so we degrade rather than expose a wrong root. Log the
            // cause at WARNING so a hashing/lifecycle bug is diagnosable instead of silent.
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

    /// Recursively delete a directory tree. Deletes children before parents
    /// (reverse-ordered walk). Failures are logged and swallowed (best-effort prune).
    ///
    /// @param root the directory tree to delete
    private static void deleteDirectoryRecursively(@NonNull final Path root) {
        try {
            if (!Files.exists(root)) {
                return;
            }
            try (var stream = Files.walk(root)) {
                stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (final IOException e) {
                        throw new RuntimeException("Failed to delete " + p, e);
                    }
                });
            }
        } catch (final IOException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Failed to prune " + root, e);
        }
    }

    /// Create a single-thread scheduled executor backed by a named daemon thread.
    ///
    /// @param threadName the name for the executor's daemon thread
    /// @return a new single-thread scheduled executor
    @NonNull
    private static ScheduledExecutorService newSingleThreadExecutor(@NonNull final String threadName) {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            return t;
        });
    }

    /// Gracefully shut down an executor, waiting up to 2 seconds before forcing
    /// shutdown. No-ops on a `null` executor; restores the interrupt flag if
    /// interrupted while awaiting termination.
    ///
    /// @param executor the executor to shut down (may be `null`)
    private static void shutdownExecutor(@Nullable final ScheduledExecutorService executor) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2L, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
