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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

/**
 * Beta plugin that maintains a live, queryable copy of Hashgraph network state inside
 * the Block Node by replaying verified block-stream state changes onto a
 * {@link VirtualMapStateLifecycleManager}-managed state.
 *
 * <p>The plugin owns:
 *
 * <ul>
 *   <li>a {@link VirtualMapStateLifecycleManager} that wraps a {@link VirtualMapState}
 *       holding the merkle-backed state on disk (per the consensus-node state-snapshot spec);</li>
 *   <li>a {@link StateMetadataStore} persisting the latest {@link StateMetadata};</li>
 *   <li>a {@link StateChangeApplier} that translates {@code state_changes} block items
 *       into {@link BinaryState} mutations;</li>
 *   <li>two single-thread scheduled executors — one for apply, one for snapshot.</li>
 * </ul>
 *
 * <p>See {@code docs/design/state/live-state.md} for the full design.
 */
public final class StateManagementPlugin implements BlockNodePlugin, BlockNotificationHandler, StateServiceInterface {

    private static final System.Logger LOGGER = System.getLogger(StateManagementPlugin.class.getName());

    private BlockNodeContext context;
    private StateManagementConfig config;
    private VirtualMapStateLifecycleManager lifecycleManager;
    private StateMetadataStore metadataStore;
    private StateChangeApplier applier;

    /** Latest applied state metadata. Volatile because reads happen on query threads. */
    private volatile StateMetadata metadata = StateMetadata.DEFAULT;

    private final ConcurrentSkipListMap<Long, BlockUnparsed> pendingBlocks = new ConcurrentSkipListMap<>();
    private final AtomicBoolean stateIsCaughtUp = new AtomicBoolean(false);
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean degraded = new AtomicBoolean(false);
    private final java.util.concurrent.atomic.AtomicLong hashMismatchTotal =
            new java.util.concurrent.atomic.AtomicLong();

    private ScheduledExecutorService snapshotExecutor;
    private ScheduledExecutorService stateChangesExecutor;
    private ScheduledExecutorService catchUpExecutor;
    private volatile long lastSnapshottedBlock = -1L;

    /**
     * Block number of the most recent block whose state_changes have been applied
     * to the mutable state. Distinct from `metadata.blockNumber` because of the
     * lag-1 commit: `metadata.blockNumber` is the most recently *committed*
     * (visible to readers) block; `lastAppliedBlock` is the staging block whose
     * changes live in the mutable copy and will be committed when the next
     * block's footer confirms them. Set to `-1` until the first apply.
     */
    private volatile long lastAppliedBlock = -1L;

    /** Round number of {@code lastAppliedBlock}, mirrored for the same lag-1 reason. */
    private volatile long lastAppliedRound = -1L;

    /**
     * Root hash of the state produced by applying {@code lastAppliedBlock} (i.e.
     * post-{@code lastAppliedBlock}). The next block's
     * {@code BlockFooter.startOfBlockStateRootHash} must equal this for us to accept
     * it — which is also the moment that next block's footer *attests*
     * {@code lastAppliedBlock}, allowing us to commit/expose it (lag-1). Empty until
     * the first apply.
     */
    private volatile Bytes lastAppliedHash = Bytes.EMPTY;

    /**
     * The latest network-attested immutable state — the only state the query API
     * reads. Under the lag-1 commit model this lags {@code lastAppliedBlock} by one
     * block: a block is applied into the live mutable first, and only promoted here
     * once the *next* block's footer confirms its root hash. {@code null} until the
     * first block has been attested (so queries report NOT_READY at pure genesis).
     */
    private volatile VirtualMapState attestedImmutable;

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
        serviceBuilder.registerGrpcService(this);
    }

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

    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        if (!notification.success() || notification.block() == null) {
            return;
        }
        pendingBlocks.put(notification.blockNumber(), notification.block());
    }

    // ── StateServiceInterface: getBinaryKV / Singleton / Queue ─────────────

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

    /**
     * Resolve the {@link BinaryState} for a read source. {@link StateSource#HISTORICAL}
     * is not yet supported.
     */
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

    /** KV value for {@code stateId}/{@code key}, or {@code null} if absent. */
    @Nullable
    Bytes mapGet(@NonNull final StateSource source, final int stateId, @NonNull final Bytes key) {
        return stateFor(source).getKv(stateId, key);
    }

    /** Singleton value for {@code stateId}, or {@code null} if absent. */
    @Nullable
    Bytes singletonGet(@NonNull final StateSource source, final int stateId) {
        return stateFor(source).getSingleton(stateId);
    }

    /**
     * Queue read for {@code stateId}: the whole queue when {@code index == 0},
     * otherwise the single element at {@code index}. Returns {@code null} when the
     * queue state is unknown or empty. {@code VirtualMapStateImpl.getQueueState}
     * returns null for unknown state ids (downstream calls then NPE), so callers
     * should guard with a try/catch and map failures to NOT_FOUND.
     */
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

    boolean isReady() {
        return stateIsCaughtUp.get();
    }

    /**
     * Queries are servable only once catch-up has finished AND at least one block
     * has been attested (so {@link #attestedImmutable} is non-null). Under lag-1 a
     * freshly-started node that has applied only genesis (block 0) has nothing
     * attested yet and must report NOT_READY until the next block confirms it.
     */
    private boolean notReadyForQueries() {
        return !stateIsCaughtUp.get() || attestedImmutable == null;
    }

    @NonNull
    StateMetadata metadata() {
        return metadata;
    }

    /**
     * The {@code startOfBlockStateRootHash} the next block's footer must carry to be
     * accepted — i.e. the root hash of the most recently applied (staged) block,
     * which under lag-1 is one ahead of {@link #metadata}. Empty before the first
     * apply. Test-visible so tests can chain a confirming block.
     */
    @NonNull
    Bytes stagedStateRootHash() {
        return lastAppliedHash;
    }

    // ── Internals ───────────────────────────────────────────────────────────

    private void loadPersistedState() {
        try {
            metadata = metadataStore.load().orElse(StateMetadata.DEFAULT);
        } catch (final IOException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Unable to load state metadata; starting from genesis", e);
            metadata = StateMetadata.DEFAULT;
        }
        boolean snapshotLoaded = false;
        final Optional<Path> snapshotDir = recentSnapshotDirectoryFor(metadata.blockNumber());
        if (snapshotDir.isPresent() && Files.isDirectory(snapshotDir.get())) {
            try {
                lifecycleManager.loadSnapshot(snapshotDir.get());
                snapshotLoaded = true;
                LOGGER.log(System.Logger.Level.INFO, "Loaded state snapshot from {0}", snapshotDir.get());
            } catch (final IOException e) {
                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "Snapshot at {0} unreadable; continuing with eager genesis state",
                        snapshotDir.get(),
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

    /**
     * Bring the live state up to the latest block the Block-Node has on hand by reading
     * blocks from {@link HistoricalBlockFacility} and feeding them through the same
     * {@link #applyPending()} loop that handles verification-delivered blocks.
     *
     * <p>Runs on the {@code StateManagement-catchup} thread. When complete (or if there is
     * nothing to catch up to) sets {@link #stateIsCaughtUp} so query traffic stops returning
     * {@code NOT_READY}.
     */
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

    /**
     * Apply a single block's `state_changes` to the live state under the **lag-1
     * commit** model: a block is applied into the live mutable, but only exposed to
     * readers once the *next* block's footer attests its root hash. Readers
     * therefore never see state the network has not yet confirmed.
     *
     * <p>Flow for block N (given block N-1 was the last applied, post-(N-1) sealed):
     *
     * <ol>
     *   <li>Pull `BlockFooter.startOfBlockStateRootHash` from N (cheap reverse scan).</li>
     *   <li>Validate it against {@link #lastAppliedHash} (the hash of post-(N-1)).
     *       A match means N's footer attests post-(N-1). Mismatch ⇒ degrade without
     *       mutating; we never expose post-(N-1).</li>
     *   <li>Now that post-(N-1) is attested, promote it to {@link #attestedImmutable}
     *       (visible to queries), record its metadata, and emit `VERIFIED(N-1)`.</li>
     *   <li>Apply N's `state_changes` to the live mutable, then `copyMutableState()`
     *       to seal post-N. Post-N stays *un-exposed* until block N+1 attests it.</li>
     * </ol>
     *
     * @return `true` on successful apply, `false` if the block was rejected
     *     (caller should leave the block in the pending queue and stop draining).
     */
    private boolean applyBlockStateChanges(@NonNull final BlockUnparsed block) {
        final Bytes startHash = StateChangeApplier.extractStartOfBlockStateRootHash(block);
        if (startHash == null) {
            LOGGER.log(System.Logger.Level.WARNING, "Refusing to apply block missing a parseable BlockFooter");
            degraded.set(true);
            return false;
        }
        if (!validateStartHash(startHash)) {
            hashMismatchTotal.incrementAndGet();
            degraded.set(true);
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "State hash mismatch: footer.startOfBlockStateRootHash diverges from the last applied "
                            + "state's root hash; plugin marked degraded (state not exposed)");
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
        return true;
    }

    /**
     * Validate block N's {@code BlockFooter.startOfBlockStateRootHash} against the
     * hash of the last state we applied (post-(N-1), held in {@link #lastAppliedHash}).
     * A match confirms our post-(N-1) matches the network's attested start-of-N hash
     * — which is exactly the attestation that lets us expose post-(N-1).
     *
     * <p>We compare against the recorded {@link #lastAppliedHash} rather than
     * recomputing from the live mutable: hashing a {@code VirtualMap} that has been
     * written to since its last hash seals it and breaks the next
     * `applier.applyBlock` with "Cannot modify already hashed node". The hash was
     * recorded at the right lifecycle point (right after the previous
     * `copyMutableState`, off the sealed immutable).
     *
     * <p>Genesis branch: when no block has been applied yet ({@code lastAppliedBlock < 0}),
     * the expected start hash is empty / all-zeros and we accept either shape.
     */
    private boolean validateStartHash(@NonNull final Bytes startHash) {
        if (lastAppliedBlock < 0L) {
            return startHash.length() == 0L || isAllZeros(startHash);
        }
        return lastAppliedHash.equals(startHash);
    }

    /**
     * Adopt {@code newAttested} as the state queries read, managing reference counts
     * so it survives the next {@code copyMutableState()} (which releases the lifecycle
     * manager's own reference to the superseded version). We {@code reserve()} the new
     * state's root and {@code release()} the previously-held one — without this the
     * held immutable is destroyed and reads/snapshots throw {@code ReferenceCountException}.
     *
     * <p>Reserve-then-swap-then-release keeps the new state pinned before it is
     * published. A read that is already in flight on the prior state holds only a Java
     * reference, not a reservation; tightening that race (reserve per-read, or a grace
     * window) is deferred as a follow-up. The apply path is
     * single-threaded, so only one rotation is ever in flight.
     */
    private void setAttested(@NonNull final VirtualMapState newAttested) {
        final VirtualMapState previous = attestedImmutable;
        newAttested.getRoot().reserve();
        attestedImmutable = newAttested;
        if (previous != null && previous != newAttested) {
            previous.getRoot().release();
        }
    }

    private static boolean isAllZeros(@NonNull final Bytes b) {
        final long len = b.length();
        for (long i = 0; i < len; i++) {
            if (b.getByte(i) != 0) {
                return false;
            }
        }
        return true;
    }

    /** Visible to tests + future server-status metrics integration. */
    long hashMismatchTotal() {
        return hashMismatchTotal.get();
    }

    /** Visible to tests. */
    boolean isDegraded() {
        return degraded.get();
    }

    void saveSnapshot() {
        final StateMetadata snapshot = metadata;
        final VirtualMapState attested = attestedImmutable;
        // Snapshot the *attested* state (the committed block named by metadata), not
        // lifecycleManager.getLatestImmutableState() — under lag-1 the latter is one
        // block ahead and unattested. Nothing attested yet ⇒ nothing to snapshot.
        if (attested == null || snapshot.blockNumber() <= lastSnapshottedBlock) {
            return;
        }
        final Optional<Path> targetOpt = recentSnapshotDirectoryFor(snapshot.blockNumber());
        if (targetOpt.isEmpty()) {
            return;
        }
        final Path target = targetOpt.get();
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

    /**
     * Delete recent snapshot directories beyond {@code stateSnapshotRecentRetentionCount}
     * after a snapshot is written. Snapshots live only under
     * {@code stateSnapshotRecentPath} as hot, ready-to-load directories — each
     * hard-links into the live MerkleDb files, so a snapshot is cheap in both time
     * and disk. Long-term archival (compaction, off-box transfer, random-read
     * indexes) is intentionally out of scope for this plugin and is left to a
     * future archiving plugin; we keep only the last N good snapshots, which is all
     * that seeding / reconnecting another BN or CN needs.
     *
     * <p>The just-written {@code currentBlock} dir is always preserved (it's the
     * freshest); remaining dirs sorted by block number ascending are deleted from
     * the oldest until the count is at or under the retention window. A retention
     * of {@code 1} keeps only the current dir.
     */
    private void pruneOldRecentSnapshots(final long currentBlock) throws IOException {
        final Path recentRoot = Path.of(config.stateSnapshotRecentPath());
        if (!Files.isDirectory(recentRoot)) {
            return;
        }
        final int retain = Math.max(0, config.stateSnapshotRecentRetentionCount());
        final List<Path> entries = listRecentDirs(recentRoot);
        // Build a list of (block, path) excluding the current block — those are
        // candidates for deletion.
        final List<java.util.Map.Entry<Long, Path>> candidates = new java.util.ArrayList<>();
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
            candidates.add(java.util.Map.entry(blockNumber, dir));
        }
        candidates.sort(java.util.Map.Entry.comparingByKey());
        // The current block counts as 1 against the retention budget; older dirs
        // beyond that get deleted from oldest to newest.
        final int allowedOlder = Math.max(0, retain - 1);
        final int excess = candidates.size() - allowedOlder;
        for (int i = 0; i < excess; i++) {
            deleteDirectoryRecursively(candidates.get(i).getValue());
        }
    }

    @NonNull
    private static List<Path> listRecentDirs(@NonNull final Path recentRoot) throws IOException {
        try (var entries = Files.list(recentRoot)) {
            return entries.filter(Files::isDirectory).toList();
        }
    }

    private static long parseBlockDirName(@NonNull final Path dir) {
        try {
            return Long.parseLong(dir.getFileName().toString());
        } catch (final NumberFormatException ignored) {
            return -1L;
        }
    }

    @NonNull
    private Optional<Path> recentSnapshotDirectoryFor(final long blockNumber) {
        return Optional.of(Path.of(config.stateSnapshotRecentPath(), Long.toString(blockNumber)));
    }

    @NonNull
    private BinaryStateQueryResponse.Builder baseResponse() {
        return BinaryStateQueryResponse.newBuilder().stateMetadata(metadata);
    }

    /**
     * Returns `true` when the request carries a {@code block_specifier} — exactly
     * one of {@code retrieve_latest} or {@code block_number}, per the
     * {@code BlockRequest.block_specifier} convention in `block_access_service.proto`.
     * A request that sets neither is malformed and must be rejected with
     * {@link Code#INVALID_REQUEST} (distinct from a well-formed request for a block
     * we do not hold, which is {@link Code#NOT_FOUND}).
     */
    private static boolean hasBlockSpecifier(@NonNull final BinaryStateQuery request) {
        return request.hasRetrieveLatest() || request.hasBlockNumber();
    }

    /**
     * Returns `true` when the request targets the currently applied state. Assumes
     * the request has already passed {@link #hasBlockSpecifier}. The plugin only
     * ever serves the latest immutable state, so a `block_number` other than the
     * latest applied block does not match — callers get {@link Code#NOT_FOUND},
     * which affirms the latest-only API rather than rejecting the request shape.
     * Block 0 (genesis) is a valid `block_number`.
     */
    private boolean matchesLatestBlock(@NonNull final BinaryStateQuery request) {
        if (request.hasRetrieveLatest() && Boolean.TRUE.equals(request.retrieveLatest())) {
            return true;
        }
        if (request.hasBlockNumber()) {
            return request.blockNumber() == metadata.blockNumber();
        }
        return false;
    }

    @NonNull
    private static Bytes rootHashOf(@Nullable final VirtualMapState state) {
        if (state == null || state.getRoot() == null) {
            return Bytes.EMPTY;
        }
        try {
            return Bytes.wrap(state.getRoot().getHash().copyToByteArray());
        } catch (final RuntimeException e) {
            return Bytes.EMPTY;
        }
    }

    private static long sizeOf(@Nullable final VirtualMapState state) {
        if (state == null || state.getRoot() == null) {
            return 0L;
        }
        return state.getRoot().size();
    }

    private static void deleteDirectoryRecursively(@NonNull final Path root) {
        try {
            if (!Files.exists(root)) {
                return;
            }
            try (var stream = Files.walk(root)) {
                stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
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

    @NonNull
    private static ScheduledExecutorService newSingleThreadExecutor(@NonNull final String threadName) {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            return t;
        });
    }

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
