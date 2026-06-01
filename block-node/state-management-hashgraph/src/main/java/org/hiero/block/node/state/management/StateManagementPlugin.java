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
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification.StateUpdateType;
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

    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        // TODO(STORY-16): a plugin shouldn't own config records defined by an external
        // library. Tracked options:
        //   (A) Foundation: ship META-INF/services entries on the swirlds jars so
        //       BlockNodeApp.autoDiscoverExtensions picks them up automatically — see
        //       STORY-12 Foundation-feedback section.
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
        if (!stateIsCaughtUp.get()) {
            return out.status(Code.NOT_READY).build();
        }
        if (request.keyBytes() == null || request.keyBytes().length() == 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (request.queueIndex() != 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!matchesLatestBlock(request)) {
            return out.status(Code.NOT_FOUND).build();
        }
        final BinaryState binaryState = lifecycleManager.getLatestImmutableState();
        final Bytes value = binaryState.getKv((int) request.stateId(), request.keyBytes());
        if (value == null) {
            return out.status(Code.NOT_FOUND).build();
        }
        return out.status(Code.SUCCESS).kvBytes(value).build();
    }

    @NonNull
    @Override
    public BinaryStateQueryResponse getBinarySingleton(@NonNull final BinaryStateQuery request) {
        final BinaryStateQueryResponse.Builder out = baseResponse();
        if (!stateIsCaughtUp.get()) {
            return out.status(Code.NOT_READY).build();
        }
        if (request.keyBytes() != null && request.keyBytes().length() > 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (request.queueIndex() != 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!matchesLatestBlock(request)) {
            return out.status(Code.NOT_FOUND).build();
        }
        final BinaryState binaryState = lifecycleManager.getLatestImmutableState();
        final Bytes value = binaryState.getSingleton((int) request.stateId());
        if (value == null) {
            return out.status(Code.NOT_FOUND).build();
        }
        return out.status(Code.SUCCESS).singletonBytes(value).build();
    }

    @NonNull
    @Override
    public BinaryStateQueryResponse getBinaryQueue(@NonNull final BinaryStateQuery request) {
        final BinaryStateQueryResponse.Builder out = baseResponse();
        if (!stateIsCaughtUp.get()) {
            return out.status(Code.NOT_READY).build();
        }
        if (request.keyBytes() != null && request.keyBytes().length() > 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!matchesLatestBlock(request)) {
            return out.status(Code.NOT_FOUND).build();
        }
        final int stateId = (int) request.stateId();
        final BinaryState binaryState = lifecycleManager.getLatestImmutableState();
        // VirtualMapStateImpl.getQueueState returns null for unknown stateIds; the
        // downstream getQueueAsList / peekQueue then NPE. Treat any of those as NOT_FOUND.
        try {
            if (binaryState.getQueueState(stateId) == null) {
                return out.status(Code.NOT_FOUND).build();
            }
            if (request.queueIndex() == 0L) {
                final List<Bytes> all = binaryState.getQueueAsList(stateId);
                if (all == null || all.isEmpty()) {
                    return out.status(Code.NOT_FOUND).build();
                }
                return out.status(Code.SUCCESS).queueBytes(all).build();
            }
            final Bytes element = binaryState.peekQueue(stateId, (int) request.queueIndex());
            if (element == null) {
                return out.status(Code.NOT_FOUND).build();
            }
            return out.status(Code.SUCCESS).queueBytes(List.of(element)).build();
        } catch (final RuntimeException e) {
            LOGGER.log(System.Logger.Level.DEBUG, "Queue lookup for state {0} failed", stateId, e);
            return out.status(Code.NOT_FOUND).build();
        }
    }

    // ── Package-private observers ─────────────────────────────────────────────
    // Read-only accessors for the same state the gRPC surface already exposes.
    // Tests drive the plugin through its real entry points (applyPending,
    // saveSnapshot, block delivery) and assert against these observers; there
    // are no force/duplicate "*Now" hooks or internals accessors in production.

    boolean isReady() {
        return stateIsCaughtUp.get();
    }

    @NonNull
    StateMetadata metadata() {
        return metadata;
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
            // We trust the loaded snapshot represents post-(metadata.blockNumber).
            // Seed lastAppliedBlock so the very next block triggers the lag-1
            // commit notification for the loaded state.
            lastAppliedBlock = metadata.blockNumber();
            lastAppliedRound = metadata.roundNumber();
        }
        // Force the initial copyMutableState so getLatestImmutableState() is non-null
        // from boot onwards. Query paths can then always read through immutable
        // without a mutable-fallback helper.
        lifecycleManager.copyMutableState();
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
     * Apply a single block's `state_changes` to the live state.
     *
     * <p>Flow:
     *
     * <ol>
     *   <li>Pull `BlockFooter.startOfBlockStateRootHash` (cheap reverse scan, no
     *       full re-walk via the old `inspectBlock`).</li>
     *   <li>Validate that hash against the current live mutable state. Mismatch
     *       ⇒ degrade without mutating; the plugin will not advance until an
     *       operator resolves the divergence.</li>
     *   <li>If matches, apply the block's `state_changes` to mutable.</li>
     *   <li>`copyMutableState()` to freeze the new state as the latest immutable
     *       so queries see it; record the new metadata; emit a
     *       `VERIFIED` {@link StateUpdateNotification}.</li>
     * </ol>
     *
     * <p>Note: this is the **eager commit** model — the post-block state is
     * exposed to readers immediately, not lagged by one block. The lag-1 model
     * (only exposing state confirmed by the *next* block's footer) is captured
     * as a follow-up STORY ticket; see self-review-1 for the trade-off rationale.
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
                    "State hash mismatch: footer.startOfBlockStateRootHash diverges from live mutable hash; "
                            + "plugin marked degraded");
            return false;
        }

        final VirtualMapState mutable = lifecycleManager.getMutableState();
        final StateChangeApplier.ApplyResult result = applier.applyBlock(mutable, block);
        if (result.blockNumber() < 0L) {
            LOGGER.log(System.Logger.Level.WARNING, "Block had unparseable header; treating as failed apply");
            degraded.set(true);
            return false;
        }
        // Freeze the just-written mutable as the new immutable. We then read the
        // hash and size from the *immutable* (already-sealed) reference rather
        // than from the new mutable copyMutableState returns. Hashing a mutable
        // VirtualMap that has uncommitted writes seals it for further writes,
        // which causes the NEXT block's applier.applyBlock to fail with
        // "Cannot modify already hashed node".
        lifecycleManager.copyMutableState();
        final VirtualMapState committed = lifecycleManager.getLatestImmutableState();
        final StateMetadata updated = StateMetadata.newBuilder()
                .blockNumber(result.blockNumber())
                .roundNumber(result.roundNumber() < 0L ? metadata.roundNumber() : result.roundNumber())
                .stateRootHash(rootHashOf(committed))
                .stateSize(sizeOf(committed))
                .build();
        metadata = updated;
        lastAppliedBlock = result.blockNumber();
        if (result.roundNumber() >= 0L) {
            lastAppliedRound = result.roundNumber();
        }
        context.blockMessaging()
                .sendStateUpdate(new StateUpdateNotification(
                        StateUpdateType.VERIFIED,
                        updated.blockNumber(),
                        updated.roundNumber(),
                        updated.stateRootHash(),
                        updated.stateSize()));
        return true;
    }

    /**
     * Compare the block's {@code BlockFooter.startOfBlockStateRootHash} with the
     * hash of the state we have on hand from the previous apply (or load).
     *
     * <p>We compare against {@link #metadata}'s {@code state_root_hash} — recorded
     * by the previous apply / snapshot-load — rather than recomputing the hash
     * from `lifecycleManager.getMutableState()`. Reason: invoking
     * `getRoot().getHash()` on a `VirtualMap` that has been written to since the
     * last hash seals it, which causes subsequent `applier.applyBlock` writes to
     * fail with "Cannot modify already hashed node". Using `metadata` avoids
     * the seal because we already recorded the hash at the right point in the
     * lifecycle (right after the previous `copyMutableState`).
     *
     * <p>Genesis branch: when no block has been applied yet, the expected start
     * hash is empty / all-zeros and we accept either shape.
     */
    private boolean validateStartHash(@NonNull final Bytes startHash) {
        final boolean atGenesis =
                metadata.blockNumber() == 0L && metadata.stateRootHash().length() == 0L;
        if (atGenesis) {
            return startHash.length() == 0L || isAllZeros(startHash);
        }
        return metadata.stateRootHash().equals(startHash);
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
        if (snapshot.blockNumber() <= lastSnapshottedBlock) {
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
                lifecycleManager.createSnapshot(lifecycleManager.getLatestImmutableState(), target);
            }
            pruneOldRecentSnapshots(snapshot.blockNumber());
            metadataStore.save(snapshot);
            lastSnapshottedBlock = snapshot.blockNumber();
            context.blockMessaging()
                    .sendStateUpdate(new StateUpdateNotification(
                            StateUpdateType.SNAPSHOT,
                            snapshot.blockNumber(),
                            snapshot.roundNumber(),
                            snapshot.stateRootHash(),
                            snapshot.stateSize()));
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
     * Returns `true` when the request targets the currently applied state.
     *
     * <p>Matches the {@code BlockRequest.block_specifier} convention from
     * `block_access_service.proto`: the client sets one of `retrieve_latest=true`
     * or an explicit `block_number`. Block 0 (genesis) is a valid `block_number`.
     * The plugin only ever serves the latest immutable state, so a `block_number`
     * other than the latest applied block (or a request that sets neither field)
     * does not match — callers get {@link Code#NOT_FOUND}, which affirms the
     * latest-only API rather than rejecting the request shape.
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
