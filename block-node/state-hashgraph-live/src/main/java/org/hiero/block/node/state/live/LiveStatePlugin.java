// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.state.BinaryState;
import org.hiero.base.file.FileSystemManager;
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
import org.hiero.block.api.BinaryStateQuery;
import org.hiero.block.api.BinaryStateQueryResponse;
import org.hiero.block.api.BinaryStateQueryResponse.Code;
import org.hiero.block.api.StateMetadata;
import org.hiero.block.api.StateServiceInterface;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification.StateUpdateType;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
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
public final class LiveStatePlugin implements BlockNodePlugin, BlockNotificationHandler, StateServiceInterface {

    private static final System.Logger LOGGER = System.getLogger(LiveStatePlugin.class.getName());

    private BlockNodeContext context;
    private LiveStateConfig config;
    private VirtualMapStateLifecycleManager lifecycleManager;
    private StateMetadataStore metadataStore;
    private StateChangeApplier applier;

    /** Latest applied state metadata. Volatile because reads happen on query threads. */
    private volatile StateMetadata metadata = StateMetadata.DEFAULT;

    private final ConcurrentSkipListMap<Long, BlockUnparsed> pendingBlocks = new ConcurrentSkipListMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean degraded = new AtomicBoolean(false);
    private final java.util.concurrent.atomic.AtomicLong hashMismatchTotal = new java.util.concurrent.atomic.AtomicLong();

    private ScheduledExecutorService snapshotExecutor;
    private ScheduledExecutorService stateChangesExecutor;
    private ScheduledExecutorService catchUpExecutor;
    private volatile long lastSnapshottedBlock = -1L;

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
                LiveStateConfig.class,
                com.swirlds.merkledb.config.MerkleDbConfig.class,
                com.swirlds.virtualmap.config.VirtualMapConfig.class,
                org.hiero.consensus.config.PathsConfig.class);
    }

    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = context;
        this.config = context.configuration().getConfigData(LiveStateConfig.class);
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

        stateChangesExecutor = newSingleThreadExecutor("LiveState-apply");
        snapshotExecutor = newSingleThreadExecutor("LiveState-snapshot");
        catchUpExecutor = newSingleThreadExecutor("LiveState-catchup");

        stateChangesExecutor.scheduleWithFixedDelay(
                this::applyPending, 0L, config.stateChangesApplyIntervalMillis(), TimeUnit.MILLISECONDS);
        snapshotExecutor.scheduleWithFixedDelay(
                this::saveSnapshot,
                config.snapshotIntervalMillis(),
                config.snapshotIntervalMillis(),
                TimeUnit.MILLISECONDS);

        // Catch-up runs on its own thread so start() returns fast. It sets ready=true
        // when the live state has been brought up to the latest historical block; query
        // traffic is gated NOT_READY until then.
        catchUpExecutor.execute(this::catchUpFromHistoricalBlocks);
    }

    @Override
    public void stop() {
        stopping.set(true);
        ready.set(false);
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
        if (!ready.get()) {
            return out.status(Code.NOT_READY).build();
        }
        if (request.keyBytes() == null || request.keyBytes().length() == 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (request.queueIndex() != 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!matchesLatestBlock(request)) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        final BinaryState binaryState = readableState();
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
        if (!ready.get()) {
            return out.status(Code.NOT_READY).build();
        }
        if (request.keyBytes() != null && request.keyBytes().length() > 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (request.queueIndex() != 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!matchesLatestBlock(request)) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        final BinaryState binaryState = readableState();
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
        if (!ready.get()) {
            return out.status(Code.NOT_READY).build();
        }
        if (request.keyBytes() != null && request.keyBytes().length() > 0L) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        if (!matchesLatestBlock(request)) {
            return out.status(Code.INVALID_REQUEST).build();
        }
        final int stateId = (int) request.stateId();
        final BinaryState binaryState = readableState();
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

    // ── Test hooks ──────────────────────────────────────────────────────────

    boolean isReady() {
        return ready.get();
    }

    @NonNull
    StateMetadata metadata() {
        return metadata;
    }

    @NonNull
    VirtualMapStateLifecycleManager lifecycleManager() {
        return lifecycleManager;
    }

    void applyPendingNow() {
        applyPending();
    }

    void saveSnapshotNow() {
        saveSnapshot();
    }

    /** Block the calling thread until catch-up completes or the timeout expires. Test-only. */
    boolean awaitReady(final long timeoutMillis) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!ready.get() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        return ready.get();
    }

    // ── Internals ───────────────────────────────────────────────────────────

    private void loadPersistedState() {
        try {
            metadata = metadataStore.load().orElse(StateMetadata.DEFAULT);
        } catch (final IOException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Unable to load state metadata; starting from genesis", e);
            metadata = StateMetadata.DEFAULT;
        }
        final Optional<Path> snapshotDir = snapshotDirectoryFor(metadata.blockNumber());
        if (snapshotDir.isPresent() && Files.isDirectory(snapshotDir.get())) {
            try {
                lifecycleManager.loadSnapshot(snapshotDir.get());
                LOGGER.log(System.Logger.Level.INFO, "Loaded state snapshot from {0}", snapshotDir.get());
            } catch (final IOException e) {
                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "Snapshot at {0} unreadable; continuing with eager genesis state",
                        snapshotDir.get(),
                        e);
            }
        }
    }

    void applyPending() {
        while (!pendingBlocks.isEmpty() && !stopping.get()) {
            final boolean atGenesis =
                    metadata.blockNumber() == 0L && metadata.stateRootHash().length() == 0L;
            final long expectedNext = atGenesis ? pendingBlocks.firstKey() : metadata.blockNumber() + 1L;
            final BlockUnparsed block = pendingBlocks.remove(expectedNext);
            if (block == null) {
                return;
            }
            applyOne(block);
        }
    }

    /**
     * Bring the live state up to the latest block the Block-Node has on hand by reading
     * blocks from {@link HistoricalBlockFacility} and feeding them through the same
     * {@link #applyPending()} loop that handles verification-delivered blocks.
     *
     * <p>Runs on the {@code LiveState-catchup} thread. When complete (or if there is
     * nothing to catch up to) sets {@link #ready} so query traffic stops returning
     * {@code NOT_READY}.
     */
    void catchUpFromHistoricalBlocks() {
        try {
            final HistoricalBlockFacility historic = context == null ? null : context.historicalBlockProvider();
            if (historic == null) {
                ready.set(true);
                return;
            }
            final BlockRangeSet available = historic.availableBlocks();
            if (available == null || available.size() == 0L) {
                ready.set(true);
                return;
            }
            final long latest = available.max();
            final boolean atGenesis =
                    metadata.blockNumber() == 0L && metadata.stateRootHash().length() == 0L;
            final long start = atGenesis ? available.min() : metadata.blockNumber() + 1L;
            if (start > latest) {
                ready.set(true);
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
            ready.set(true);
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

    private void applyOne(@NonNull final BlockUnparsed block) {
        if (degraded.get()) {
            return;
        }
        // Inspect the block for block number + footer hash BEFORE mutating state so we can
        // reject a divergent slice without leaving a half-applied mutable behind.
        final StateChangeApplier.ApplyResult preview = applier.inspectBlock(block);
        if (preview.blockNumber() < 0L) {
            LOGGER.log(System.Logger.Level.WARNING, "Refusing to apply block with unparseable header");
            return;
        }
        if (!validateStartHash(preview.startOfBlockStateRootHash())) {
            hashMismatchTotal.incrementAndGet();
            degraded.set(true);
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "State hash mismatch at block {0}: footer.startOfBlockStateRootHash diverges from live state; "
                            + "plugin marked degraded",
                    preview.blockNumber());
            return;
        }

        final VirtualMapState mutable = lifecycleManager.getMutableState();
        final StateChangeApplier.ApplyResult result = applier.applyBlock(mutable, block);
        final VirtualMapState immutable = lifecycleManager.copyMutableState();
        final StateMetadata updated = StateMetadata.newBuilder()
                .blockNumber(result.blockNumber())
                .roundNumber(result.roundNumber() < 0L ? metadata.roundNumber() : result.roundNumber())
                .stateRootHash(rootHashOf(immutable))
                .stateSize(sizeOf(immutable))
                .build();
        metadata = updated;
        context.blockMessaging()
                .sendStateUpdate(new StateUpdateNotification(
                        StateUpdateType.VERIFIED,
                        updated.blockNumber(),
                        updated.roundNumber(),
                        updated.stateRootHash(),
                        updated.stateSize()));
    }

    /**
     * Compare the block's {@code BlockFooter.startOfBlockStateRootHash} with the current
     * live state's hash. The check is skipped at genesis: when no block has been applied
     * yet, the expected start hash is empty / all-zeros and we accept either shape.
     */
    private boolean validateStartHash(@NonNull final Bytes startHash) {
        final boolean atGenesis = metadata.blockNumber() == 0L && metadata.stateRootHash().length() == 0L;
        if (atGenesis) {
            return startHash.length() == 0L || isAllZeros(startHash);
        }
        final Bytes liveHash = rootHashOf(lifecycleManager.getMutableState());
        return liveHash.equals(startHash);
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
        final Optional<Path> targetOpt = snapshotDirectoryFor(snapshot.blockNumber());
        if (targetOpt.isEmpty()) {
            return;
        }
        final Path target = targetOpt.get();
        try {
            Files.createDirectories(target.getParent());
            if (!Files.exists(target)) {
                lifecycleManager.createSnapshot(lifecycleManager.getLatestImmutableState(), target);
            }
            pruneRecentExcept(snapshot.blockNumber());
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

    private void pruneRecentExcept(final long keep) throws IOException {
        final Path recentRoot = Path.of(config.stateSnapshotRecentPath());
        final Path historicRoot = Path.of(config.stateSnapshotHistoricPath());
        if (!Files.isDirectory(recentRoot)) {
            return;
        }
        try (var entries = Files.list(recentRoot)) {
            entries.filter(Files::isDirectory)
                    .filter(p -> !p.getFileName().toString().equals(Long.toString(keep)))
                    .forEach(p -> archiveThenDelete(p, historicRoot));
        }
        enforceHistoricRetention(historicRoot);
    }

    /**
     * Delete the oldest historic tar archives until the archive count is at or under
     * {@code historicArchiveRetentionCount}. A value of {@code 0} disables the policy.
     * Mirrors {@code BlockFileHistoricPlugin.cleanup} — track count and prune the
     * lowest-block-number archive until under threshold.
     */
    private void enforceHistoricRetention(@NonNull final Path historicRoot) {
        final long retention = config.historicArchiveRetentionCount();
        if (retention <= 0L || !Files.isDirectory(historicRoot)) {
            return;
        }
        final java.util.List<java.util.Map.Entry<Long, Path>> archives;
        try (var entries = Files.list(historicRoot)) {
            archives = entries.filter(Files::isRegularFile)
                    .map(p -> {
                        final String name = p.getFileName().toString();
                        if (!name.endsWith(".tar")) {
                            return null;
                        }
                        try {
                            final long block = Long.parseLong(name.substring(0, name.length() - 4));
                            return java.util.Map.entry(block, p);
                        } catch (final NumberFormatException ignored) {
                            return null;
                        }
                    })
                    .filter(java.util.Objects::nonNull)
                    .sorted(java.util.Map.Entry.comparingByKey())
                    .toList();
        } catch (final IOException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Failed to enumerate historic tar archives", e);
            return;
        }
        long excess = archives.size() - retention;
        int i = 0;
        while (excess > 0 && i < archives.size()) {
            final Path victim = archives.get(i).getValue();
            try {
                Files.delete(victim);
            } catch (final IOException e) {
                LOGGER.log(System.Logger.Level.WARNING, "Failed to delete historic archive " + victim, e);
            }
            excess--;
            i++;
        }
    }

    /**
     * Tar an older recent-snapshot directory to the historic store, then delete it from
     * {@code recent/}. Failures are logged but non-fatal — a leftover recent directory is
     * harmless beyond disk usage, and the next snapshot cycle will retry the prune.
     */
    private static void archiveThenDelete(@NonNull final Path source, @NonNull final Path historicRoot) {
        final String name = source.getFileName().toString();
        final long blockNumber;
        try {
            blockNumber = Long.parseLong(name);
        } catch (final NumberFormatException e) {
            // Not a block-number directory — just delete.
            deleteRecursively(source);
            return;
        }
        try {
            SnapshotArchiver.archive(source, historicRoot, blockNumber);
        } catch (final IOException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Failed to archive snapshot " + source + " to historic", e);
            return; // keep the recent directory so next cycle can retry the archive.
        }
        deleteRecursively(source);
    }

    @NonNull
    private Optional<Path> snapshotDirectoryFor(final long blockNumber) {
        return Optional.of(Path.of(config.stateSnapshotRecentPath(), Long.toString(blockNumber)));
    }

    @NonNull
    private BinaryStateQueryResponse.Builder baseResponse() {
        return BinaryStateQueryResponse.newBuilder().stateMetadata(metadata);
    }

    private boolean matchesLatestBlock(@NonNull final BinaryStateQuery request) {
        return request.blockNumber() == 0L || request.blockNumber() == metadata.blockNumber();
    }

    /**
     * Choose the state to read from. Before the first {@code copyMutableState}, the
     * latest-immutable reference is null and reads fall back to the mutable state.
     */
    @NonNull
    private VirtualMapState readableState() {
        final VirtualMapState immutable = lifecycleManager.getLatestImmutableState();
        return immutable != null ? immutable : lifecycleManager.getMutableState();
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

    private static void deleteRecursively(@NonNull final Path root) {
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
