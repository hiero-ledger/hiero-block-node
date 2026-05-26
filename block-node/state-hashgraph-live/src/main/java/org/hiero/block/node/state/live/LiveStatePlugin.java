// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import com.hedera.pbj.runtime.io.buffer.Bytes;
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
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification.StateUpdateType;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/**
 * Beta plugin that maintains a live, queryable copy of Hashgraph network state inside
 * the Block Node by replaying verified block-stream state changes.
 *
 * <p>Backed by an in-house {@link LiveState} so the plugin can ship without dragging
 * the {@code swirlds-state-impl} / {@code swirlds-virtualmap} dependency chain into the
 * block-node build. STORY-5 fills out {@link StateChangeApplier} to actually mutate the
 * {@link LiveState} from each verified block; until then this plugin tracks block /
 * round numbers and emits {@link StateUpdateNotification}s but leaves the in-memory
 * maps empty.
 *
 * <p>See {@code docs/design/state/live-state.md} for the full design.
 */
public final class LiveStatePlugin implements BlockNodePlugin, BlockNotificationHandler, StateServiceInterface {

    private static final System.Logger LOGGER = System.getLogger(LiveStatePlugin.class.getName());
    private static final String SNAPSHOT_FILE_NAME = "live-state.bin";

    private BlockNodeContext context;
    private LiveStateConfig config;
    private StateMetadataStore metadataStore;
    private StateChangeApplier applier;

    private final LiveState liveState = new LiveState();
    private volatile StateMetadata metadata = StateMetadata.DEFAULT;

    private final ConcurrentSkipListMap<Long, BlockUnparsed> pendingBlocks = new ConcurrentSkipListMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);

    private ScheduledExecutorService snapshotExecutor;
    private ScheduledExecutorService stateChangesExecutor;
    private volatile long lastSnapshottedBlock = -1L;

    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(LiveStateConfig.class);
    }

    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = context;
        this.config = context.configuration().getConfigData(LiveStateConfig.class);
        this.metadataStore = new StateMetadataStore(Path.of(config.stateMetadataPath()));
        this.applier = new StateChangeApplier();
        serviceBuilder.registerGrpcService(this);
    }

    @Override
    public void start() {
        loadPersistedState();
        context.blockMessaging().registerBlockNotificationHandler(this, true, name());

        stateChangesExecutor = newSingleThreadExecutor("LiveState-apply");
        snapshotExecutor = newSingleThreadExecutor("LiveState-snapshot");

        stateChangesExecutor.scheduleWithFixedDelay(
                this::applyPending, 0L, config.stateChangesApplyIntervalMillis(), TimeUnit.MILLISECONDS);
        snapshotExecutor.scheduleWithFixedDelay(
                this::saveSnapshot,
                config.snapshotIntervalMillis(),
                config.snapshotIntervalMillis(),
                TimeUnit.MILLISECONDS);

        ready.set(true);
    }

    @Override
    public void stop() {
        ready.set(false);
        if (context != null) {
            try {
                context.blockMessaging().unregisterBlockNotificationHandler(this);
            } catch (final RuntimeException ignored) {
                // facility may already be torn down — non-fatal.
            }
        }
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
        final Bytes value = liveState.getKv((int) request.stateId(), request.keyBytes());
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
        final Bytes value = liveState.getSingleton((int) request.stateId());
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
        if (request.queueIndex() == 0L) {
            final List<Bytes> all = liveState.getQueueAsList(stateId);
            if (all.isEmpty()) {
                return out.status(Code.NOT_FOUND).build();
            }
            return out.status(Code.SUCCESS).queueBytes(all).build();
        }
        final Bytes element = liveState.peekQueue(stateId, (int) request.queueIndex());
        if (element == null) {
            return out.status(Code.NOT_FOUND).build();
        }
        return out.status(Code.SUCCESS).queueBytes(List.of(element)).build();
    }

    @NonNull
    private BinaryStateQueryResponse.Builder baseResponse() {
        return BinaryStateQueryResponse.newBuilder().stateMetadata(metadata);
    }

    /**
     * Live-state v1 only serves the latest applied block. A request that pins a specific
     * block_number must match the current metadata exactly; 0 is treated as "latest".
     */
    private boolean matchesLatestBlock(@NonNull final BinaryStateQuery request) {
        return request.blockNumber() == 0L || request.blockNumber() == metadata.blockNumber();
    }

    // ── Test hooks ──────────────────────────────────────────────────────────

    /** {@code true} once {@link #start()} has finished initialisation. */
    boolean isReady() {
        return ready.get();
    }

    @NonNull
    StateMetadata metadata() {
        return metadata;
    }

    @NonNull
    LiveState liveState() {
        return liveState;
    }

    /** Drives one apply pass synchronously. Used in tests to avoid waiting for the executor. */
    void applyPendingNow() {
        applyPending();
    }

    /** Writes a snapshot immediately. Used in tests. */
    void saveSnapshotNow() {
        saveSnapshot();
    }

    // ── Internals ───────────────────────────────────────────────────────────

    private void loadPersistedState() {
        try {
            metadata = metadataStore.load().orElse(StateMetadata.DEFAULT);
        } catch (final IOException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Unable to load state metadata; starting from genesis", e);
            metadata = StateMetadata.DEFAULT;
        }
        final Optional<Path> snapshot = snapshotPathFor(metadata.blockNumber());
        if (snapshot.isPresent() && Files.exists(snapshot.get())) {
            try {
                LiveStateSnapshotIO.read(snapshot.get(), liveState);
                LOGGER.log(System.Logger.Level.INFO, "Restored live state from {0}", snapshot.get());
            } catch (final IOException e) {
                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "Snapshot at {0} unreadable; resetting to empty in-memory state",
                        snapshot.get(),
                        e);
                liveState.restoreFrom(java.util.Map.of(), java.util.Map.of(), java.util.Map.of());
            }
        }
    }

    void applyPending() {
        // Drain in number order. At genesis (no metadata yet) accept whatever the lowest
        // queued block is. After that, require strict +1 ordering and stop on the first gap.
        while (!pendingBlocks.isEmpty() && ready.get()) {
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

    private void applyOne(@NonNull final BlockUnparsed block) {
        final StateChangeApplier.ApplyResult result = applier.applyBlock(liveState, block);
        if (result.blockNumber() < 0L) {
            LOGGER.log(System.Logger.Level.WARNING, "Refusing to apply block with unparseable header");
            return;
        }
        final StateMetadata updated = StateMetadata.newBuilder()
                .blockNumber(result.blockNumber())
                .roundNumber(result.roundNumber() < 0L ? metadata.roundNumber() : result.roundNumber())
                .stateRootHash(liveState.computeHash())
                .stateSize(liveState.size())
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

    void saveSnapshot() {
        final StateMetadata snapshot = metadata;
        if (snapshot.blockNumber() <= lastSnapshottedBlock) {
            return;
        }
        final Optional<Path> target = snapshotPathFor(snapshot.blockNumber());
        if (target.isEmpty()) {
            return;
        }
        try {
            LiveStateSnapshotIO.write(target.get(), liveState);
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
        if (!Files.isDirectory(recentRoot)) {
            return;
        }
        try (var entries = Files.list(recentRoot)) {
            entries.filter(Files::isDirectory)
                    .filter(p -> !p.getFileName().toString().equals(Long.toString(keep)))
                    .forEach(LiveStatePlugin::deleteRecursively);
        }
    }

    @NonNull
    private Optional<Path> snapshotPathFor(final long blockNumber) {
        final Path dir = Path.of(config.stateSnapshotRecentPath(), Long.toString(blockNumber));
        return Optional.of(dir.resolve(SNAPSHOT_FILE_NAME));
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
