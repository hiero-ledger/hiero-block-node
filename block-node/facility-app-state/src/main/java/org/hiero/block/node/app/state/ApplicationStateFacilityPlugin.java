// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.state;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.app.state.ApplicationStateUtility.filterToUniqueConnections;
import static org.hiero.block.node.app.state.ApplicationStateUtility.isNewerHistory;
import static org.hiero.block.node.app.state.ApplicationStateUtility.loadNetworkData;
import static org.hiero.block.node.app.state.ApplicationStateUtility.mergeRanges;
import static org.hiero.block.node.app.state.ApplicationStateUtility.publisherConnectionsFrom;
import static org.hiero.block.node.app.state.ApplicationStateUtility.toBlockRange;
import static org.hiero.block.node.app.state.ApplicationStateUtility.validateAddressBook;
import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.lang.System.Logger;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.NetworkData;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.api.TssData;
import org.hiero.block.internal.BlockRangesState;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
import org.hiero.block.node.spi.blockmessaging.AvailableBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;

/// The block node application state facility. This plugin owns the mutable application state
/// (TSS data, address book history, stored and available block ranges, and the connection sets
/// reported by the `/statusz` endpoints), persists it to disk, and dispatches changes to the other
/// plugins through the messaging facility.
///
/// The messaging facility must be started before this plugin, because loading the persisted state
/// dispatches notifications immediately. The plugin must also be initialized before any block
/// provider plugin, because providers may report blocks from their own `init()`.
public class ApplicationStateFacilityPlugin implements ApplicationStateFacility {
    /// An address book history paired with the lookup index built from it, so both can be
    /// installed with one compare-and-set.
    ///
    /// @param history the current history, null until the first one is accepted
    /// @param index the index built from that history
    record AddressBookState(RangedAddressBookHistory history, NavigableMap<Long, RangedNodeAddressBook> index) {
        /// The state before any history has been accepted.
        static final AddressBookState EMPTY =
                new AddressBookState(null, Collections.unmodifiableNavigableMap(new TreeMap<>()));
    }

    /// The logger for this class.
    private static final Logger LOGGER = System.getLogger(ApplicationStateFacilityPlugin.class.getCanonicalName());
    /// Metric key for the oldest historical block available
    public static final MetricKey<ObservableGauge> METRIC_APP_HISTORICAL_OLDEST_BLOCK =
            MetricKey.of("app_historical_oldest_block", ObservableGauge.class).addCategory(METRICS_CATEGORY);
    /// Metric key for the newest historical block available
    public static final MetricKey<ObservableGauge> METRIC_APP_HISTORICAL_NEWEST_BLOCK =
            MetricKey.of("app_historical_newest_block", ObservableGauge.class).addCategory(METRICS_CATEGORY);
    /// Number of stored blocks between automatic persistence of the block range sets
    private static final long BLOCK_RANGE_PERSIST_INTERVAL = 1000;

    /// The block node context, set in [#init]. Volatile because plugin threads read it.
    private volatile BlockNodeContext context;
    /// The configuration for the application state facility, set in [#init].
    private volatile ApplicationStateConfig appStateConfig;
    /// The current TSS data. Compare-and-set, not plainly assigned: verification threads and the
    /// TSS bootstrap scanner both update it, and check-then-assign lets older data win.
    final AtomicReference<TssData> currentTssData = new AtomicReference<>();
    /// The current RSA address-book history and its lookup index, installed together so a reader
    /// never sees an index built from a different history. Compare-and-set for the same reason as
    /// currentTssData: the RSA bootstrap plugin updates it from two independent executors.
    final AtomicReference<AddressBookState> addressBookState = new AtomicReference<>(AddressBookState.EMPTY);
    /// The latest merged stored-blocks list (stored ConcurrentLongRangeSet merged with available
    /// blocks). Updated by [#addStoredBlockRange] and [#updateAvailableBlocks].
    /// Provider threads update it concurrently, so it is compare-and-set in
    /// `refreshStoredBlocks` rather than plainly assigned.
    final AtomicReference<List<BlockRange>> currentStoredBlocks = new AtomicReference<>(List.of());
    /// The current available-blocks list (union across all providers). Updated by
    /// [#updateAvailableBlocks] and [#start]. Compare-and-set for the same reason as
    /// `currentStoredBlocks`.
    final AtomicReference<List<BlockRange>> currentAvailableBlocks = new AtomicReference<>(List.of());
    /// Blocks reported as stored by plugins that do not serve them for retrieval
    final ConcurrentLongRangeSet storedBlocks = new ConcurrentLongRangeSet();
    /// Known inbound publishers loaded from configuration on startup; exposed for /statusz/inbound.
    private final AtomicReference<NetworkData> knownPublishers = new AtomicReference<>(NetworkData.DEFAULT);
    /// Designated inbound partners loaded from configuration on startup; exposed for /statusz/inbound.
    private final AtomicReference<NetworkData> inboundPartners = new AtomicReference<>(NetworkData.DEFAULT);
    /// Designated outbound partners loaded from configuration on startup; exposed for /statusz/outbound.
    private final AtomicReference<NetworkData> outboundPartners = new AtomicReference<>(NetworkData.DEFAULT);
    /// Backfill source connections reported by the backfill plugin; exposed for both /statusz endpoints.
    private final AtomicReference<NetworkData> backfillSources = new AtomicReference<>(NetworkData.DEFAULT);
    /// Block count at the time of the last scheduled persist; only read/written by the dispatcher thread
    private long lastPersistedBlockCount = 0;
    /// The ScheduledExecutorService used by this facility to run the periodic block-range
    /// persist check and the queued TSS data and address book syncs.
    /// Volatile because the update methods submit to it from arbitrary plugin threads.
    private volatile ScheduledExecutorService applicationStateExecutor;
    /// The next expected block for publishers, used by Server Status plugins
    /// and set by Publisher plugins
    private volatile long nextExpectedBlock = -1L;

    /// {@inheritDoc}
    ///
    /// Captures the context and configuration and registers the oldest and newest block gauges.
    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        this.context = context;
        this.appStateConfig = context.configuration().getConfigData(ApplicationStateConfig.class);
        final HistoricalBlockFacility historicalBlocks = context.historicalBlockProvider();
        context.metricRegistry()
                .register(ObservableGauge.builder(METRIC_APP_HISTORICAL_OLDEST_BLOCK)
                        .setDescription("The oldest block the BN has access to")
                        .observe(() -> historicalBlocks.availableBlocks().min()));
        context.metricRegistry()
                .register(ObservableGauge.builder(METRIC_APP_HISTORICAL_NEWEST_BLOCK)
                        .setDescription("The newest block the BN has")
                        .observe(() -> historicalBlocks.availableBlocks().max()));
    }

    /// {@inheritDoc}
    ///
    /// Loads the persisted state, dispatching initial notifications directly as each datum is loaded,
    /// then dispatches the available and stored blocks the providers loaded during `init()`. Finally
    /// creates the dispatcher thread and schedules the block-range persist-interval check on it.
    ///
    /// The dispatcher is created last on purpose: the load is single threaded, so its updates
    /// persist and dispatch inline rather than being handed to a thread that does not exist yet.
    @Override
    public void start() {
        loadApplicationState();

        // Anything dispatched during plugin init() was published before the messaging facility attached
        // any handler, so it was lost. Clear the snapshots so the startup available and stored blocks
        // (including the stored ranges just loaded) are dispatched now, whatever init() already recorded.
        currentAvailableBlocks.set(List.of());
        currentStoredBlocks.set(List.of());
        updateAvailableBlocks();

        // Create the dispatcher thread and schedule the periodic block-range persist-interval check on it.
        applicationStateExecutor = context.threadPoolManager()
                .createVirtualThreadScheduledExecutor(
                        1, "ApplicationStateDispatcher", ApplicationStateUtility::uncaughtExceptionHandler);
        applicationStateExecutor.scheduleAtFixedRate(
                this::persistBlockRangesIfDue,
                appStateConfig.updateInitialDelay(),
                appStateConfig.updateScanInterval(),
                TimeUnit.MILLISECONDS);
    }

    /// {@inheritDoc}
    ///
    /// Stops the dispatcher, letting queued syncs finish, and persists the block ranges regardless
    /// of the persist threshold. The messaging facility must still be running, and is stopped by the
    /// application after this plugin.
    @Override
    public void stop() {
        final ScheduledExecutorService executor = applicationStateExecutor;
        if (executor != null) {
            // Clear the field first so that an update arriving during shutdown syncs inline
            // instead of being handed to an executor that is going away.
            applicationStateExecutor = null;
            // shutdown() cancels the periodic persist check but still runs the syncs already queued
            // and lets a running write finish, so nothing needs to be redone here.
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    final String executorTerminationMsg = "applicationStateExecutor did not terminate in time";
                    LOGGER.log(INFO, executorTerminationMsg);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // Persist all block ranges at shutdown regardless of threshold.
        persistBlockRanges();
    }

    private HistoricalBlockFacility historicalBlockFacility() {
        return context.historicalBlockProvider();
    }

    /// Periodically persists block ranges when the running total crosses a boundary.
    private void persistBlockRangesIfDue() {
        final long current = storedBlocks.size();
        if (current / BLOCK_RANGE_PERSIST_INTERVAL > lastPersistedBlockCount / BLOCK_RANGE_PERSIST_INTERVAL) {
            persistBlockRanges();
            lastPersistedBlockCount = current;
        }
    }

    /// {@inheritDoc}
    ///
    /// Installs the data immediately if it is newer than the currently stored value; persisting it
    /// and dispatching the notification happen on the ApplicationStateDispatcher thread.
    @Override
    public void updateTssData(TssData tssData) {
        if (tssData == null) {
            return;
        }
        // getAndUpdate retries internally, so no loop is needed here. The update function is pure, so
        // whether this call installed tssData can be recomputed from the value it replaced. Concurrent
        // callers each publish the newest value because syncTssData re-reads it.
        final TssData previous =
                currentTssData.getAndUpdate(current -> shouldInstall(tssData, current) ? tssData : current);
        if (shouldInstall(tssData, previous)) {
            runOnDispatcherThread(this::syncTssData);
        }
    }

    /// A candidate replaces the current TSS data only if it differs and is not valid from an earlier
    /// block, so a stale datum can never overwrite a newer one.
    private static boolean shouldInstall(final TssData candidate, final TssData current) {
        return !candidate.equals(current)
                && (current == null || candidate.validFromBlock() >= current.validFromBlock());
    }

    /// Runs one of the sync methods on the ApplicationStateDispatcher thread, or inline before that
    /// thread exists (startup) and after it is gone (shutdown). The executor is read once because
    /// it is volatile and shutdown clears it. Shutdown can still stop the executor between that read
    /// and the hand-off, so a rejected hand-off also runs inline rather than throwing into the caller.
    ///
    /// @param sync the sync method to run
    private void runOnDispatcherThread(final Runnable sync) {
        final ScheduledExecutorService executor = applicationStateExecutor;
        if (executor == null) {
            sync.run();
        } else {
            try {
                executor.execute(sync);
            } catch (final RejectedExecutionException e) {
                sync.run();
            }
        }
    }

    /// Persists the current TSS data and dispatches it to the plugins. Runs on the
    /// ApplicationStateDispatcher thread, so writes to the bootstrap file are serialized and a
    /// notification can never carry an older datum than one already dispatched. Reads the
    /// current value rather than taking one, so a queued run always publishes the newest.
    private void syncTssData() {
        final TssData tssData = currentTssData.get();
        if (tssData != null) {
            persistTssData(tssData);
            context.blockMessaging().sendTssDataUpdate(new TssDataNotification(tssData));
        }
    }

    @Override
    public void addStoredBlockRange(LongRange blockRange) {
        storedBlocks.add(blockRange);
        refreshStoredBlocks(historicalBlockFacility().availableBlocks());
    }

    @Override
    public NetworkData knownPublishers() {
        return knownPublishers.get();
    }

    @Override
    public NetworkData inboundPartners() {
        return inboundPartners.get();
    }

    @Override
    public NetworkData outboundPartners() {
        return outboundPartners.get();
    }

    @Override
    public NetworkData backfillSources() {
        return backfillSources.get();
    }

    @Override
    public long nextExpectedBlock() {
        return nextExpectedBlock;
    }

    @Override
    public TssData tssData() {
        return currentTssData.get();
    }

    @Override
    public RangedAddressBookHistory rangedAddressBookHistory() {
        return addressBookState.get().history();
    }

    @Override
    public List<BlockRange> storedBlocks() {
        return currentStoredBlocks.get();
    }

    @Override
    public void updateBackfillSources(NetworkData sources) {
        backfillSources.set(sources != null ? sources : NetworkData.DEFAULT);
    }

    @Override
    public void updateExpectedBlock(final long updatedExpectedBlock) {
        nextExpectedBlock = updatedExpectedBlock;
    }

    /// {@inheritDoc}
    ///
    /// Installs the history and its index immediately if the supplied history is newer than the
    /// currently stored value; persisting it, deriving the known publishers and dispatching the
    /// notification happen on the ApplicationStateDispatcher thread.
    ///
    /// @param history the history to store; must not be {@code null}
    /// @return {@code true} if accepted, {@code false} if rejected
    @Override
    public boolean updateAddressBookHistory(RangedAddressBookHistory history) {
        boolean updated = false;
        while (true) {
            final AddressBookState current = addressBookState.get();
            if (history == null || history.equals(current.history()) || !isNewerHistory(history, current.history())) {
                break;
            }
            if (addressBookState.compareAndSet(
                    current, new AddressBookState(history, AddressBookHistoryLookup.buildIndex(history)))) {
                // Update knownPublishers immediately on the caller's thread so that publisher
                // authentication reflects the new address book before this method returns.
                // The dispatcher also calls updateKnownPublishersFromAddressBook in
                // syncAddressBookHistory, where it reads the current (possibly newer) value.
                updateKnownPublishersFromAddressBook(history);
                runOnDispatcherThread(this::syncAddressBookHistory);
                updated = true;
                break;
            }
        }
        return updated;
    }

    /// Persists the current address book history, derives the known publishers from it and
    /// dispatches it to the plugins. Runs on the ApplicationStateDispatcher thread, so writes to the
    /// bootstrap file are serialized and a notification can never carry an older history than one
    /// already dispatched. Reads the current value rather than taking one, so a queued run always
    /// publishes the newest.
    private void syncAddressBookHistory() {
        final RangedAddressBookHistory history = addressBookState.get().history();
        if (history != null) {
            persistNodeAddressBookHistory(history);
            updateKnownPublishersFromAddressBook(history);
            context.blockMessaging().sendAddressBookHistoryUpdate(new AddressBookHistoryNotification(history));
        }
    }

    @Override
    public void updateAvailableBlocks() {
        // Capture one snapshot so both Available and Stored notifications are always derived from
        // the same moment in time, preserving the invariant stored >= available.
        final BlockRangeSet snapshot = historicalBlockFacility().availableBlocks();
        refreshAvailableBlocks(snapshot);
        refreshStoredBlocks(snapshot);
    }

    private void refreshAvailableBlocks(BlockRangeSet availableBlocks) {
        while (true) {
            final List<BlockRange> current = currentAvailableBlocks.get();
            final List<BlockRange> candidate = toBlockRange(availableBlocks);
            if (candidate.equals(current)) {
                // Nothing changed, either because there was no update or because another thread
                // already installed an identical snapshot; there is nothing to notify.
                break;
            }
            if (currentAvailableBlocks.compareAndSet(current, candidate)) {
                runOnDispatcherThread(this::syncAvailableBlocks);
                break;
            }
        }
    }

    /// Dispatches the current available blocks to the plugins. Runs on the ApplicationStateDispatcher
    /// thread and reads the current value rather than taking one, so a notification can never
    /// carry an older snapshot than one already dispatched.
    private void syncAvailableBlocks() {
        context.blockMessaging()
                .sendAvailableBlocksUpdate(new AvailableBlocksNotification(currentAvailableBlocks.get()));
    }

    private void refreshStoredBlocks(BlockRangeSet availableBlocks) {
        while (true) {
            final List<BlockRange> current = currentStoredBlocks.get();
            final List<BlockRange> candidate = mergeRanges(storedBlocks, availableBlocks);
            if (candidate.equals(current)) {
                break;
            }
            if (currentStoredBlocks.compareAndSet(current, candidate)) {
                runOnDispatcherThread(this::syncStoredBlocks);
                break;
            }
        }
    }

    /// Dispatches the current stored blocks to the plugins. Runs on the ApplicationStateDispatcher
    /// thread and reads the current value rather than taking one, so a notification can never
    /// carry an older snapshot than one already dispatched.
    private void syncStoredBlocks() {
        context.blockMessaging().sendStoredBlocksUpdate(new StoredBlocksNotification(currentStoredBlocks.get()));
    }

    /// Rebuilds the set of known publisher connections from the newest era in the supplied
    /// address-book history and merges it with the connections already tracked in
    /// [#knownPublishers].
    ///
    /// The "newest" era is chosen by streaming the [RangedNodeAddressBook] entries, sorting them
    /// with a [RangedAddressBookComparator], and taking the last (greatest) element. Its
    /// [NodeAddressBook] is transformed into publisher connections by
    /// [ApplicationStateUtility#publisherConnectionsFrom],
    /// which are then merged with the currently known publishers.
    ///
    /// @param history the address-book history to derive publisher connections from; may be null
    private void updateKnownPublishersFromAddressBook(final RangedAddressBookHistory history) {
        if (history == null
                || history.addressBooks() == null
                || history.addressBooks().isEmpty()) {
            return;
        }
        // Stream the eras, order them with the comparator, and keep the last (i.e. newest) one.
        // The last era's address book is non-null by the comparator's ordering contract.
        final NodeAddressBook latestBook = history.addressBooks().stream()
                .sorted(new RangedAddressBookComparator())
                .toList()
                .getLast()
                .addressBook();

        final List<NetworkConnection> connections = publisherConnectionsFrom(latestBook, appStateConfig);
        // Merge in the publishers already known, then publish the combined, de-duplicated set.
        connections.addAll(knownPublishers.get().activeEndpoints());
        final List<NetworkConnection> uniqueConnections = filterToUniqueConnections(connections);
        knownPublishers.set(new NetworkData(uniqueConnections));
    }

    /// Returns the {@link NodeAddressBook} whose block range covers {@code blockNum}, using the
    /// cached index built from the current {@link RangedAddressBookHistory}.
    ///
    /// @param blockNum the block number to look up
    /// @return the matching {@link NodeAddressBook}, or {@code null} if no era covers it
    @Override
    public NodeAddressBook getAddressBookForBlock(long blockNum) {
        return AddressBookHistoryLookup.findAddressBookForBlock(
                addressBookState.get().index(), blockNum);
    }

    /// Persist the TssData
    /// Persists the TssData to the file path specified in the ApplicationStateConfig class.
    ///
    /// @param tssData The TssData to persist
    private void persistTssData(TssData tssData) {
        final Path appStateDataFilePath = appStateConfig.tssBootstrapFilePath();
        try {
            replaceFile(appStateDataFilePath, TssData.JSON.toBytes(tssData));
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist TssData to %s: %s".formatted(appStateDataFilePath, e), e);
        }
    }

    private void persistNodeAddressBookHistory(RangedAddressBookHistory history) {
        final Path filePath = appStateConfig.rsaBootstrapFilePath();
        try {
            replaceFile(filePath, RangedAddressBookHistory.JSON.toBytes(history));
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist RSA address book history to %s".formatted(filePath), e);
        }
    }

    /// Persists both block range sets as JSON to the single file specified in the ApplicationStateConfig.
    private void persistBlockRanges() {
        final Path filePath = appStateConfig.blockRangesFilePath();
        try {
            replaceFile(filePath, BlockRangesState.JSON.toBytes(toBlockRangesState()));
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist block ranges to {0}: {1}", filePath, e.getMessage());
        }
    }

    /// Writes the bytes to a uniquely named sibling `.tmp` file, then moves it over the target. The
    /// target is never deleted first, so a crash part way through leaves the previous file intact
    /// rather than a missing or half written one. Each call gets its own tmp file, so two threads
    /// replacing the same target (possible during shutdown) cannot corrupt each other's write; the
    /// last move wins. Falls back to a non-atomic replace on filesystems that do not support atomic
    /// moves.
    ///
    /// @param target the file to replace
    /// @param bytes the new content
    /// @throws IOException if the write or the move fails
    private static void replaceFile(final Path target, final Bytes bytes) throws IOException {
        final Path tmp =
                Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
        try {
            Files.write(tmp, bytes.toByteArray());
            try {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                LOGGER.log(DEBUG, "Atomic move not supported for {0}; falling back to non-atomic replace", target);
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            // No-op after a successful move; otherwise stops a failed write leaving a new orphan each time.
            Files.deleteIfExists(tmp);
        }
    }

    private BlockRangesState toBlockRangesState() {
        final List<BlockRange> stored = storedBlocks
                .streamRanges()
                .map(r -> new BlockRange(r.start(), r.end()))
                .toList();
        final List<BlockRange> available = historicalBlockFacility()
                .availableBlocks()
                .streamRanges()
                .map(r -> new BlockRange(r.start(), r.end()))
                .toList();
        return new BlockRangesState(stored, available);
    }

    /// Loads all ApplicationState from file paths specified in the ApplicationStateConfig class.
    /// Must be called after the BlockNodeContext is available and the messaging facility is started.
    ///
    /// Note: This method currently uses _exceptions_ for flow control, with try/catch
    /// almost every sub block of code and calling methods that throw instead
    /// of returning errors. This needs to be fixed.
    private void loadApplicationState() {
        // Load TssData (JSON format). The dispatcher executor does not exist yet, so it is persisted and
        // dispatched inline.
        final Path tssDataJsonPath = appStateConfig.tssBootstrapFilePath();
        if (Files.exists(tssDataJsonPath)) {
            try {
                TssData tssData = standardParse(TssData.JSON, Bytes.wrap(Files.readAllBytes(tssDataJsonPath)));
                updateTssData(tssData);
            } catch (ParseException | IOException e) {
                // @todo(3321) this is using an exception to decide to log, but
                //      doesn't resolve the problem, so the code still fails
                //      later in the same method. This catch should be method level,
                //      or should be in the singular caller.
                LOGGER.log(WARNING, "Failed to read TssData file: " + tssDataJsonPath, e);
            }
        } else {
            // make sure the directory is created for the writes
            Path parent = tssDataJsonPath.getParent();
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                // @todo(3321) this is using an exception to decide to log, but
                //      doesn't resolve the problem, so the code still fails
                //      later in the same method. This catch should be method level,
                //      or should be in the singular caller.
                LOGGER.log(WARNING, "Failed to create TssData directory: " + parent, e);
            }
        }

        // Load RSA address book history (JSON format) — takes precedence over the single-book file.
        // If parsing fails check for single-book file format, wrap it into a single open-ended era
        // so the rest of the application sees a consistent RangedAddressBookHistory regardless of
        // which file is present (backward-compatibility bridge).
        final Path historyFilePath = appStateConfig.rsaBootstrapFilePath();
        if (Files.exists(historyFilePath)) {
            try {
                final RangedAddressBookHistory history =
                        standardParse(RangedAddressBookHistory.JSON, Bytes.wrap(Files.readAllBytes(historyFilePath)));
                if (history.addressBooks().isEmpty()) {
                    // Try the old bootstrap file format, standard parse swallows it and returns an empty history
                    final NodeAddressBook book =
                            standardParse(NodeAddressBook.JSON, Bytes.wrap(Files.readAllBytes(historyFilePath)));
                    if (validateAddressBook(book, historyFilePath.toString())) {
                        final RangedAddressBookHistory wrapped = RangedAddressBookHistory.newBuilder()
                                .addressBooks(List.of(RangedNodeAddressBook.newBuilder()
                                        .addressBook(book)
                                        .startBlock(0L)
                                        .endBlock(-1L)
                                        .build()))
                                .build();
                        updateAddressBookHistory(wrapped);
                    } else {
                        // @todo(3321) This is bad design. This entire method uses
                        //     exceptions as flow control, and we need to fix that.
                        throw new IllegalStateException("Address book is not valid");
                    }
                } else {
                    updateAddressBookHistory(history);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read RSA address book history file: " + historyFilePath, e);
            } catch (ParseException e) {
                // @todo(3321) This is bad design. This entire method uses
                //     exceptions as flow control, and we need to fix that.
                final String message =
                        "Corrupt RSA bootstrap file at %s — delete and restart to re-fetch from Mirror Node"
                                .formatted(historyFilePath);
                throw new IllegalStateException(message, e);
            }
        } else {
            // History file absent — ensure parent directory exists for future writes.
            final Path parent = historyFilePath.getParent();
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                // @todo(3321) this is using an exception to decide to log, but
                //      doesn't resolve the problem, so the code still fails
                //      later in the same method. This catch should be method level,
                //      or should be in the singular caller.
                LOGGER.log(WARNING, "Failed to create RSA address book history directory: " + parent, e);
            }
        }

        // Load block ranges (JSON format) — restored directly into the in-memory range sets.
        final Path blockRangesPath = appStateConfig.blockRangesFilePath();
        if (Files.exists(blockRangesPath)) {
            try {
                final BlockRangesState rangeSet =
                        standardParse(BlockRangesState.JSON, Bytes.wrap(Files.readAllBytes(blockRangesPath)));
                rangeSet.storedBlocks().forEach(r -> storedBlocks.add(new LongRange(r.rangeStart(), r.rangeEnd())));
                LOGGER.log(INFO, "Loaded block ranges from file: {0}", blockRangesPath);
            } catch (ParseException | IOException | IllegalArgumentException e) {
                // @todo(3321) this is using an exception to decide to log, but
                //      doesn't resolve the problem, so the code still fails
                //      later in the same method. This catch should be method level,
                //      or should be in the singular caller.
                LOGGER.log(WARNING, "Failed to read block ranges file: " + blockRangesPath, e);
            }
        } else {
            Path parent = blockRangesPath.getParent();
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                // @todo(3321) this is using an exception to decide to log, but
                //      doesn't resolve the problem, so the code still fails
                //      later in the same method. This catch should be method level,
                //      or should be in the singular caller.
                LOGGER.log(WARNING, "Failed to create block ranges directory: " + parent, e);
            }
        }

        // Load the connection-information sets (JSON-serialized NetworkData) used by the /statusz endpoints.
        // These are read-only configuration; absent or unreadable files yield an empty set.
        knownPublishers.set(loadNetworkData(appStateConfig.knownPublishersFilePath()));
        inboundPartners.set(loadNetworkData(appStateConfig.inboundPartnersFilePath()));
        outboundPartners.set(loadNetworkData(appStateConfig.outboundPartnersFilePath()));
    }
}
