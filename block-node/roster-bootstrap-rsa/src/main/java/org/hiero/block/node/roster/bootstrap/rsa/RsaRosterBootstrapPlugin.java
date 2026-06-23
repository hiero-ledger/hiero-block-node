// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockNodeSource;
import org.hiero.block.internal.MirrorNodeBlocksResponse;
import org.hiero.block.internal.MirrorNodeNodesResponse;
import org.hiero.block.internal.NodeEntry;
import org.hiero.block.internal.RangedAddressBookHistory;
import org.hiero.block.internal.RangedNodeAddressBook;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Loads the RSA address-book history at Block Node startup and publishes it to all plugins via
/// {@link ApplicationStateFacility#updateAddressBookHistory}.
///
/// File loading is handled by {@code BlockNodeApp}: it reads the history file (or the legacy
/// single-book file) in {@code loadApplicationState()} before plugins are initialised. This
/// plugin's sole responsibility is to check whether the history was already loaded and, if not,
/// to fetch it from a peer block node or the Mirror Node.
///
/// **Startup sequence:**
///
/// 1. If {@code context.nodeAddressBookHistory()} is non-null: history was pre-loaded — emit
///    metrics and return (no periodic refresh; history updates are an operator concern).
/// 2. If {@code context.nodeAddressBook()} is non-null: a single-book file was pre-loaded —
///    emit metrics and schedule periodic peer BN / Mirror Node refreshes.
/// 3. Otherwise, start concurrent peer BN and Mirror Node fallback queries. The Mirror Node
///    path builds a full {@link org.hiero.block.internal.RangedAddressBookHistory} by converting
///    each node entry's timestamp range to a block-number range via the blocks API.
/// 4. If neither source succeeds and {@code mirrorNodeBaseUrl} is blank: log WARNING and return.
///
/// See {@code docs/design/wrb-streaming/bootstrap-roster-plugin.md} for the full design.
public class RsaRosterBootstrapPlugin implements BlockNodePlugin {

    private static final System.Logger LOGGER = System.getLogger(RsaRosterBootstrapPlugin.class.getName());

    /// `blocknode:roster_entries_loaded` — number of `NodeAddress` entries loaded at startup.
    static final MetricKey<ObservableGauge> METRIC_ROSTER_ENTRIES_LOADED =
            MetricKey.of("roster_entries_loaded", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:roster_load_duration_ms` — time to load the roster at startup in ms.
    static final MetricKey<ObservableGauge> METRIC_ROSTER_LOAD_DURATION_MS =
            MetricKey.of("roster_load_duration_ms", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:rsa_roster_peer_requests` — number of peer gRPC requests made.
    static final MetricKey<LongCounter> METRIC_PEER_REQUESTS =
            MetricKey.of("rsa_roster_peer_requests", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:rsa_roster_peer_errors` — number of peer gRPC request errors.
    static final MetricKey<LongCounter> METRIC_PEER_ERRORS =
            MetricKey.of("rsa_roster_peer_errors", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:rsa_roster_addressbook_errors` — number of peers returning invalid/empty address books.
    static final MetricKey<LongCounter> METRIC_ADDRESSBOOK_ERRORS =
            MetricKey.of("rsa_roster_addressbook_errors", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:roster_eras_loaded` — number of distinct block-range eras in the loaded history.
    static final MetricKey<ObservableGauge> METRIC_ROSTER_ERAS_LOADED =
            MetricKey.of("roster_eras_loaded", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /// Holder for peer-query metrics, passed to `AddressBookFetcher`.
    public record MetricsHolder(
            LongCounter.Measurement peerRequests,
            LongCounter.Measurement peerErrors,
            LongCounter.Measurement addressBookErrors) {

        /// Factory that registers and returns a MetricsHolder.
        public static MetricsHolder create(@NonNull MetricRegistry metricRegistry) {
            return new MetricsHolder(
                    metricRegistry
                            .register(LongCounter.builder(METRIC_PEER_REQUESTS)
                                    .setDescription("Number of peer gRPC requests made for RSA address book"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_PEER_ERRORS)
                                    .setDescription("Number of peer gRPC request errors for RSA address book"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_ADDRESSBOOK_ERRORS)
                                    .setDescription("Number of invalid address books fetched"))
                            .getOrCreateNotLabeled());
        }
    }

    // ScheduledExecutors for peer-query and Mirror Node steps
    private ScheduledExecutorService queryBnExecutor;
    private volatile ScheduledFuture<?> bnScheduledFuture = null;
    private ScheduledExecutorService queryMnExecutor;
    private volatile ScheduledFuture<?> mnScheduledFuture = null;

    private volatile BlockNodeContext context;
    private RsaRosterBootstrapConfig config;
    private ApplicationStateFacility applicationStateFacility;
    private AddressBookFetcher addressBookFetcher;

    // Metric values stored after startup so ObservableGauge can read them
    private volatile long rosterEntriesLoaded = 0L;
    private volatile long rosterLoadDurationMs = 0L;
    private volatile long rosterErasLoaded = 0L;

    /// {@inheritDoc}
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(RsaRosterBootstrapConfig.class);
    }

    /// {@inheritDoc}
    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        this.context = context;
        this.config = context.configuration().getConfigData(RsaRosterBootstrapConfig.class);
        this.applicationStateFacility = context.applicationStateFacility();
        final MetricRegistry metricRegistry = context.metricRegistry();

        metricRegistry.register(ObservableGauge.builder(METRIC_ROSTER_ENTRIES_LOADED)
                .setDescription("Number of NodeAddress entries loaded at startup")
                .observe(() -> rosterEntriesLoaded));
        metricRegistry.register(ObservableGauge.builder(METRIC_ROSTER_LOAD_DURATION_MS)
                .setDescription("Time to load the RSA roster at startup in ms")
                .observe(() -> rosterLoadDurationMs));
        metricRegistry.register(ObservableGauge.builder(METRIC_ROSTER_ERAS_LOADED)
                .setDescription("Number of distinct block-range eras in the loaded address book history")
                .observe(() -> rosterErasLoaded));

        // Initialise the peer fetcher if a sources file is configured
        if (!config.blockNodeSourcesPath().isBlank()) {
            final Path sourcesPath = Path.of(config.blockNodeSourcesPath());
            if (Files.isRegularFile(sourcesPath)) {
                try {
                    final BlockNodeSource blockNodeSource =
                            standardParse(BlockNodeSource.JSON, Bytes.wrap(Files.readAllBytes(sourcesPath)));
                    addressBookFetcher =
                            new AddressBookFetcher(blockNodeSource, config, MetricsHolder.create(metricRegistry));
                } catch (ParseException | IOException e) {
                    LOGGER.log(
                            WARNING,
                            "Failed to parse block node sources from [{0}], peer query disabled: {1}",
                            config.blockNodeSourcesPath(),
                            e.getMessage());
                }
            } else {
                LOGGER.log(
                        WARNING,
                        "blockNodeSourcesPath [{0}] does not exist or is not a regular file, peer query disabled",
                        config.blockNodeSourcesPath());
            }
        }
    }

    /// {@inheritDoc}
    @Override
    public void onContextUpdate(final BlockNodeContext updatedContext) {
        this.context = updatedContext;
    }

    /// Implements the bootstrap strategy:
    /// 1. History file loaded (preferred) → record history metrics; no periodic refresh
    ///    (history updates are an operator / T3 concern).
    /// 2. Single address book loaded → record book metrics; schedule periodic peer BN and Mirror
    ///    Node refreshes as before.
    /// 3. Neither loaded → start concurrent peer BN and Mirror Node fallback queries.
    @Override
    public void start() {
        long startTimeMillis = System.currentTimeMillis();

        // Prefer the block-number-keyed history over the legacy single-book
        final RangedAddressBookHistory history = context.nodeAddressBookHistory();
        if (history != null) {
            recordHistoryMetrics(history, startTimeMillis);
            return;
        }

        final NodeAddressBook book = context.nodeAddressBook();
        if (book != null) {
            // Single-book file was pre-loaded by BlockNodeApp
            recordSuccessMetrics(book, startTimeMillis, "File");
            schedulePeriodicBlockNodeRefresh();
            schedulePeriodicMirrorNodeRefresh();
            return;
        }

        startBlockNodeFallback();
        startMirrorNodeFallback();
    }

    // -------------------------------------------------------------------------
    // Peer BN query
    // -------------------------------------------------------------------------

    private void startBlockNodeFallback() {
        if (addressBookFetcher == null) {
            LOGGER.log(
                    INFO,
                    "roster.bootstrap.rsa.blockNodeSourcesPath is blank or not valid."
                            + " set roster.bootstrap.rsa.blockNodeSourcesPath, to check for node address book updates.");
            return;
        }
        queryBnExecutor = context.threadPoolManager()
                .createVirtualThreadScheduledExecutor(1, "queryPeerScanner", this::uncaughtExceptionHandler);
        bnScheduledFuture = queryBnExecutor.scheduleAtFixedRate(
                this::fetchFromPeer, 0, config.bnInitialQueryIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    private void schedulePeriodicBlockNodeRefresh() {
        if (addressBookFetcher == null) return;
        if (queryBnExecutor == null) {
            queryBnExecutor = context.threadPoolManager()
                    .createVirtualThreadScheduledExecutor(1, "queryPeerScanner", this::uncaughtExceptionHandler);
        }
        queryBnExecutor.scheduleAtFixedRate(
                this::fetchFromPeer, 0, config.bnSubsequentQueryIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    private void fetchFromPeer() {
        final long startTime = System.currentTimeMillis();
        final RangedAddressBookHistory bookHistory =
                addressBookFetcher.getRangedNodeAddressBookHistory(context.nodeAddressBookHistory());
        if (bookHistory != null) {
            applicationStateFacility.updateAddressBookHistory(bookHistory);
            recordHistoryMetrics(bookHistory, startTime);
            if (bnScheduledFuture != null) {
                cancelScheduledFuture(bnScheduledFuture);
                bnScheduledFuture = null;
                schedulePeriodicBlockNodeRefresh();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Mirror Node fallback
    // -------------------------------------------------------------------------

    private void startMirrorNodeFallback() {
        if (config.mirrorNodeBaseUrl().isBlank()) {
            LOGGER.log(
                    INFO,
                    "roster.bootstrap.rsa.mirrorNodeBaseUrl is blank and no RSA bootstrap file or peer BN is"
                            + " configured. Provide rsa-bootstrap-roster.json, set"
                            + " roster.bootstrap.rsa.blockNodeSourcesPath, or set"
                            + " roster.bootstrap.rsa.mirrorNodeBaseUrl.");
            return;
        }
        queryMnExecutor = context.threadPoolManager()
                .createVirtualThreadScheduledExecutor(1, "queryMnScanner", this::uncaughtExceptionHandler);
        mnScheduledFuture = queryMnExecutor.scheduleAtFixedRate(
                this::fetchFromMirrorNode, 0, config.mnInitialQueryIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    private void schedulePeriodicMirrorNodeRefresh() {
        if (config.mirrorNodeBaseUrl().isBlank()) return;
        if (queryMnExecutor == null) {
            queryMnExecutor = context.threadPoolManager()
                    .createVirtualThreadScheduledExecutor(1, "queryMnScanner", this::uncaughtExceptionHandler);
        }
        queryMnExecutor.scheduleAtFixedRate(
                this::fetchFromMirrorNode,
                config.mnSubsequentQueryIntervalMillis(),
                config.mnSubsequentQueryIntervalMillis(),
                TimeUnit.MILLISECONDS);
    }

    /// Fetches all historical and current node entries from the Mirror Node REST API, converts
    /// each unique timestamp era to a block-number range, and builds a {@link RangedAddressBookHistory}.
    ///
    /// For each group of nodes sharing the same (from, to) timestamp pair:
    /// <ol>
    ///   <li>The Mirror Node blocks API ({@code /api/v1/blocks?timestamp=gte:...}) resolves
    ///       the first block in the era.</li>
    ///   <li>For closed eras ({@code to} is non-blank) a second call resolves the last block.</li>
    ///   <li>Open-ended eras ({@code to} is blank) use {@code endBlock = 0} as the sentinel.</li>
    /// </ol>
    private void fetchFromMirrorNode() {
        long startTimeMillis = System.currentTimeMillis();
        try (final HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(config.mirrorNodeConnectTimeoutSeconds()))
                .build()) {

            // Collect all node entries grouped by (from|to) era key, preserving insertion order
            final Map<String, List<NodeEntry>> eraGroups = new LinkedHashMap<>();
            String nextUrl = config.mirrorNodeBaseUrl() + "/api/v1/network/nodes?limit=" + config.mirrorNodePageSize()
                    + "&order=desc";

            while (nextUrl != null) {
                final MirrorNodeNodesResponse response = fetchAndParse(client, nextUrl);
                if (response == null) return;
                for (final NodeEntry entry : response.nodes()) {
                    if (entry.publicKey().isBlank()) {
                        LOGGER.log(WARNING, "Mirror Node: node {0} has no public_key — skipped", entry.nodeId());
                        continue;
                    }
                    final String from =
                            entry.timestamp() != null ? entry.timestamp().from() : "";
                    final String to =
                            entry.timestamp() != null ? entry.timestamp().to() : "";
                    eraGroups
                            .computeIfAbsent(from + "|" + to, k -> new ArrayList<>())
                            .add(entry);
                }
                final String rawNext =
                        response.links() == null ? null : response.links().next();
                nextUrl = (rawNext == null || rawNext.isBlank())
                        ? null
                        : rawNext.startsWith("http") ? rawNext : config.mirrorNodeBaseUrl() + rawNext;
            }

            if (eraGroups.isEmpty()) {
                LOGGER.log(
                        WARNING,
                        "Mirror Node returned zero nodes with a valid public_key from {0}.",
                        config.mirrorNodeBaseUrl());
                return;
            }

            // Convert each era's timestamp pair to a block range and build RangedNodeAddressBook entries
            final List<RangedNodeAddressBook> rangedBooks = new ArrayList<>();
            for (final Map.Entry<String, List<NodeEntry>> eraEntry : eraGroups.entrySet()) {
                final String eraKey = eraEntry.getKey();
                final int pipeIdx = eraKey.indexOf('|');
                final String from = eraKey.substring(0, pipeIdx);
                final String to = eraKey.substring(pipeIdx + 1);

                final long[] blockRange = fetchBlockRange(client, from, to);
                if (blockRange == null) {
                    LOGGER.log(WARNING, "Could not resolve block range for era from={0} to={1} — skipped", from, to);
                    continue;
                }

                final List<NodeAddress> addresses = eraEntry.getValue().stream()
                        .map(entry -> {
                            final String hexKey = entry.publicKey().startsWith("0x")
                                    ? entry.publicKey().substring(2)
                                    : entry.publicKey();
                            return NodeAddress.newBuilder()
                                    .nodeId(entry.nodeId())
                                    .rsaPubKey(hexKey)
                                    .build();
                        })
                        .toList();

                rangedBooks.add(RangedNodeAddressBook.newBuilder()
                        .addressBook(NodeAddressBook.newBuilder()
                                .nodeAddress(addresses)
                                .build())
                        .startBlock(blockRange[0])
                        .endBlock(blockRange[1])
                        .build());
            }

            if (rangedBooks.isEmpty()) {
                LOGGER.log(WARNING, "Could not build any address book eras from Mirror Node data.");
                return;
            }

            rangedBooks.sort(Comparator.comparingLong(RangedNodeAddressBook::startBlock));

            final RangedAddressBookHistory history = RangedAddressBookHistory.newBuilder()
                    .addressBooks(rangedBooks)
                    .build();

            recordHistoryMetrics(history, startTimeMillis);
            applicationStateFacility.updateAddressBookHistory(history);

            if (mnScheduledFuture != null) {
                cancelScheduledFuture(mnScheduledFuture);
                mnScheduledFuture = null;
                schedulePeriodicMirrorNodeRefresh();
            }
        }
    }

    /// Converts a (from, to) timestamp pair to a [startBlock, endBlock] pair by querying the
    /// Mirror Node blocks API. Returns {@code null} if the block range cannot be determined.
    ///
    /// Blank timestamps are treated as sentinels: blank {@code from} means genesis (startBlock=0)
    /// with no API call; blank {@code to} means open-ended (endBlock=0) with no API call.
    private long[] fetchBlockRange(final HttpClient client, final String from, final String to) {
        final long startBlock;
        if (from == null || from.isBlank()) {
            startBlock = 0L; // no from-timestamp → treat as genesis
        } else {
            final MirrorNodeBlocksResponse startResp = fetchAndParseBlocks(
                    client, config.mirrorNodeBaseUrl() + "/api/v1/blocks?timestamp=gte:" + from + "&order=asc&limit=1");
            if (startResp == null || startResp.blocks().isEmpty()) return null;
            startBlock = startResp.blocks().get(0).number();
        }

        if (to == null || to.isBlank()) {
            return new long[] {startBlock, 0L}; // open-ended
        }

        final MirrorNodeBlocksResponse endResp = fetchAndParseBlocks(
                client, config.mirrorNodeBaseUrl() + "/api/v1/blocks?timestamp=lte:" + to + "&order=desc&limit=1");
        if (endResp == null || endResp.blocks().isEmpty()) return null;
        return new long[] {startBlock, endResp.blocks().get(0).number()};
    }

    private MirrorNodeBlocksResponse fetchAndParseBlocks(final HttpClient client, final String url) {
        try {
            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(config.mirrorNodeReadTimeoutSeconds()))
                    .GET()
                    .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return MirrorNodeBlocksResponse.JSON.parse(Bytes.wrap(response.body()));
            }
            LOGGER.log(WARNING, "HTTP {0} from Mirror Node blocks API at {1}", response.statusCode(), url);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.log(WARNING, "Interrupted while fetching blocks from {0}", url);
        } catch (IOException | ParseException | RuntimeException e) {
            LOGGER.log(WARNING, "Failed to fetch blocks from {0}: {1}", url, e.getMessage());
        }
        return null;
    }

    private MirrorNodeNodesResponse fetchAndParse(final HttpClient client, final String url) {
        Exception lastCause;
        try {
            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(config.mirrorNodeReadTimeoutSeconds()))
                    .GET()
                    .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return standardParse(MirrorNodeNodesResponse.JSON, Bytes.wrap(response.body()));
            }
            lastCause = new IOException("HTTP " + response.statusCode() + " from " + url);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            lastCause = ie;
        } catch (IOException | RuntimeException | ParseException e) {
            lastCause = e;
        }
        LOGGER.log(
                ERROR,
                "RSA address book could not be created — Mirror Node API unavailable at {0}. "
                        + "Provide rsa-bootstrap-roster.json or ensure Mirror Node is reachable.",
                config.mirrorNodeBaseUrl(),
                lastCause);
        return null;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void recordSuccessMetrics(NodeAddressBook book, long startTimeMillis, String source) {
        rosterEntriesLoaded = book.nodeAddress().size();
        rosterLoadDurationMs = System.currentTimeMillis() - startTimeMillis;
        LOGGER.log(
                INFO,
                "RSA roster available: {0} entries obtained from {1} in {2}ms",
                rosterEntriesLoaded,
                source,
                rosterLoadDurationMs);
    }

    private void recordHistoryMetrics(RangedAddressBookHistory history, long startTimeMillis) {
        rosterErasLoaded = history.addressBooks().size();
        rosterEntriesLoaded = history.addressBooks().stream()
                .map(RangedNodeAddressBook::addressBook)
                .filter(book -> book != null)
                .mapToLong(book -> book.nodeAddress().size())
                .sum();
        rosterLoadDurationMs = System.currentTimeMillis() - startTimeMillis;
        LOGGER.log(
                INFO,
                "RSA address book history available: {0} eras, {1} total entries loaded in {2}ms",
                rosterErasLoaded,
                rosterEntriesLoaded,
                rosterLoadDurationMs);
    }

    private static void cancelScheduledFuture(ScheduledFuture<?> future) {
        if (future != null) future.cancel(false);
    }

    private void uncaughtExceptionHandler(Thread thread, Throwable throwable) {
        LOGGER.log(ERROR, "Uncaught exception in {0} thread", thread.getName(), throwable);
    }

    /// {@inheritDoc}
    @Override
    public void stop() {
        shutdownExecutor(queryBnExecutor, "queryPeerExecutor");
        shutdownExecutor(queryMnExecutor, "queryMnExecutor");
        if (addressBookFetcher != null) {
            addressBookFetcher.close();
        }
    }

    private void shutdownExecutor(ScheduledExecutorService executor, String name) {
        if (executor == null) return;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                LOGGER.log(INFO, "{0} did not terminate in time, calling shutdownNow()", name);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
