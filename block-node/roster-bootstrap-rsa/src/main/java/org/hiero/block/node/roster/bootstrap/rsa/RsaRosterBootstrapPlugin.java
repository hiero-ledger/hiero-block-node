// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Loads the consensus node RSA roster at Block Node startup and publishes it to all plugins via
/// `ApplicationStateFacility.updateAddressBook()`.
///
/// File loading and persistence are handled by `BlockNodeApp`: it reads the bootstrap file
/// in `loadApplicationState()` before plugins are initialised, and persists the file in
/// `updateAddressBook()` after a Mirror Node fetch. This plugin's sole responsibility is to
/// check whether the address book was already loaded (`context.nodeAddressBook() != null`) and,
/// if not, to fetch it from the Mirror Node.
///
/// **Startup sequence:**
///
/// 1. If `context.nodeAddressBook()` is non-null: BlockNodeApp loaded it from the
///    bootstrap file — emit metrics and return.
/// 2. Otherwise: query `GET /api/v1/network/nodes` (paginated) on the configured Mirror
///    Node, build a `NodeAddressBook`, and call
///    `applicationStateFacility.updateAddressBook()` — which persists the file and
///    notifies all plugins via `onContextUpdate`.
/// 3. If neither source succeeds: log `ERROR` and throw to trigger BN fail-fast.
///
/// **Async-delivery note:** When the Mirror Node fetch path is taken, `updateAddressBook()`
/// queues the book in `BlockNodeApp`'s pending state and returns before the context is
/// updated. The address book becomes available to other plugins only after the
/// `applicationStateExecutor` fires its next scan tick and calls `onContextUpdate`.
/// Any plugin that relies on `context.nodeAddressBook()` must implement `onContextUpdate`
/// and guard on `null` during its own `start()` execution.
///
/// See `docs/design/wrb-streaming/bootstrap-roster-plugin.md` for the full design.
public class RsaRosterBootstrapPlugin implements BlockNodePlugin {

    private static final System.Logger LOGGER = System.getLogger(RsaRosterBootstrapPlugin.class.getName());

    /// `blocknode:roster_entries_loaded` — number of `NodeAddress` entries loaded at startup.
    static final MetricKey<ObservableGauge> METRIC_ROSTER_ENTRIES_LOADED =
            MetricKey.of("roster_entries_loaded", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /// `blocknode:roster_load_duration_ms` — time to load the roster at startup in ms.
    static final MetricKey<ObservableGauge> METRIC_ROSTER_LOAD_DURATION_MS =
            MetricKey.of("roster_load_duration_ms", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /// The ScheduledExecutorService used by the RosterBootstrapPlugin to query peer Mirror Nodes for a Node
    /// Address Book
    private ScheduledExecutorService queryMnExecutor;
    private ScheduledFuture<?> scheduledFuture;

    private volatile BlockNodeContext context;
    private RsaRosterBootstrapConfig config;
    private ApplicationStateFacility applicationStateFacility;

    // Metric values stored after startup so ObservableGauge can read them
    private volatile long rosterEntriesLoaded = 0L;
    private volatile long rosterLoadDurationMs = 0L;

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
    }

    /// {@inheritDoc}
    @Override
    public void onContextUpdate(final BlockNodeContext updatedContext) {
        this.context = updatedContext;
    }

    /// Checks whether BlockNodeApp already loaded the address book from the bootstrap file.
    /// If so, records metrics and returns. Otherwise fetches from Mirror Node and calls
    /// `applicationStateFacility.updateAddressBook()` which persists the file and notifies
    /// all plugins. Throws if neither source succeeds, triggering BN fail-fast.
    @Override
    public void start() {
        final long startMs = System.currentTimeMillis();
        NodeAddressBook book = context.nodeAddressBook();
        String addressBookSource = "File";

        if (book == null) {
            // No bootstrap file was loaded by BlockNodeApp.loadApplicationState()
            if (config.mirrorNodeBaseUrl().isBlank()) {
                // @todo(#XXXX) replace this warning with proper plugin health reporting once
                //   the block node supports plugin-level healthy/unhealthy status indication.
                LOGGER.log(
                        WARNING,
                        "roster.bootstrap.rsa.mirrorNodeBaseUrl is blank and no RSA bootstrap file is present."
                                + " Provide rsa-bootstrap-roster.json or set roster.bootstrap.rsa.mirrorNodeBaseUrl.");
                return;
            }
            // Set up the asynchronous node address book fetcher
            // Create thread executors via threadPoolManager.
            queryMnExecutor = context.threadPoolManager()
                    .createVirtualThreadScheduledExecutor(1, "queryMnScanner", this::uncaughtExceptionHandler);

            // Schedule periodic checking of mirror node for address book data
            scheduledFuture = queryMnExecutor.scheduleAtFixedRate(
                    this::fetchFromMirrorNode, 0, config.mirrorNodeQueryInterval(), TimeUnit.MILLISECONDS);
        } else {
            rosterEntriesLoaded = book.nodeAddress().size();
            rosterLoadDurationMs = System.currentTimeMillis() - startMs;
            LOGGER.log(
                    INFO,
                    "RSA roster available: {0} entries obtained from {1} loaded in {2}ms",
                    rosterEntriesLoaded,
                    addressBookSource,
                    rosterLoadDurationMs);
        }
    }

    /// {@inheritDoc}
    @Override
    public void stop() {
        shutdownExecutor();
    }

    // -------------------------------------------------------------------------
    // Mirror Node fetch
    // -------------------------------------------------------------------------

    /// Fetches the node address book from the Mirror Node REST API with pagination and retries.
    ///
    /// @throws IllegalStateException if the Mirror Node is unreachable after retries or returns
    ///     no usable entries
    private void fetchFromMirrorNode() {
        try (final HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(config.mirrorNodeConnectTimeoutSeconds()))
                .build()) {

            final List<NodeAddress> addresses = new ArrayList<>();
            // Use order=desc so the most-recently-active entries (timestamp.to == null) arrive first.
            // We stop pagination as soon as we encounter a superseded entry (timestamp.to != null),
            // since all remaining entries in descending order are also historical.
            String nextUrl = config.mirrorNodeBaseUrl() + "/api/v1/network/nodes?limit=" + config.mirrorNodePageSize()
                    + "&order=desc";

            outer:
            while (nextUrl != null) {
                final MirrorNodeNodesResponse response = fetchAndParse(client, nextUrl);
                if (response == null) {
                    return;
                }
                for (final NodeEntry entry : response.nodes()) {
                    // A non-null timestamp.to means this entry has been superseded — all subsequent
                    // entries (in descending order) are also historical; stop here.
                    if (entry.timestamp() != null && !entry.timestamp().to().isBlank()) {
                        break outer;
                    }
                    if (entry.publicKey().isBlank()) {
                        LOGGER.log(WARNING, "Mirror Node: node {0} has no public_key — skipped", entry.nodeId());
                        continue;
                    }
                    // Strip optional 0x prefix
                    final String hexKey = entry.publicKey().startsWith("0x")
                            ? entry.publicKey().substring(2)
                            : entry.publicKey();
                    final NodeAddress addr = NodeAddress.newBuilder()
                            .nodeId(entry.nodeId())
                            .rsaPubKey(hexKey)
                            .build();
                    addresses.add(addr);
                }
                // Mirror Node may return a relative path; resolve it against the configured base URL.
                final String rawNext =
                        response.links() == null ? null : response.links().next();
                nextUrl = (rawNext == null || rawNext.isBlank())
                        ? null
                        : rawNext.startsWith("http") ? rawNext : config.mirrorNodeBaseUrl() + rawNext;
            }

            if (addresses.isEmpty()) {
                LOGGER.log(
                        WARNING,
                        "Mirror Node returned zero nodes with a non-blank public_key from {0}. Provide rsa-bootstrap-roster.json or ensure Mirror Node is reachable.",
                        config.mirrorNodeBaseUrl());
                return;
            }

            NodeAddressBook book =
                    NodeAddressBook.newBuilder().nodeAddress(addresses).build();

            // BlockNodeApp.updateAddressBook() persists the file and calls onContextUpdate.
            applicationStateFacility.updateAddressBook(book);

            // We found an address book stop the mirror node requests
            scheduledFuture.cancel(true);

            String addressBookSource = "Mirror Node";
            LOGGER.log(
                    INFO,
                    "RSA roster available: {0} entries obtained from {1}",
                    book.nodeAddress().size(),
                    addressBookSource,
                    rosterLoadDurationMs);
        }
    }

    /// Performs a single HTTP GET with exponential-backoff retries. The response body is
    /// parsed into a `MirrorNodeNodesResponse`
    ///
    /// @param client the HTTP client to use
    /// @param url the URL to fetch and parse
    /// @return the parsed Mirror Node response
    /// @throws IllegalStateException if all retries are exhausted
    private MirrorNodeNodesResponse fetchAndParse(final HttpClient client, final String url) {
        Exception lastCause;
        try {
            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(config.mirrorNodeReadTimeoutSeconds()))
                    .GET()
                    .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IOException("HTTP " + response.statusCode() + " from " + url);
            }
            return MirrorNodeNodesResponse.JSON.parse(Bytes.wrap(response.body()));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            lastCause = ie;
        } catch (IOException | RuntimeException | ParseException e) {
            lastCause = e;
        }
        LOGGER.log(
                ERROR,
                "RSA address book could not be created — Mirror Node API unavailable at {0}. BN cannot verify WRB proofs. Provide rsa-bootstrap-roster.json or ensure Mirror Node is reachable.",
                config.mirrorNodeBaseUrl(),
                lastCause);
        return null;
    }

    /// UncaughtExceptionHandler for logging uncaught exceptions
    private void uncaughtExceptionHandler(Thread thread, Throwable throwable) {
        LOGGER.log(WARNING, "Uncaught exception in RsaRosterBootstrapPlugin thread {0}", thread.getName(), throwable);
    }

    /// Shutdown the executor
    private void shutdownExecutor() {
        if (queryMnExecutor != null) {
            queryMnExecutor.shutdown();
            try {
                if (!queryMnExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    final String executorTerminationMsg =
                            "queryMNExecutor did not terminate in time, calling shutdownNow()";
                    queryMnExecutor.shutdownNow();
                    LOGGER.log(INFO, executorTerminationMsg);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
