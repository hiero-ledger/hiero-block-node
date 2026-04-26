// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/**
 * Loads the consensus node RSA roster at Block Node startup and publishes it to all plugins via
 * `ApplicationStateFacility.updateAddressBook()`.
 *
 * ## Startup sequence
 *
 * 1. If `rsa-bootstrap-roster.pb` exists at the configured path: parse it and validate it has
 *    at least one entry with a non-blank `RSA_PubKey`.
 * 2. Otherwise: query `GET /api/v1/network/nodes` (paginated) on the configured Mirror Node,
 *    build a `NodeAddressBook`, and write it to the bootstrap file for future startups.
 * 3. If neither source succeeds: log `ERROR` and throw to trigger BN fail-fast shutdown.
 *
 * ## Thread safety
 *
 * `start()` runs on the plugin startup thread. After `updateAddressBook()` returns the roster
 * is available to all other plugins via `context.nodeAddressBook()`.
 *
 * See `docs/design/wrb-streaming/bootstrap-roster-plugin.md` for the full design.
 */
public class RsaRosterBootstrapPlugin implements BlockNodePlugin {

    private static final System.Logger LOGGER =
            System.getLogger(RsaRosterBootstrapPlugin.class.getName());

    /** `blocknode:roster_entries_loaded` — number of `NodeAddress` entries loaded at startup. */
    static final MetricKey<ObservableGauge> METRIC_ROSTER_ENTRIES_LOADED =
            MetricKey.of("roster_entries_loaded", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /** `blocknode:roster_load_duration_ms` — time to load the roster at startup in ms. */
    static final MetricKey<ObservableGauge> METRIC_ROSTER_LOAD_DURATION_MS =
            MetricKey.of("roster_load_duration_ms", ObservableGauge.class).addCategory(METRICS_CATEGORY);

    /** Max Mirror Node fetch retries before fail-fast. */
    private static final int MAX_RETRIES = 3;

    private BootstrapRosterConfig config;
    private ApplicationStateFacility applicationStateFacility;
    private MetricRegistry metricRegistry;

    // Metric values stored after startup so ObservableGauge can read them
    private volatile long rosterEntriesLoaded = 0L;
    private volatile long rosterLoadDurationMs = 0L;

    /** {@inheritDoc} */
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(BootstrapRosterConfig.class);
    }

    /** {@inheritDoc} */
    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        config = context.configuration().getConfigData(BootstrapRosterConfig.class);
        applicationStateFacility = context.applicationStateFacility();
        metricRegistry = context.metricRegistry();
        metricRegistry.register(ObservableGauge.builder(METRIC_ROSTER_ENTRIES_LOADED)
                .setDescription("Number of NodeAddress entries loaded at startup")
                .observe(() -> rosterEntriesLoaded));
        metricRegistry.register(ObservableGauge.builder(METRIC_ROSTER_LOAD_DURATION_MS)
                .setDescription("Time to load the RSA roster at startup in ms")
                .observe(() -> rosterLoadDurationMs));
    }

    /**
     * Loads the address book (file-first, Mirror Node fallback) and publishes it to all plugins
     * via `applicationStateFacility.updateAddressBook()`. Throws an unchecked exception if
     * neither source is available, triggering BN fail-fast.
     */
    @Override
    public void start() {
        final long startMs = System.currentTimeMillis();
        final NodeAddressBook book;
        final String source;

        if (Files.exists(config.filePath())) {
            book = loadFromFile(config.filePath());
            source = "file";
        } else {
            LOGGER.log(INFO, "RSA bootstrap file not found at {0}, querying Mirror Node", config.filePath());
            book = fetchFromMirrorNode();
            persistToFile(book, config.filePath());
            source = "mirror_node";
        }

        rosterEntriesLoaded = book.nodeAddress().size();
        rosterLoadDurationMs = System.currentTimeMillis() - startMs;

        LOGGER.log(INFO, "RSA roster loaded: {0} entries from {1} in {2}ms",
                rosterEntriesLoaded, source, rosterLoadDurationMs);

        applicationStateFacility.updateAddressBook(book);
    }

    // -------------------------------------------------------------------------
    // File loading
    // -------------------------------------------------------------------------

    /**
     * Parses `NodeAddressBook` from the binary protobuf file at `filePath`.
     *
     * @param filePath path to the binary protobuf bootstrap file
     * @return the parsed and validated `NodeAddressBook`
     * @throws IllegalStateException if the file cannot be parsed or contains no usable entries
     */
    private NodeAddressBook loadFromFile(final Path filePath) {
        LOGGER.log(INFO, "Loading RSA address book from {0}", filePath);
        try {
            final byte[] raw = Files.readAllBytes(filePath);
            final NodeAddressBook book = NodeAddressBook.PROTOBUF.parse(BufferedData.wrap(raw));
            validate(book, filePath.toString());
            return book;
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to read RSA bootstrap file at " + filePath + ": " + e.getMessage(), e);
        } catch (com.hedera.pbj.runtime.ParseException e) {
            throw new IllegalStateException(
                    "Corrupt RSA bootstrap file at " + filePath + " — delete and restart to re-fetch from Mirror Node",
                    e);
        }
    }

    /**
     * Validates that `book` has at least one entry with a non-blank `RSA_PubKey`.
     *
     * @param book the address book to validate
     * @param source human-readable source name for error messages
     * @throws IllegalStateException if the book is empty or has no usable entries
     */
    private void validate(final NodeAddressBook book, final String source) {
        if (book.nodeAddress().isEmpty()) {
            throw new IllegalStateException("RSA address book from " + source + " contains no entries — cannot verify WRB proofs");
        }
        final long usable = book.nodeAddress().stream()
                .filter(a -> !a.rsaPubKey().isBlank())
                .count();
        if (usable == 0) {
            throw new IllegalStateException(
                    "RSA address book from " + source + " has " + book.nodeAddress().size()
                            + " entries but none have a non-blank RSA_PubKey");
        }
    }

    // -------------------------------------------------------------------------
    // Mirror Node fetch
    // -------------------------------------------------------------------------

    /**
     * Fetches the node address book from the Mirror Node REST API with pagination and retries.
     *
     * @return a non-empty `NodeAddressBook`
     * @throws IllegalStateException if the Mirror Node is unreachable after retries or returns no usable entries
     */
    private NodeAddressBook fetchFromMirrorNode() {
        final HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(config.mirrorNodeConnectTimeoutSeconds()))
                .build();

        final List<NodeAddress> addresses = new ArrayList<>();
        String nextUrl = config.mirrorNodeBaseUrl() + "/api/v1/network/nodes?limit="
                + config.mirrorNodePageSize() + "&order=asc";

        while (nextUrl != null) {
            final String body = fetchWithRetry(client, nextUrl);
            final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.parse(body);
            for (final MirrorNodeNodesResponse.NodeEntry entry : response.nodes()) {
                if (entry.publicKey() == null || entry.publicKey().isBlank()) {
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
            nextUrl = response.nextLink();
        }

        if (addresses.isEmpty()) {
            throw new IllegalStateException(
                    "Mirror Node returned zero nodes with a non-blank public_key from " + config.mirrorNodeBaseUrl()
                            + ". Provide rsa-bootstrap-roster.pb or ensure Mirror Node is reachable.");
        }

        return NodeAddressBook.newBuilder().nodeAddress(addresses).build();
    }

    /**
     * Performs a single HTTP GET with exponential-backoff retries.
     *
     * @param client the HTTP client to use
     * @param url the URL to fetch
     * @return the response body as a string
     * @throws IllegalStateException if all retries are exhausted
     */
    private String fetchWithRetry(final HttpClient client, final String url) {
        long delayMs = 1_000L;
        Exception lastCause = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            if (attempt > 0) {
                LOGGER.log(WARNING, "Mirror Node fetch retry {0}/{1}: {2}", attempt, MAX_RETRIES - 1, url);
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for Mirror Node retry", ie);
                }
                delayMs *= 2;
            }
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
                return response.body();
            } catch (IOException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                lastCause = e;
            }
        }
        LOGGER.log(ERROR,
                "RSA address book could not be created — Mirror Node API unavailable after {0} attempts at {1}."
                        + " BN cannot verify WRB proofs. Provide rsa-bootstrap-roster.pb or ensure Mirror Node is reachable.",
                MAX_RETRIES, config.mirrorNodeBaseUrl());
        throw new IllegalStateException("Mirror Node unreachable after " + MAX_RETRIES + " attempts", lastCause);
    }

    // -------------------------------------------------------------------------
    // File persistence
    // -------------------------------------------------------------------------

    /**
     * Atomically writes the `NodeAddressBook` to `filePath` using a write-to-temp-then-rename
     * pattern. A write failure is logged but does not abort startup — the in-memory book is
     * already published to the application state.
     *
     * @param book the address book to persist
     * @param filePath the target file path
     */
    private void persistToFile(final NodeAddressBook book, final Path filePath) {
        try {
            Files.createDirectories(filePath.getParent());
            final Path tmp = filePath.resolveSibling(filePath.getFileName() + ".tmp");
            final com.hedera.pbj.runtime.io.buffer.Bytes encoded = NodeAddressBook.PROTOBUF.toBytes(book);
            Files.write(tmp, encoded.toByteArray());
            Files.move(tmp, filePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            // Restrict file permissions to owner read/write
            try {
                filePath.toFile().setReadable(false, false);
                filePath.toFile().setReadable(true, true);
                filePath.toFile().setWritable(false, false);
                filePath.toFile().setWritable(true, true);
            } catch (Exception e) {
                LOGGER.log(WARNING, "Could not set file permissions on {0}: {1}", filePath, e.getMessage());
            }
            LOGGER.log(INFO, "RSA bootstrap file written to {0}", filePath);
        } catch (IOException e) {
            LOGGER.log(WARNING, "Could not persist RSA bootstrap file to {0}: {1} — will re-fetch on next startup",
                    filePath, e.getMessage());
        }
    }
}
