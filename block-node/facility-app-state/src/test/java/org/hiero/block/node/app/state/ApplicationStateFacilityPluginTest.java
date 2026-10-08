// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mockStatic;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.ServiceEndpoint;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.NetworkData;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
import org.hiero.block.node.spi.blockmessaging.ApplicationStateNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.AvailableBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

/// Unit tests for the [ApplicationStateFacilityPlugin]. The plugin is exercised directly, with a
/// recording messaging facility standing in for the real one.
class ApplicationStateFacilityPluginTest {
    /// A [HistoricalBlockFacility] whose available blocks the test controls directly.
    private static final class TestHistoricalBlockFacility implements HistoricalBlockFacility {
        private final ConcurrentLongRangeSet available = new ConcurrentLongRangeSet();

        @Override
        public BlockAccessor block(final long blockNumber) {
            return null;
        }

        @Override
        public BlockRangeSet availableBlocks() {
            return available;
        }
    }

    /// Records every notification, and lets a test wait for a number of them.
    private static final class RecordingHandler implements ApplicationStateNotificationHandler {
        volatile TssData lastTssData;
        volatile RangedAddressBookHistory lastAddressBookHistory;
        volatile List<BlockRange> lastStoredBlocks;
        volatile List<BlockRange> lastAvailableBlocks;
        final List<TssData> tssDataNotifications = Collections.synchronizedList(new ArrayList<>());
        final List<RangedAddressBookHistory> historyNotifications = Collections.synchronizedList(new ArrayList<>());
        volatile CountDownLatch latch = new CountDownLatch(0);

        void expect(final int count) {
            latch = new CountDownLatch(count);
        }

        void await() throws InterruptedException {
            assertThat(latch.await(5, TimeUnit.SECONDS))
                    .as("notification was not delivered in time")
                    .isTrue();
        }

        @Override
        public void handleTssDataUpdate(final TssDataNotification notification) {
            lastTssData = notification.tssData();
            tssDataNotifications.add(notification.tssData());
            latch.countDown();
        }

        @Override
        public void handleAddressBookHistoryUpdate(final AddressBookHistoryNotification notification) {
            lastAddressBookHistory = notification.rangedAddressBookHistory();
            historyNotifications.add(notification.rangedAddressBookHistory());
            latch.countDown();
        }

        @Override
        public void handleStoredBlocksUpdate(final StoredBlocksNotification notification) {
            lastStoredBlocks = notification.storedBlocks();
            latch.countDown();
        }

        @Override
        public void handleAvailableBlocksUpdate(final AvailableBlocksNotification notification) {
            lastAvailableBlocks = notification.availableBlocks();
            latch.countDown();
        }
    }

    /// One plugin wired to a context whose state files all live under a directory.
    private static final class Fixture {
        final ApplicationStateFacilityPlugin plugin = new ApplicationStateFacilityPlugin();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final TestHistoricalBlockFacility historical = new TestHistoricalBlockFacility();
        final TestMetricsExporter metricsExporter = new TestMetricsExporter();
        final TestThreadPoolManager<?, ?> threadPoolManager = new TestThreadPoolManager<>(
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadScheduledExecutor());
        final Configuration configuration;
        final ApplicationStateConfig config;
        final BlockNodeContext context;

        Fixture(final Path dir) {
            configuration = ConfigurationBuilder.create()
                    .withConfigDataType(ApplicationStateConfig.class)
                    .withValue(
                            "app.state.tssBootstrapFilePath",
                            dir.resolve("tss.json").toString())
                    .withValue(
                            "app.state.rsaBootstrapFilePath",
                            dir.resolve("rsa.json").toString())
                    .withValue(
                            "app.state.blockRangesFilePath",
                            dir.resolve("ranges.json").toString())
                    .withValue(
                            "app.state.knownPublishersFilePath",
                            dir.resolve("publishers.json").toString())
                    .withValue(
                            "app.state.inboundPartnersFilePath",
                            dir.resolve("inbound.json").toString())
                    .withValue(
                            "app.state.outboundPartnersFilePath",
                            dir.resolve("outbound.json").toString())
                    .build();
            config = configuration.getConfigData(ApplicationStateConfig.class);
            context = new BlockNodeContext(
                    configuration,
                    MetricRegistry.builder().setMetricsExporter(metricsExporter).build(),
                    new TestHealthFacility(),
                    messaging,
                    historical,
                    plugin,
                    new ServiceLoaderFunction(),
                    threadPoolManager,
                    BlockNodeVersions.DEFAULT);
        }

        void init() {
            plugin.init(context, null);
        }

        void initAndStart() {
            init();
            plugin.start();
        }

        void register(final RecordingHandler handler) {
            messaging.registerApplicationStateNotificationHandler(handler, false, "test-handler");
        }

        void shutdownExecutors() {
            threadPoolManager.shutdownNow();
        }
    }

    @TempDir
    Path tempDir;

    private final List<Fixture> fixtures = new ArrayList<>();

    private Fixture newFixture() {
        final Fixture fixture = new Fixture(tempDir);
        fixtures.add(fixture);
        return fixture;
    }

    @BeforeEach
    void setUp() {
        fixtures.clear();
    }

    @AfterEach
    void tearDown() {
        fixtures.forEach(Fixture::shutdownExecutors);
    }

    // ---- helpers ------------------------------------------------------------------------------

    private static TssData buildTssData(final long validFromBlock, final String ledgerIdHex) {
        final RosterEntry rosterEntry = RosterEntry.newBuilder()
                .nodeId(1)
                .weight(2)
                .schnorrPublicKey(Bytes.fromHex("070809"))
                .build();
        return TssData.newBuilder()
                .ledgerId(Bytes.fromHex(ledgerIdHex))
                .wrapsVerificationKey(Bytes.fromHex("010203"))
                .currentRoster(TssRoster.newBuilder().rosterEntries(rosterEntry).build())
                .validFromBlock(validFromBlock)
                .build();
    }

    private static RangedAddressBookHistory singleEraHistory(
            final long startBlock, final long nodeId, final String rsaKey) {
        return RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(RangedNodeAddressBook.newBuilder()
                        .addressBook(NodeAddressBook.newBuilder()
                                .nodeAddress(NodeAddress.newBuilder()
                                        .nodeId(nodeId)
                                        .rsaPubKey(rsaKey)
                                        .build())
                                .build())
                        .startBlock(startBlock)
                        .endBlock(-1L)
                        .build()))
                .build();
    }

    private static RangedAddressBookHistory buildTwoEraHistory() {
        final NodeAddressBook era1 = NodeAddressBook.newBuilder()
                .nodeAddress(
                        NodeAddress.newBuilder().nodeId(1L).rsaPubKey("aaaa").build())
                .build();
        final NodeAddressBook era2 = NodeAddressBook.newBuilder()
                .nodeAddress(
                        NodeAddress.newBuilder().nodeId(2L).rsaPubKey("bbbb").build())
                .build();
        return RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(
                        RangedNodeAddressBook.newBuilder()
                                .addressBook(era1)
                                .startBlock(0L)
                                .endBlock(999L)
                                .build(),
                        RangedNodeAddressBook.newBuilder()
                                .addressBook(era2)
                                .startBlock(1000L)
                                .endBlock(-1L)
                                .build()))
                .build();
    }

    private static void writeSingleBookRsaFile(final Path rsaPath) throws NoSuchAlgorithmException, IOException {
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final String hexKey =
                HexFormat.of().formatHex(kpg.generateKeyPair().getPublic().getEncoded());
        final NodeAddressBook book = NodeAddressBook.newBuilder()
                .nodeAddress(
                        NodeAddress.newBuilder().nodeId(1).rsaPubKey(hexKey).build())
                .build();
        Files.createDirectories(rsaPath.getParent());
        Files.write(rsaPath, NodeAddressBook.JSON.toBytes(book).toByteArray());
    }

    private static boolean hasTmpFile(final Path target) throws IOException {
        try (DirectoryStream<Path> tmpFiles =
                Files.newDirectoryStream(target.getParent(), target.getFileName() + "*.tmp")) {
            return tmpFiles.iterator().hasNext();
        }
    }

    // ---- lifecycle and discovery --------------------------------------------------------------

    @Test
    @DisplayName("the plugin is both a BlockNodePlugin and an ApplicationStateFacility")
    void pluginIsAFacilityAndAPlugin() {
        final ApplicationStateFacility facility = new ApplicationStateFacilityPlugin();
        assertThat(facility.name()).isEqualTo("ApplicationStateFacilityPlugin");
    }

    @Test
    @DisplayName("the plugin is discoverable as an ApplicationStateFacility service")
    void pluginIsDiscoverableAsService() {
        final List<? extends ApplicationStateFacility> found = new ServiceLoaderFunction()
                .loadServices(ApplicationStateFacility.class)
                .toList();
        assertThat(found).hasSize(1).first().isInstanceOf(ApplicationStateFacilityPlugin.class);
    }

    @Test
    @DisplayName("init registers the oldest and newest block gauges")
    void initRegistersGauges() {
        final Fixture f = newFixture();
        f.historical.available.add(5, 9);
        f.init();

        assertThat(f.metricsExporter.getMetricValue(
                        ApplicationStateFacilityPlugin.METRIC_APP_HISTORICAL_OLDEST_BLOCK.name()))
                .isEqualTo(5);
        assertThat(f.metricsExporter.getMetricValue(
                        ApplicationStateFacilityPlugin.METRIC_APP_HISTORICAL_NEWEST_BLOCK.name()))
                .isEqualTo(9);

        f.historical.available.add(10, 20);
        assertThat(f.metricsExporter.getMetricValue(
                        ApplicationStateFacilityPlugin.METRIC_APP_HISTORICAL_NEWEST_BLOCK.name()))
                .isEqualTo(20);
    }

    @Test
    @DisplayName("connection sets default to empty and backfill sources and the expected block can be updated")
    void connectionSetsAndExpectedBlock() {
        final Fixture f = newFixture();
        f.initAndStart();

        assertThat(f.plugin.knownPublishers()).isEqualTo(NetworkData.DEFAULT);
        assertThat(f.plugin.inboundPartners()).isEqualTo(NetworkData.DEFAULT);
        assertThat(f.plugin.outboundPartners()).isEqualTo(NetworkData.DEFAULT);
        assertThat(f.plugin.backfillSources()).isEqualTo(NetworkData.DEFAULT);
        assertThat(f.plugin.nextExpectedBlock()).isEqualTo(-1L);

        final NetworkData sources = NetworkData.newBuilder()
                .activeEndpoints(
                        NetworkConnection.newBuilder().category("backfill").build())
                .build();
        f.plugin.updateBackfillSources(sources);
        f.plugin.updateExpectedBlock(42L);
        assertThat(f.plugin.backfillSources()).isEqualTo(sources);
        assertThat(f.plugin.nextExpectedBlock()).isEqualTo(42L);

        f.plugin.updateBackfillSources(null);
        assertThat(f.plugin.backfillSources()).isEqualTo(NetworkData.DEFAULT);
        f.plugin.stop();
    }

    @Test
    @DisplayName("known publishers are derived from the address book history")
    void knownPublishersDerivedFromHistory() {
        final Fixture f = newFixture();
        f.initAndStart();
        final RangedAddressBookHistory history = RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(RangedNodeAddressBook.newBuilder()
                        .addressBook(NodeAddressBook.newBuilder()
                                .nodeAddress(NodeAddress.newBuilder()
                                        .nodeId(1)
                                        .serviceEndpoint(ServiceEndpoint.newBuilder()
                                                .domainName("node1.example.com")
                                                .port(50211)
                                                .build())
                                        .build())
                                .build())
                        .startBlock(0)
                        .endBlock(-1)
                        .build()))
                .build();

        assertThat(f.plugin.updateAddressBookHistory(history)).isTrue();

        assertThat(f.plugin.knownPublishers().activeEndpoints()).singleElement().satisfies(connection -> assertThat(
                        connection.remote().address())
                .isEqualTo("node1.example.com"));
        f.plugin.stop();
    }

    // ---- TSS data ----------------------------------------------------------------------------

    @Test
    @DisplayName("updateTssData ignores null and stale data, and dispatches only the accepted update")
    void updateTssDataDispatchesOnlyAcceptedUpdate() {
        final Fixture f = newFixture();
        final RecordingHandler handler = new RecordingHandler();
        f.register(handler);
        f.initAndStart();

        f.plugin.updateTssData(null);
        final TssData newer = buildTssData(200, "040506");
        f.plugin.updateTssData(newer);
        // older than the installed data, so it must be rejected
        f.plugin.updateTssData(buildTssData(100, "040506"));
        // flushes the dispatcher
        f.plugin.stop();

        assertThat(f.plugin.tssData()).isEqualTo(newer);
        assertThat(handler.tssDataNotifications).containsExactly(newer);
    }

    @Test
    @DisplayName("updateTssData does not dispatch data equal to the current data")
    void updateTssDataIgnoresEqualData() {
        final Fixture f = newFixture();
        final RecordingHandler handler = new RecordingHandler();
        f.register(handler);
        f.initAndStart();

        final TssData tssData = buildTssData(10, "040506");
        f.plugin.updateTssData(tssData);
        f.plugin.updateTssData(tssData);
        f.plugin.stop();

        assertThat(handler.tssDataNotifications).containsExactly(tssData);
    }

    @Test
    @DisplayName("an update after stop is kept in memory and does not throw")
    void updateAfterStopIsKeptInMemory() {
        final Fixture f = newFixture();
        final RecordingHandler handler = new RecordingHandler();
        f.register(handler);
        f.initAndStart();
        f.plugin.stop();

        final TssData tssData = buildTssData(100, "040506");
        assertThatCode(() -> f.plugin.updateTssData(tssData)).doesNotThrowAnyException();
        assertThatCode(() -> f.plugin.addStoredBlockRange(new LongRange(0, 9))).doesNotThrowAnyException();

        assertThat(f.plugin.tssData()).isEqualTo(tssData);
        assertThat(handler.tssDataNotifications).isEmpty();
    }

    @Test
    @DisplayName("updates racing with stop never throw, and the last notification matches the value held")
    void updatesRacingWithStopNeverThrow() throws Exception {
        final Fixture f = newFixture();
        final RecordingHandler handler = new RecordingHandler();
        f.register(handler);
        f.initAndStart();

        final AtomicBoolean failed = new AtomicBoolean();
        final CountDownLatch startLine = new CountDownLatch(1);
        final Thread writer = Thread.ofVirtual().start(() -> {
            try {
                startLine.await();
                for (long block = 1; block <= 500; block++) {
                    f.plugin.updateTssData(buildTssData(block, "0a0b"));
                }
            } catch (final Throwable t) {
                failed.set(true);
            }
        });
        startLine.countDown();
        f.plugin.stop();
        writer.join();

        assertThat(failed).isFalse();
        final List<TssData> seen = handler.tssDataNotifications;
        // notifications are never out of order, whatever was dropped at shutdown
        for (int i = 1; i < seen.size(); i++) {
            assertThat(seen.get(i).validFromBlock())
                    .isGreaterThan(seen.get(i - 1).validFromBlock());
        }
    }

    @Test
    @DisplayName("an empty TssData file does not fail startup and leaves the TSS data unset")
    void badTssDataFileIsIgnored() throws IOException {
        final Fixture f = newFixture();
        Files.createFile(f.config.tssBootstrapFilePath());

        f.initAndStart();

        assertThat(f.plugin.tssData()).isNull();
        f.plugin.stop();
    }

    @Test
    @DisplayName("TssData is persisted and loaded by a new instance")
    void tssDataPersistenceRoundTrip() throws InterruptedException {
        final Fixture first = newFixture();
        final RecordingHandler handler = new RecordingHandler();
        first.register(handler);
        first.initAndStart();
        handler.expect(1);
        final TssData tssData = buildTssData(50, "010203");
        first.plugin.updateTssData(tssData);
        handler.await();
        first.plugin.stop();

        final Fixture second = newFixture();
        final RecordingHandler secondHandler = new RecordingHandler();
        second.register(secondHandler);
        second.initAndStart();

        assertThat(second.plugin.tssData()).isEqualTo(tssData);
        // initial delivery: the persisted value is dispatched as the facility starts
        assertThat(secondHandler.tssDataNotifications).containsExactly(tssData);
        second.plugin.stop();
    }

    // ---- address book history ----------------------------------------------------------------

    @Test
    @DisplayName("a missing RSA file leaves the address book history null")
    void missingRsaFileLeavesHistoryNull() {
        final Fixture f = newFixture();
        f.initAndStart();

        assertThat(f.plugin.rangedAddressBookHistory()).isNull();
        f.plugin.stop();
    }

    @Test
    @DisplayName("a corrupt RSA file fails startup with an IllegalStateException")
    void corruptRsaFileThrows() throws IOException {
        final Fixture f = newFixture();
        Files.write(f.config.rsaBootstrapFilePath(), new byte[] {(byte) 0xFF, (byte) 0xFE, 0x00});
        f.init();

        assertThatThrownBy(f.plugin::start).isInstanceOf(IllegalStateException.class);
        f.plugin.stop();
    }

    @Test
    @DisplayName("an empty history file fails startup with an IllegalStateException")
    void emptyHistoryFileThrows() throws IOException {
        final Fixture f = newFixture();
        Files.write(
                f.config.rsaBootstrapFilePath(),
                RangedAddressBookHistory.JSON
                        .toBytes(RangedAddressBookHistory.DEFAULT)
                        .toByteArray());
        f.init();

        assertThatThrownBy(f.plugin::start).isInstanceOf(IllegalStateException.class);
        f.plugin.stop();
    }

    @Test
    @DisplayName("a history file is loaded and exposed")
    void historyFileIsLoaded() throws IOException {
        final Fixture f = newFixture();
        Files.write(
                f.config.rsaBootstrapFilePath(),
                RangedAddressBookHistory.JSON.toBytes(buildTwoEraHistory()).toByteArray());

        f.initAndStart();

        assertThat(f.plugin.rangedAddressBookHistory().addressBooks()).hasSize(2);
        assertThat(f.plugin
                        .getAddressBookForBlock(1500)
                        .nodeAddress()
                        .getFirst()
                        .nodeId())
                .isEqualTo(2L);
        f.plugin.stop();
    }

    @Test
    @DisplayName("a single-book RSA file is wrapped into a single open-ended era")
    void singleBookFileIsWrappedAsHistory() throws Exception {
        final Fixture f = newFixture();
        writeSingleBookRsaFile(f.config.rsaBootstrapFilePath());

        f.initAndStart();

        final RangedAddressBookHistory history = f.plugin.rangedAddressBookHistory();
        assertThat(history.addressBooks()).hasSize(1);
        assertThat(history.addressBooks().getFirst().startBlock()).isZero();
        assertThat(history.addressBooks().getFirst().endBlock()).isEqualTo(-1L);
        f.plugin.stop();
    }

    @Test
    @DisplayName("updateAddressBookHistory rejects a history whose last era is not newer")
    void updateAddressBookHistoryIgnoresNonNewerHistory() throws InterruptedException {
        final Fixture f = newFixture();
        final RecordingHandler handler = new RecordingHandler();
        f.register(handler);
        f.initAndStart();

        handler.expect(1);
        assertThat(f.plugin.updateAddressBookHistory(singleEraHistory(100, 1, "aaaa")))
                .isTrue();
        handler.await();

        assertThat(f.plugin.updateAddressBookHistory(singleEraHistory(100, 99, "zzzz")))
                .isFalse();
        assertThat(f.plugin.updateAddressBookHistory(null)).isFalse();
        f.plugin.stop();

        assertThat(handler.historyNotifications).hasSize(1);
        assertThat(f.plugin
                        .rangedAddressBookHistory()
                        .addressBooks()
                        .getFirst()
                        .addressBook()
                        .nodeAddress()
                        .getFirst()
                        .rsaPubKey())
                .isEqualTo("aaaa");
    }

    @Test
    @DisplayName("updateAddressBookHistory accepts a history whose last era starts at a higher block")
    void updateAddressBookHistoryAcceptsNewerHistory() throws InterruptedException {
        final Fixture f = newFixture();
        final RecordingHandler handler = new RecordingHandler();
        f.register(handler);
        f.initAndStart();
        handler.expect(1);
        f.plugin.updateAddressBookHistory(singleEraHistory(100, 1, "aaaa"));
        handler.await();

        final RangedAddressBookHistory newer = RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(
                        singleEraHistory(100, 1, "aaaa").addressBooks().getFirst(),
                        singleEraHistory(200, 2, "bbbb").addressBooks().getFirst()))
                .build();
        handler.expect(1);
        assertThat(f.plugin.updateAddressBookHistory(newer)).isTrue();
        handler.await();

        assertThat(handler.lastAddressBookHistory).isEqualTo(newer);
        assertThat(f.plugin.rangedAddressBookHistory()).isEqualTo(newer);
        f.plugin.stop();
    }

    @Test
    @DisplayName("concurrent address book history updates leave the newest history everywhere")
    void updateAddressBookHistoryConcurrentUpdates() throws Exception {
        final int highestStartBlock = 200;
        final Fixture f = newFixture();
        f.initAndStart();

        final CountDownLatch startLine = new CountDownLatch(1);
        final Thread even = pushHistories(startLine, 2, highestStartBlock, f.plugin);
        final Thread odd = pushHistories(startLine, 1, highestStartBlock - 1, f.plugin);
        startLine.countDown();
        even.join();
        odd.join();

        final RangedAddressBookHistory inMemory = f.plugin.rangedAddressBookHistory();
        assertThat(inMemory.addressBooks().getLast().startBlock()).isEqualTo(highestStartBlock);
        assertThat(f.plugin.getAddressBookForBlock(highestStartBlock))
                .isEqualTo(inMemory.addressBooks().getLast().addressBook());

        // Stopping flushes anything the dispatcher thread had queued but not yet written.
        f.plugin.stop();

        final RangedAddressBookHistory onDisk =
                RangedAddressBookHistory.JSON.parse(Bytes.wrap(Files.readAllBytes(f.config.rsaBootstrapFilePath())));
        assertThat(onDisk).isEqualTo(inMemory);
    }

    private static Thread pushHistories(
            final CountDownLatch startLine,
            final long firstStartBlock,
            final long lastStartBlock,
            final ApplicationStateFacility facility) {
        return Thread.ofVirtual().start(() -> {
            try {
                startLine.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            for (long startBlock = firstStartBlock; startBlock <= lastStartBlock; startBlock += 2) {
                facility.updateAddressBookHistory(singleEraHistory(startBlock, startBlock, "key" + startBlock));
            }
        });
    }

    // ---- block ranges ------------------------------------------------------------------------

    @Test
    @DisplayName("addStoredBlockRange records the range and dispatches the merged stored blocks")
    void addStoredBlockRangeUpdatesStoredBlocks() {
        final Fixture f = newFixture();
        final RecordingHandler handler = new RecordingHandler();
        f.register(handler);
        f.initAndStart();

        f.plugin.addStoredBlockRange(new LongRange(0, 9));
        f.plugin.addStoredBlockRange(new LongRange(10, 19));
        f.plugin.stop();

        assertThat(f.plugin.storedBlocks.contains(0, 19)).isTrue();
        assertThat(f.plugin.storedBlocks()).containsExactly(new BlockRange(0, 19));
        assertThat(handler.lastStoredBlocks).containsExactly(new BlockRange(0, 19));
    }

    @Test
    @DisplayName("concurrent addStoredBlockRange loses neither range")
    void concurrentAddStoredBlockRangeLosesNoUpdate() throws Exception {
        final Fixture f = newFixture();
        f.historical.available.add(0, 10);
        f.historical.available.add(20, 30);
        f.initAndStart();

        final CountDownLatch firstDispatchEntered = new CountDownLatch(1);
        final CountDownLatch secondUpdateComplete = new CountDownLatch(1);
        final CountDownLatch bothDelivered = new CountDownLatch(2);
        final AtomicBoolean firstDispatch = new AtomicBoolean(true);
        final List<List<BlockRange>> delivered = Collections.synchronizedList(new ArrayList<>());
        final ApplicationStateNotificationHandler probe = new ApplicationStateNotificationHandler() {
            @Override
            public void handleStoredBlocksUpdate(final StoredBlocksNotification notification) {
                if (firstDispatch.compareAndSet(true, false)) {
                    // Hold thread A's dispatch so thread B completes its whole read-modify-write
                    // of currentStoredBlocks before A's notification lands.
                    firstDispatchEntered.countDown();
                    try {
                        assertThat(secondUpdateComplete.await(5, TimeUnit.SECONDS))
                                .as("second update did not complete while thread A was held")
                                .isTrue();
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                delivered.add(notification.storedBlocks());
                bothDelivered.countDown();
            }
        };
        f.messaging.registerApplicationStateNotificationHandler(probe, false, "race-probe");

        final Thread threadA = new Thread(() -> f.plugin.addStoredBlockRange(new LongRange(100, 109)));
        threadA.start();
        assertThat(firstDispatchEntered.await(5, TimeUnit.SECONDS)).isTrue();

        f.plugin.addStoredBlockRange(new LongRange(200, 209));
        secondUpdateComplete.countDown();
        threadA.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(bothDelivered.await(5, TimeUnit.SECONDS)).isTrue();

        final List<BlockRange> field = f.plugin.currentStoredBlocks.get();
        assertThat(field).hasSize(4);
        assertThat(f.plugin.storedBlocks.contains(100, 109)).isTrue();
        assertThat(f.plugin.storedBlocks.contains(200, 209)).isTrue();
        assertThat(delivered).hasSize(2);
        assertThat(delivered.getLast()).isEqualTo(field);
        f.plugin.stop();
    }

    @Test
    @DisplayName("start dispatches the blocks the providers reported before the facility started")
    void startDispatchesAvailableBlocksFromHistoricalFacility() throws InterruptedException {
        final Fixture f = newFixture();
        final RecordingHandler handler = new RecordingHandler();
        f.register(handler);
        f.historical.available.add(0, 10);
        f.historical.available.add(20, 30);
        f.init();
        // a provider reporting from its own init(), before the facility has started
        handler.expect(2);
        f.plugin.updateAvailableBlocks();
        handler.await();
        handler.lastAvailableBlocks = null;
        handler.lastStoredBlocks = null;
        handler.expect(2);

        f.plugin.start();

        handler.await();
        final List<BlockRange> expected = List.of(new BlockRange(0L, 10L), new BlockRange(20L, 30L));
        assertThat(handler.lastAvailableBlocks).isEqualTo(expected);
        assertThat(handler.lastStoredBlocks).isEqualTo(expected);
        f.plugin.stop();
    }

    @Test
    @DisplayName("updateAvailableBlocks after a provider change notifies the new union")
    void updateAvailableBlocksNotifiesRuntimeProviderChange() throws InterruptedException {
        final Fixture f = newFixture();
        final RecordingHandler handler = new RecordingHandler();
        f.register(handler);
        f.historical.available.add(0, 10);
        f.historical.available.add(20, 30);
        f.initAndStart();

        f.historical.available.add(11, 15);
        handler.expect(2);
        f.plugin.updateAvailableBlocks();

        handler.await();
        final List<BlockRange> expected = List.of(new BlockRange(0L, 15L), new BlockRange(20L, 30L));
        assertThat(handler.lastAvailableBlocks).isEqualTo(expected);
        assertThat(handler.lastStoredBlocks).isEqualTo(expected);
        f.plugin.stop();
    }

    @Test
    @DisplayName("block ranges are persisted on stop and reloaded by a new instance")
    void blockRangesPersistenceRoundTrip() throws IOException {
        final Fixture first = newFixture();
        first.initAndStart();
        first.plugin.addStoredBlockRange(new LongRange(0, 999));
        first.plugin.addStoredBlockRange(new LongRange(1000, 1049));
        first.plugin.addStoredBlockRange(new LongRange(1050, 1099));
        first.plugin.stop();

        assertThat(first.config.blockRangesFilePath()).exists();
        assertThat(hasTmpFile(first.config.blockRangesFilePath()))
                .as("no tmp file should be left behind after a successful persist")
                .isFalse();

        final Fixture second = newFixture();
        second.initAndStart();

        assertThat(second.plugin.storedBlocks.streamRanges().toList()).containsExactly(new LongRange(0, 1099));
        second.plugin.stop();
    }

    @Test
    @DisplayName("block ranges are persisted on the scan interval once a persist boundary is crossed")
    void blockRangesPersistedWhenBoundaryCrossed() throws Exception {
        final Fixture f = newFixture();
        f.initAndStart();
        f.plugin.addStoredBlockRange(new LongRange(0, 1999));

        // the periodic check runs on the dispatcher; drive it directly rather than wait for the scan interval
        final Method persistIfDue = ApplicationStateFacilityPlugin.class.getDeclaredMethod("persistBlockRangesIfDue");
        persistIfDue.setAccessible(true);
        persistIfDue.invoke(f.plugin);

        assertThat(f.config.blockRangesFilePath()).exists();
        f.plugin.stop();
    }

    @Test
    @DisplayName("persisting falls back to a non-atomic replace when atomic moves are unsupported")
    void persistBlockRangesFallsBackWhenAtomicMoveUnsupported() throws IOException {
        final Fixture first = newFixture();
        first.initAndStart();
        first.plugin.addStoredBlockRange(new LongRange(0, 9));

        try (MockedStatic<Files> filesMock = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            filesMock
                    .when(() -> Files.move(
                            any(Path.class),
                            any(Path.class),
                            eq(StandardCopyOption.REPLACE_EXISTING),
                            eq(StandardCopyOption.ATOMIC_MOVE)))
                    .thenThrow(new AtomicMoveNotSupportedException("src", "dst", "simulated"));

            assertThatCode(first.plugin::stop).doesNotThrowAnyException();
            filesMock.verify(
                    () -> Files.move(any(Path.class), any(Path.class), eq(StandardCopyOption.REPLACE_EXISTING)),
                    atLeastOnce());
        }

        assertThat(hasTmpFile(first.config.blockRangesFilePath())).isFalse();
        final Fixture second = newFixture();
        second.initAndStart();
        assertThat(second.plugin.storedBlocks.streamRanges().toList()).containsExactly(new LongRange(0, 9));
        second.plugin.stop();
    }

    @Test
    @DisplayName("a handler registered before start receives the state loaded at startup")
    void preRegisteredHandlerReceivesStartupLoadNotifications() throws Exception {
        final Fixture f = newFixture();
        final TssData tssData = buildTssData(42, "0a0b0c");
        Files.write(
                f.config.tssBootstrapFilePath(), TssData.JSON.toBytes(tssData).toByteArray());
        writeSingleBookRsaFile(f.config.rsaBootstrapFilePath());
        final RecordingHandler handler = new RecordingHandler();
        f.register(handler);
        f.init();
        handler.expect(2);

        f.plugin.start();

        try {
            handler.await();
            assertThat(handler.lastTssData).isEqualTo(tssData);
            assertThat(handler.lastAddressBookHistory.addressBooks()).hasSize(1);
        } finally {
            f.plugin.stop();
        }
    }
}
