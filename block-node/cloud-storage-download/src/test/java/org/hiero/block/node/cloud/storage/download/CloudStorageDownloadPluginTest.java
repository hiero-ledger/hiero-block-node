// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.hedera.bucky.S3Client;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.swirlds.config.api.validation.ConfigViolationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Logger;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.app.fixtures.logging.TestLogHandler;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.ApplicationStateNotificationHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CloudStorageDownloadPluginTest
        extends PluginTestBase<CloudStorageDownloadPlugin, ExecutorService, ScheduledThreadPoolExecutor> {

    private static final String ACCESS_KEY = "test-access-value";
    private static final String SECRET_KEY = "test-secret-value";

    private final S3Client s3Client = mock(S3Client.class);

    CloudStorageDownloadPluginTest() {
        super(Executors.newSingleThreadExecutor(), new ScheduledThreadPoolExecutor(1));
    }

    private static Map<String, String> validConfig() {
        final Map<String, String> config = new HashMap<>();
        config.put("cloud.storage.download.endpointUrl", "http://localhost:9000");
        config.put("cloud.storage.download.regionName", "us-east-1");
        config.put("cloud.storage.download.bucketName", "blocks");
        config.put("cloud.storage.download.accessKey", ACCESS_KEY);
        config.put("cloud.storage.download.secretKey", SECRET_KEY);
        config.put("cloud.storage.download.aheadThresholdBlocks", "10");
        // the scheduled run never comes during a test
        config.put("cloud.storage.download.pollIntervalMs", "3600000");
        return config;
    }

    private static TssData tssData(final long... nodeIds) {
        final List<RosterEntry> entries = new ArrayList<>();
        for (final long nodeId : nodeIds) {
            entries.add(RosterEntry.newBuilder().nodeId(nodeId).build());
        }
        return TssData.newBuilder()
                .currentRoster(TssRoster.newBuilder().rosterEntries(entries).build())
                .build();
    }

    @Test
    @DisplayName("Default configuration leaves the plugin idle, with a WARNING for every required setting")
    void defaultConfigurationLeavesPluginIdle() {
        final TestLogHandler logHandler = new TestLogHandler();
        final Logger pluginLogger = Logger.getLogger(CloudStorageDownloadPlugin.class.getName());
        pluginLogger.addHandler(logHandler);
        try {
            start(new CloudStorageDownloadPlugin(), new SimpleInMemoryHistoricalBlockFacility());
            assertThat(logHandler.countContaining("cloud.storage.download.endpointUrl is blank"))
                    .isOne();
            assertThat(logHandler.countContaining("cloud.storage.download.regionName is blank"))
                    .isOne();
            assertThat(logHandler.countContaining("cloud.storage.download.bucketName is blank"))
                    .isOne();
            assertThat(logHandler.countContaining("cloud.storage.download.accessKey is blank"))
                    .isOne();
            assertThat(logHandler.countContaining("cloud.storage.download.secretKey is blank"))
                    .isOne();
            assertThat(plugin.poller()).isNull();
            assertThat(testThreadPoolManager.scheduledExecutor().getQueue()).isEmpty();
            plugin.stop();
        } finally {
            pluginLogger.removeHandler(logHandler);
        }
    }

    @ParameterizedTest
    @CsvSource({"endpointUrl, ''", "regionName, ''", "bucketName, ' '", "accessKey, ''", "secretKey, ''"})
    @DisplayName("A blank bucket setting leaves the plugin idle, naming the setting but never a value")
    void blankSettingLeavesPluginIdle(final String property, final String value) {
        final Map<String, String> config = validConfig();
        config.put("cloud.storage.download." + property, value);
        final TestLogHandler logHandler = new TestLogHandler();
        final Logger pluginLogger = Logger.getLogger(CloudStorageDownloadPlugin.class.getName());
        pluginLogger.addHandler(logHandler);
        try {
            start(new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility(), config);
            assertThat(logHandler.countContaining("cloud.storage.download." + property + " is blank"))
                    .isOne();
            assertThat(logHandler.countContaining(ACCESS_KEY)).isZero();
            assertThat(logHandler.countContaining(SECRET_KEY)).isZero();
            assertThat(plugin.poller()).isNull();
            assertThat(testThreadPoolManager.scheduledExecutor().getQueue()).isEmpty();
            verifyNoInteractions(s3Client);
        } finally {
            pluginLogger.removeHandler(logHandler);
        }
    }

    @Test
    @DisplayName("Valid configuration registers the counters at zero and schedules the poll")
    void validConfigurationStarts() {
        start(new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility(), validConfig());
        assertThat(plugin.metricsHolder().blocksPulled().get()).isZero();
        assertThat(plugin.metricsHolder().blocksAlreadyStored().get()).isZero();
        assertThat(testThreadPoolManager.scheduledExecutor().getQueue()).hasSize(1);
        verifyNoInteractions(s3Client);
    }

    /// A handler's `isGating()` decides every registration it is used for. The block
    /// notification registration must keep back pressure, so application state updates, which
    /// are non-gating, must reach the plugin through a separate handler.
    @Test
    @DisplayName("Block notifications apply back pressure, application state is a separate handler")
    void blockNotificationRegistrationAppliesBackPressure() {
        start(new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility(), validConfig());
        assertThat(plugin.isGating()).isTrue();
        assertThat(plugin).isNotInstanceOf(ApplicationStateNotificationHandler.class);
        assertThat(blockMessaging.getApplicationStateNotificationHandlerCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Without a test client, init creates the client from the settings without a request")
    void validConfigurationCreatesClient() {
        start(new CloudStorageDownloadPlugin(), new SimpleInMemoryHistoricalBlockFacility(), validConfig());
        assertThat(plugin.poller()).isNotNull();
        plugin.stop();
    }

    @ParameterizedTest
    @CsvSource({"aheadThresholdBlocks, 0", "downloadWindow, 0"})
    @DisplayName(
            "A setting that is not positive fails the configuration, naming the setting, and never touches the bucket")
    void invalidSettingFailsConfiguration(final String property, final String value) {
        final Map<String, String> config = validConfig();
        config.put("cloud.storage.download." + property, value);
        final ConfigViolationException exception = catchThrowableOfType(
                ConfigViolationException.class,
                () -> start(
                        new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility(), config));
        assertThat(exception.getViolations()).hasSize(1);
        assertThat(exception.getViolations().getFirst().getPropertyName())
                .isEqualTo("cloud.storage.download." + property);
        verifyNoInteractions(s3Client);
    }

    @Test
    @DisplayName("A TSS data update that adds a node makes its folder eligible")
    void tssDataUpdateAddsNode() {
        start(new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility(), validConfig());
        assertThat(plugin.poller().nodeIds()).isEmpty();
        updateTssData(tssData(3L));
        assertThat(plugin.poller().nodeIds()).containsExactly(3L);
        updateTssData(tssData(3L, 4L));
        assertThat(plugin.poller().nodeIds()).containsExactly(3L, 4L);
    }

    /// An open-ended address book holding the given node IDs.
    private static NodeAddressBook addressBook(final long... nodeIds) {
        final List<NodeAddress> addresses = new ArrayList<>();
        for (final long nodeId : nodeIds) {
            addresses.add(NodeAddress.newBuilder().nodeId(nodeId).build());
        }
        return NodeAddressBook.newBuilder().nodeAddress(addresses).build();
    }

    @Test
    @DisplayName("Without TSS data the address book known at start makes its nodes' folders eligible")
    void addressBookAtStart() {
        updateAddressBook(addressBook(3L, 4L, 5L));
        start(new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility(), validConfig());
        assertThat(plugin.poller().nodeIds()).containsExactly(3L, 4L, 5L);
    }

    @Test
    @DisplayName("An address book update that adds a node makes its folder eligible; a later TSS roster wins")
    void addressBookUpdateAddsNodeUntilRoster() {
        start(new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility(), validConfig());
        updateAddressBook(addressBook(3L, 4L));
        assertThat(plugin.poller().nodeIds()).containsExactly(3L, 4L);
        updateAddressBook(addressBook(3L, 4L, 5L));
        assertThat(plugin.poller().nodeIds()).containsExactly(3L, 4L, 5L);
        updateTssData(tssData(7L));
        assertThat(plugin.poller().nodeIds()).containsExactly(7L);
    }

    @Test
    @DisplayName("A stored blocks update replaces the stored ranges")
    void storedBlocksUpdateReplacesRanges() {
        start(new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility(), validConfig());
        final List<BlockRange> ranges = List.of(
                BlockRange.newBuilder().rangeStart(0).rangeEnd(49).build(),
                BlockRange.newBuilder().rangeStart(60).rangeEnd(99).build());
        replaceStoredBlocks(ranges);
        assertThat(plugin.poller().storedBlocks()).isEqualTo(ranges);
    }
}
