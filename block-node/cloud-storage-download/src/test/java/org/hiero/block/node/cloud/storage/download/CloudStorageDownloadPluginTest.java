// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.hedera.bucky.S3Client;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
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
    @DisplayName("Default configuration fails init, naming every required setting")
    void defaultConfigurationFailsInit() {
        assertThatThrownBy(() -> start(new CloudStorageDownloadPlugin(), new SimpleInMemoryHistoricalBlockFacility()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cloud.storage.download.endpointUrl")
                .hasMessageContaining("cloud.storage.download.regionName")
                .hasMessageContaining("cloud.storage.download.bucketName")
                .hasMessageContaining("cloud.storage.download.accessKey")
                .hasMessageContaining("cloud.storage.download.secretKey");
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

    @Test
    @DisplayName("Without a test client, init creates the client from the settings without a request")
    void validConfigurationCreatesClient() {
        start(new CloudStorageDownloadPlugin(), new SimpleInMemoryHistoricalBlockFacility(), validConfig());
        assertThat(plugin.poller()).isNotNull();
        plugin.stop();
    }

    @ParameterizedTest
    @CsvSource({
        "endpointUrl, ''",
        "regionName, ''",
        "bucketName, ' '",
        "accessKey, ''",
        "secretKey, ''",
        "aheadThresholdBlocks, 0",
        "downloadWindow, 0"
    })
    @DisplayName("An invalid setting fails init, naming the setting but never a value, and never touches the bucket")
    void invalidSettingFailsInit(final String property, final String value) {
        final Map<String, String> config = validConfig();
        config.put("cloud.storage.download." + property, value);
        assertThatThrownBy(() -> start(
                        new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility(), config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cloud.storage.download." + property)
                .hasMessageNotContaining(ACCESS_KEY)
                .hasMessageNotContaining(SECRET_KEY);
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
