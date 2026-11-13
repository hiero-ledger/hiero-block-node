// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static org.assertj.core.api.Assertions.assertThat;
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

    private final S3Client s3Client = mock(S3Client.class);

    CloudStorageDownloadPluginTest() {
        super(Executors.newSingleThreadExecutor(), new ScheduledThreadPoolExecutor(1));
    }

    private static Map<String, String> validConfig() {
        final Map<String, String> config = new HashMap<>();
        config.put("cloud.storage.download.enabled", "true");
        config.put("cloud.storage.download.endpointUrl", "http://localhost:9000");
        config.put("cloud.storage.download.regionName", "us-east-1");
        config.put("cloud.storage.download.bucketName", "blocks");
        config.put("cloud.storage.download.accessKey", "access");
        config.put("cloud.storage.download.secretKey", "secret");
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
    @DisplayName("Default configuration leaves the plugin inactive, schedules nothing and never touches the bucket")
    void defaultConfigurationIsInactive() {
        start(new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility());
        assertThat(plugin.isActive()).isFalse();
        assertThat(plugin.metricsHolder()).isNull();
        assertThat(testThreadPoolManager.scheduledExecutor().getQueue()).isEmpty();
        verifyNoInteractions(s3Client);
    }

    @Test
    @DisplayName("Valid configuration makes the plugin active, registers the counters and schedules the poll")
    void validConfigurationIsActive() {
        start(new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility(), validConfig());
        assertThat(plugin.isActive()).isTrue();
        assertThat(plugin.metricsHolder().blocksPulled().get()).isZero();
        assertThat(plugin.metricsHolder().blocksAlreadyStored().get()).isZero();
        assertThat(testThreadPoolManager.scheduledExecutor().getQueue()).hasSize(1);
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
    @DisplayName("An invalid setting leaves the enabled plugin inactive and never touches the bucket")
    void invalidSettingIsInactive(final String property, final String value) {
        final Map<String, String> config = validConfig();
        config.put("cloud.storage.download." + property, value);
        start(new CloudStorageDownloadPlugin(s3Client), new SimpleInMemoryHistoricalBlockFacility(), config);
        assertThat(plugin.isActive()).isFalse();
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
