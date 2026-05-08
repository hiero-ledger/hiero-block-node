// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.roster.bootstrap.tss.client.BlockNodeClient;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Unit tests for [TssDataFetcher].
@Timeout(value = 5, unit = TimeUnit.SECONDS)
class TssDataFetcherTest {

    private static RosterBootstrapTssPlugin.MetricsHolder metricsHolder;
    private static TestMetricsExporter testMetricsExporter;

    private static BlockNodeSourceConfig node(String host, int port, int priority) {
        return BlockNodeSourceConfig.newBuilder()
                .address(host)
                .port(port)
                .priority(priority)
                .build();
    }

    private static TssDataFetcher newClient(BlockNodeSourceConfig... nodes) {
        final BlockNodeSource source =
                BlockNodeSource.newBuilder().nodes(List.of(nodes)).build();
        return new TssDataFetcher(source, createTestConfig(), createTestMetricsHolder());
    }

    private static RosterBootstrapTssConfig createTestConfig() {
        return new RosterBootstrapTssConfig(
                "", // blockNodeSourcesPath
                500, // queryPeerInterval
                1_000, // queryPeerInitialDelay
                104_857_600, // maxIncomingBufferSize (100 MB default)
                false // enableTLS
                );
    }

    private static RosterBootstrapTssPlugin.MetricsHolder createTestMetricsHolder() {
        if (metricsHolder == null) {
            testMetricsExporter = new TestMetricsExporter();
            final MetricRegistry metricRegistry = MetricRegistry.builder()
                    .setMetricsExporter(testMetricsExporter)
                    .build();
            metricsHolder = RosterBootstrapTssPlugin.MetricsHolder.createMetrics(metricRegistry, new AtomicLong());
        }
        return metricsHolder;
    }

    @Nested
    @DisplayName("fetchTssDataFromNode")
    class FetchTssDataFromNodeTests {

        @Test
        @DisplayName("returns empty for empty availability")
        void returnsEmptyForEmptyAvailability() {
            final BlockNodeSourceConfig nodeConfig = node("localhost", 1, 1);
            final TssDataFetcher client = newClient(nodeConfig);

            List<TssData> tssDataList = client.getTssData();
            assertTrue(tssDataList.isEmpty());
        }

        @Test
        @Timeout(value = 10, unit = TimeUnit.SECONDS)
        @DisplayName("returns tss data on success")
        void returnsTssDataOnSuccess() {
            final BlockNodeSourceConfig nodeConfig = node("localhost", 1, 1);
            final RosterBootstrapTssPlugin.MetricsHolder metrics = createTestMetricsHolder();

            BlockNodeClient successClient = mockClientReturning();
            TssDataFetcher fetcher = createFetcherWithClient(nodeConfig, metrics, successClient);
            assertEquals(1, fetcher.getTssData().size());
            assertEquals(1, getMetricValue(RosterBootstrapTssPlugin.METRIC_TSS_DATA_REQUESTS));
        }

        private BlockNodeClient mockClientReturning() {
            BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);

            when(serviceClient.serverStatusDetail(any()))
                    .thenReturn(ServerStatusDetailResponse.newBuilder()
                            .tssData(buildTssData(
                                    Bytes.fromHex("010203"),
                                    Bytes.fromHex("040506"),
                                    1,
                                    2,
                                    Bytes.fromHex("070809"),
                                    50,
                                    100))
                            .build());

            BlockNodeClient blockNodeClient = mock(BlockNodeClient.class);
            when(blockNodeClient.getBlockNodeServiceClient()).thenReturn(serviceClient);
            when(blockNodeClient.isNodeReachable()).thenReturn(true);

            return blockNodeClient;
        }
    }

    // Helper methods
    private static BlockNodeSource createSource(BlockNodeSourceConfig... nodes) {
        return BlockNodeSource.newBuilder().nodes(List.of(nodes)).build();
    }

    private static TssDataFetcher createFetcherWithClient(
            BlockNodeSourceConfig nodeConfig, RosterBootstrapTssPlugin.MetricsHolder metrics, BlockNodeClient client) {
        final BlockNodeSource source = createSource(nodeConfig);
        final RosterBootstrapTssConfig config = createTestConfig();
        return new TssDataFetcher(source, config, metrics) {
            @Override
            protected BlockNodeClient getNodeClient(BlockNodeSourceConfig ignored) {
                return client;
            }
        };
    }

    private static long getMetricValue(MetricKey<?> metricKey) {
        return testMetricsExporter.getMetricValue(metricKey.name());
    }

    /// build a `TssData` object from individual fields from the `TssBootstrapConfig`
    ///
    /// @param ledgerId The ledgerId Bytes
    /// @param wrapsVerificationKey The wrapsVerificationKey Bytes
    /// @param nodeId The node id
    /// @param weight The weight
    /// @param schnorrPublicKey The schnorrPublicKey Bytes
    /// @param validFromBlock The block from which this TssData is valid
    /// @param rosterValidFromBlock The block from which this TssRoster is valid
    /// @return a `TssData` object
    static TssData buildTssData(
            Bytes ledgerId,
            Bytes wrapsVerificationKey,
            long nodeId,
            long weight,
            Bytes schnorrPublicKey,
            long validFromBlock,
            long rosterValidFromBlock) {
        RosterEntry rosterEntry = RosterEntry.newBuilder()
                .nodeId(nodeId)
                .weight(weight)
                .schnorrPublicKey(schnorrPublicKey)
                .build();
        TssRoster tssRoster = TssRoster.newBuilder()
                .rosterEntries(rosterEntry)
                .validFromBlock(rosterValidFromBlock)
                .build();
        return TssData.newBuilder()
                .ledgerId(ledgerId)
                .wrapsVerificationKey(wrapsVerificationKey)
                .currentRoster(tssRoster)
                .validFromBlock(validFromBlock)
                .build();
    }
}
