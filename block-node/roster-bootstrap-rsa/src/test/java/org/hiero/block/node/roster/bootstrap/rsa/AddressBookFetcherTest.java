// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.util.List;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.internal.BlockNodeSource;
import org.hiero.block.internal.BlockNodeSourceConfig;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.base.client.BlockNodeClient;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Unit tests for {@link AddressBookFetcher}.
class AddressBookFetcherTest {

    private static RsaRosterBootstrapPlugin.MetricsHolder metricsHolder;
    private static TestMetricsExporter testMetricsExporter;

    @BeforeEach
    void initMetrics() {
        testMetricsExporter = new TestMetricsExporter();
        final MetricRegistry metricRegistry =
                MetricRegistry.builder().setMetricsExporter(testMetricsExporter).build();
        metricsHolder = RsaRosterBootstrapPlugin.MetricsHolder.create(metricRegistry);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static BlockNodeSourceConfig peer(String host, int port) {
        return BlockNodeSourceConfig.newBuilder()
                .address(host)
                .port(port)
                .priority(1)
                .build();
    }

    private static RsaRosterBootstrapConfig testConfig() {
        return new RsaRosterBootstrapConfig(
                "", // mirrorNodeBaseUrl
                5_000, // mnInitialQueryIntervalMillis
                60_000, // mnSubsequentQueryIntervalMillis
                5, // mirrorNodeConnectTimeoutSeconds
                10, // mirrorNodeReadTimeoutSeconds
                100, // mirrorNodePageSize
                "", // blockNodeSourcesPath
                5_000, // bnInitialQueryIntervalMillis
                60_000, // bnInitialQueryIntervalMillis
                3, // peerQueryMaxRetries
                false, // enableTLS
                60_000,
                104_857_600 // maxIncomingBufferSize
                );
    }

    private static NodeAddressBook bookWith(String... keys) {
        List<NodeAddress> addrs = new java.util.ArrayList<>();
        for (int i = 0; i < keys.length; i++) {
            addrs.add(NodeAddress.newBuilder().nodeId(i).rsaPubKey(keys[i]).build());
        }
        return NodeAddressBook.newBuilder().nodeAddress(addrs).build();
    }

    private AddressBookFetcher fetcherWithClient(BlockNodeSourceConfig peer, BlockNodeClient client) {
        final BlockNodeSource source =
                BlockNodeSource.newBuilder().nodes(List.of(peer)).build();
        return new AddressBookFetcher(source, testConfig(), metricsHolder) {
            @Override
            protected BlockNodeClient getNodeClient(BlockNodeSourceConfig ignored) {
                return client;
            }
        };
    }

    private BlockNodeClient mockClientReturning(NodeAddressBook book) {
        BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
        when(serviceClient.serverStatusDetail(any()))
                .thenReturn(ServerStatusDetailResponse.newBuilder()
                        .nodeAddressBook(book)
                        .build());
        BlockNodeClient client = mock(BlockNodeClient.class);
        when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);
        when(client.isNodeReachable()).thenReturn(true);
        return client;
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("getNodeAddressBook")
    class GetNodeAddressBookTests {

        @Test
        @DisplayName("Returns valid book from a reachable peer")
        void returnsValidBookFromPeer() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            NodeAddressBook expected = bookWith("aabbcc", "ddeeff");
            AddressBookFetcher fetcher = fetcherWithClient(peer, mockClientReturning(expected));

            NodeAddressBook result = fetcher.getNodeAddressBook();

            assertNotNull(result);
            assertEquals(2, result.nodeAddress().size());
            assertEquals("aabbcc", result.nodeAddress().getFirst().rsaPubKey());
        }

        @Test
        @DisplayName("Returns null when peer returns null book")
        void returnsNullWhenPeerReturnsNullBook() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            AddressBookFetcher fetcher = fetcherWithClient(peer, mockClientReturning(null));

            assertNull(fetcher.getNodeAddressBook());
        }

        @Test
        @DisplayName("Returns null when peer returns empty book")
        void returnsNullWhenBookIsEmpty() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            NodeAddressBook empty =
                    NodeAddressBook.newBuilder().nodeAddress(List.of()).build();
            AddressBookFetcher fetcher = fetcherWithClient(peer, mockClientReturning(empty));

            assertNull(fetcher.getNodeAddressBook());
        }

        @Test
        @DisplayName("Returns null when all keys are blank")
        void returnsNullWhenAllKeysBlank() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            NodeAddressBook blankKeys = bookWith("", "  ");
            AddressBookFetcher fetcher = fetcherWithClient(peer, mockClientReturning(blankKeys));

            assertNull(fetcher.getNodeAddressBook());
        }

        @Test
        @DisplayName("Increments peerRequests metric on success")
        void incrementsPeerRequestsOnSuccess() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            AddressBookFetcher fetcher = fetcherWithClient(peer, mockClientReturning(bookWith("aabbcc")));

            fetcher.getNodeAddressBook();

            assertEquals(1, testMetricsExporter.getMetricValue(RsaRosterBootstrapPlugin.METRIC_PEER_REQUESTS.name()));
        }

        @Test
        @DisplayName("Increments peerErrors and removes client on exception")
        void incrementsPeerErrorsOnException() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.serverStatusDetail(any())).thenThrow(new RuntimeException("connection refused"));
            BlockNodeClient client = mock(BlockNodeClient.class);
            when(client.isNodeReachable()).thenReturn(true);
            when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);

            AddressBookFetcher fetcher =
                    new AddressBookFetcher(
                            BlockNodeSource.newBuilder().nodes(List.of(peer)).build(), testConfig(), metricsHolder) {
                        @Override
                        protected BlockNodeClient getNodeClient(BlockNodeSourceConfig ignored) {
                            nodeClientMap.put(ignored, client);
                            return client;
                        }
                    };

            assertNull(fetcher.getNodeAddressBook());
            assertEquals(1, testMetricsExporter.getMetricValue(RsaRosterBootstrapPlugin.METRIC_PEER_ERRORS.name()));
            // Client must be evicted so a fresh one is created on next call
            assertTrue(fetcher.nodeClientMap.isEmpty());
        }

        @Test
        @DisplayName("Falls over to second peer when first fails")
        void failoverToSecondPeer() {
            BlockNodeSourceConfig failingPeer = peer("failing", 8080);
            BlockNodeSourceConfig goodPeer = peer("good", 8081);
            NodeAddressBook expected = bookWith("aabbcc");

            BlockNodeServiceInterface.BlockNodeServiceClient failingService =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(failingService.serverStatusDetail(any())).thenThrow(new RuntimeException("timeout"));
            BlockNodeClient failingClient = mock(BlockNodeClient.class);
            when(failingClient.isNodeReachable()).thenReturn(true);
            when(failingClient.getBlockNodeServiceClient()).thenReturn(failingService);

            BlockNodeClient goodClient = mockClientReturning(expected);

            BlockNodeSource source = BlockNodeSource.newBuilder()
                    .nodes(List.of(failingPeer, goodPeer))
                    .build();

            AddressBookFetcher fetcher = new AddressBookFetcher(source, testConfig(), metricsHolder) {
                @Override
                protected BlockNodeClient getNodeClient(BlockNodeSourceConfig node) {
                    return node.address().equals("failing") ? failingClient : goodClient;
                }
            };

            NodeAddressBook result = fetcher.getNodeAddressBook();
            assertNotNull(result);
            assertEquals("aabbcc", result.nodeAddress().getFirst().rsaPubKey());
        }
    }

    @Nested
    @DisplayName("isValid")
    class IsValidTests {

        @Test
        @DisplayName("null book is invalid")
        void nullIsInvalid() {
            assertTrue(!AddressBookFetcher.isValid(null));
        }

        @Test
        @DisplayName("empty book is invalid")
        void emptyIsInvalid() {
            assertTrue(!AddressBookFetcher.isValid(
                    NodeAddressBook.newBuilder().nodeAddress(List.of()).build()));
        }

        @Test
        @DisplayName("book with one non-blank key is valid")
        void oneNonBlankKeyIsValid() {
            assertTrue(AddressBookFetcher.isValid(bookWith("abc")));
        }

        @Test
        @DisplayName("book with only blank keys is invalid")
        void onlyBlankKeysIsInvalid() {
            assertTrue(!AddressBookFetcher.isValid(bookWith("", "")));
        }
    }
}
