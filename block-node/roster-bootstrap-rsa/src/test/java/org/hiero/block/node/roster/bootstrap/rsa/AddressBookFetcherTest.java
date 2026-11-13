// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import java.util.List;
import org.hiero.block.api.AddressBookResponse;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.api.ServerStatusDetailRequest;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.internal.BlockNodeSource;
import org.hiero.block.internal.BlockNodeSourceConfig;
import org.hiero.block.node.base.client.BlockNodeClient;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/// Unit tests for {@link AddressBookFetcher}.
class AddressBookFetcherTest {

    private static RsaRosterBootstrapPlugin.MetricsHolder metricsHolder;

    @BeforeEach
    void initMetrics() {
        final MetricRegistry metricRegistry = MetricRegistry.builder().build();
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
                60_000, // bnSubsequentQueryIntervalMillis
                false, // enableTLS
                60_000,
                104_857_600 // maxIncomingBufferSize
                );
    }

    /** Builds a single open-ended era containing nodes with the given RSA keys. */
    private static RangedAddressBookHistory historyWith(String... keys) {
        List<NodeAddress> addrs = new java.util.ArrayList<>();
        for (int i = 0; i < keys.length; i++) {
            addrs.add(NodeAddress.newBuilder().nodeId(i).rsaPubKey(keys[i]).build());
        }
        final NodeAddressBook book =
                NodeAddressBook.newBuilder().nodeAddress(addrs).build();
        return RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(RangedNodeAddressBook.newBuilder()
                        .addressBook(book)
                        .startBlock(0L)
                        .endBlock(-1L)
                        .build()))
                .build();
    }

    private AddressBookFetcher fetcherWithClient(BlockNodeSourceConfig peer, BlockNodeClient client) {
        final BlockNodeSource source =
                BlockNodeSource.newBuilder().nodes(List.of(peer)).build();
        return new AddressBookFetcher(source, testConfig(), metricsHolder) {
            @Override
            BlockNodeClient getNodeClient(BlockNodeSourceConfig ignored) {
                return client;
            }
        };
    }

    private BlockNodeClient mockClientReturning(RangedAddressBookHistory history) {
        BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
        when(serviceClient.getAddressBook(any()))
                .thenReturn(AddressBookResponse.newBuilder()
                        .rangedAddressBookHistory(history)
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
    @DisplayName("getRangedNodeAddressBookHistory")
    class GetRangedNodeAddressBookHistoryTests {

        @Test
        @DisplayName("Returns valid history from a reachable peer")
        void returnsValidHistoryFromPeer() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            RangedAddressBookHistory expected = historyWith("aabbcc", "ddeeff");
            AddressBookFetcher fetcher = fetcherWithClient(peer, mockClientReturning(expected));

            RangedAddressBookHistory result = fetcher.getRangedNodeAddressBookHistory();

            assertNotNull(result);
            assertEquals(1, result.addressBooks().size());
            assertEquals(
                    2, result.addressBooks().get(0).addressBook().nodeAddress().size());
            assertEquals(
                    "aabbcc",
                    result.addressBooks()
                            .get(0)
                            .addressBook()
                            .nodeAddress()
                            .getFirst()
                            .rsaPubKey());
        }

        @Test
        @DisplayName("Returns null when peer returns null history")
        void returnsNullWhenPeerReturnsNull() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            AddressBookFetcher fetcher = fetcherWithClient(peer, mockClientReturning(null));

            assertNull(fetcher.getRangedNodeAddressBookHistory());
        }

        @Test
        @DisplayName("Returns null when peer returns empty history")
        void returnsNullWhenHistoryIsEmpty() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            RangedAddressBookHistory empty = RangedAddressBookHistory.newBuilder()
                    .addressBooks(List.of())
                    .build();
            AddressBookFetcher fetcher = fetcherWithClient(peer, mockClientReturning(empty));

            assertNull(fetcher.getRangedNodeAddressBookHistory());
        }

        @Test
        @DisplayName("Returns null when all keys are blank")
        void returnsNullWhenAllKeysBlank() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            AddressBookFetcher fetcher = fetcherWithClient(peer, mockClientReturning(historyWith("", "  ")));

            assertNull(fetcher.getRangedNodeAddressBookHistory());
        }

        @Test
        @DisplayName("Increments peerRequests metric on success")
        void incrementsPeerRequestsOnSuccess() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            AddressBookFetcher fetcher = fetcherWithClient(peer, mockClientReturning(historyWith("aabbcc")));

            fetcher.getRangedNodeAddressBookHistory();

            assertEquals(1, metricsHolder.peerRequests().get());
        }

        @Test
        @DisplayName("Increments peerErrors and removes client on exception")
        void incrementsPeerErrorsOnException() {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.getAddressBook(any())).thenThrow(new RuntimeException("connection refused"));
            BlockNodeClient client = mock(BlockNodeClient.class);
            when(client.isNodeReachable()).thenReturn(true);
            when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);

            AddressBookFetcher fetcher =
                    new AddressBookFetcher(
                            BlockNodeSource.newBuilder().nodes(List.of(peer)).build(), testConfig(), metricsHolder) {
                        @Override
                        BlockNodeClient getNodeClient(BlockNodeSourceConfig ignored) {
                            nodeClientMap.put(ignored, client);
                            return client;
                        }
                    };

            assertNull(fetcher.getRangedNodeAddressBookHistory());
            assertEquals(1, metricsHolder.peerErrors().get());
            // Client must be evicted so a fresh one is created on next call
            assertTrue(fetcher.nodeClientMap.isEmpty());
        }

        @Test
        @DisplayName("Falls over to second peer when first fails")
        void failoverToSecondPeer() {
            BlockNodeSourceConfig failingPeer = peer("failing", 8080);
            BlockNodeSourceConfig goodPeer = peer("good", 8081);
            RangedAddressBookHistory expected = historyWith("aabbcc");

            BlockNodeServiceInterface.BlockNodeServiceClient failingService =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(failingService.getAddressBook(any())).thenThrow(new RuntimeException("timeout"));
            BlockNodeClient failingClient = mock(BlockNodeClient.class);
            when(failingClient.isNodeReachable()).thenReturn(true);
            when(failingClient.getBlockNodeServiceClient()).thenReturn(failingService);

            BlockNodeClient goodClient = mockClientReturning(expected);

            BlockNodeSource source = BlockNodeSource.newBuilder()
                    .nodes(List.of(failingPeer, goodPeer))
                    .build();

            AddressBookFetcher fetcher = new AddressBookFetcher(source, testConfig(), metricsHolder) {
                @Override
                BlockNodeClient getNodeClient(BlockNodeSourceConfig node) {
                    return node.address().equals("failing") ? failingClient : goodClient;
                }
            };

            RangedAddressBookHistory result = fetcher.getRangedNodeAddressBookHistory();
            assertNotNull(result);
            assertEquals(
                    "aabbcc",
                    result.addressBooks()
                            .get(0)
                            .addressBook()
                            .nodeAddress()
                            .getFirst()
                            .rsaPubKey());
        }
    }

    @Nested
    @DisplayName("close")
    class CloseTests {

        @Test
        @DisplayName("close swallows IOException from a client that fails to close")
        void closeSwallowsIoException() throws Exception {
            BlockNodeSourceConfig peer = peer("localhost", 8080);
            BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.getAddressBook(any()))
                    .thenReturn(AddressBookResponse.newBuilder()
                            .rangedAddressBookHistory(historyWith("aabbcc"))
                            .build());
            when(serviceClient.fullName()).thenReturn("localhost:8080");

            BlockNodeClient client = mock(BlockNodeClient.class);
            when(client.isNodeReachable()).thenReturn(true);
            when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);
            org.mockito.Mockito.doThrow(new java.io.IOException("simulated close failure"))
                    .when(client)
                    .close();

            AddressBookFetcher fetcher = fetcherWithClient(peer, client);
            // Populate nodeClientMap by fetching
            fetcher.getRangedNodeAddressBookHistory();

            // close() must not propagate IOException from the underlying client
            org.junit.jupiter.api.Assertions.assertDoesNotThrow(fetcher::close);
        }
    }

    @Nested
    @DisplayName("isValid")
    class IsValidTests {

        @Test
        @DisplayName("null history is invalid")
        void nullIsInvalid() {
            assertFalse(AddressBookFetcher.isValid(null));
        }

        @Test
        @DisplayName("empty history (no eras) is invalid")
        void emptyIsInvalid() {
            assertFalse(AddressBookFetcher.isValid(RangedAddressBookHistory.newBuilder()
                    .addressBooks(List.of())
                    .build()));
        }

        @Test
        @DisplayName("history with one non-blank key is valid")
        void oneNonBlankKeyIsValid() {
            assertTrue(AddressBookFetcher.isValid(historyWith("abc")));
        }

        @Test
        @DisplayName("history with any blank key is invalid")
        void anyBlankKeyIsInvalid() {
            assertFalse(AddressBookFetcher.isValid(historyWith("", "")));
        }
    }

    @Nested
    @DisplayName("Fallback to serverStatusDetail")
    class FallbackToServerStatusDetail {

        private BlockNodeClient clientWith(final BlockNodeServiceInterface.BlockNodeServiceClient serviceClient) {
            BlockNodeClient client = mock(BlockNodeClient.class);
            when(client.isNodeReachable()).thenReturn(true);
            when(client.getBlockNodeServiceClient()).thenReturn(serviceClient);
            return client;
        }

        @Test
        @DisplayName("Reads the history from serverStatusDetail when the peer answers UNIMPLEMENTED")
        void fallsBackWhenUnimplemented() {
            BlockNodeSourceConfig peer = peer("old-peer", 8080);
            RangedAddressBookHistory expected = historyWith("aabbcc");
            BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.getAddressBook(any()))
                    .thenThrow(new GrpcException(GrpcStatus.UNIMPLEMENTED, "getAddressBook is not implemented"));
            when(serviceClient.serverStatusDetail(any()))
                    .thenReturn(ServerStatusDetailResponse.newBuilder()
                            .rangedAddressBookHistory(expected)
                            .build());

            AddressBookFetcher fetcher = fetcherWithClient(peer, clientWith(serviceClient));

            assertEquals(expected, fetcher.getRangedNodeAddressBookHistory());
        }

        @Test
        @DisplayName("The fallback asks for none of the other parts of serverStatusDetail")
        void fallbackRequestsOnlyTheRosterData() {
            BlockNodeSourceConfig peer = peer("old-peer", 8080);
            BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.getAddressBook(any())).thenThrow(new GrpcException(GrpcStatus.UNIMPLEMENTED, "no"));
            when(serviceClient.serverStatusDetail(any()))
                    .thenReturn(ServerStatusDetailResponse.newBuilder()
                            .rangedAddressBookHistory(historyWith("aabbcc"))
                            .build());

            fetcherWithClient(peer, clientWith(serviceClient)).getRangedNodeAddressBookHistory();

            ArgumentCaptor<ServerStatusDetailRequest> request =
                    ArgumentCaptor.forClass(ServerStatusDetailRequest.class);
            verify(serviceClient).serverStatusDetail(request.capture());
            assertTrue(request.getValue().excludeVersions());
            assertTrue(request.getValue().excludeAvailableRanges());
            assertTrue(request.getValue().excludeStoredRanges());
        }

        @Test
        @DisplayName("Does not call serverStatusDetail when getAddressBook succeeds")
        void noFallbackOnSuccess() {
            BlockNodeSourceConfig peer = peer("new-peer", 8080);
            BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.getAddressBook(any()))
                    .thenReturn(AddressBookResponse.newBuilder()
                            .rangedAddressBookHistory(historyWith("aabbcc"))
                            .build());

            fetcherWithClient(peer, clientWith(serviceClient)).getRangedNodeAddressBookHistory();

            verify(serviceClient, never()).serverStatusDetail(any());
        }

        @Test
        @DisplayName("Does not fall back for a failure other than UNIMPLEMENTED")
        void noFallbackForOtherFailures() {
            BlockNodeSourceConfig peer = peer("flaky-peer", 8080);
            BlockNodeServiceInterface.BlockNodeServiceClient serviceClient =
                    mock(BlockNodeServiceInterface.BlockNodeServiceClient.class);
            when(serviceClient.getAddressBook(any())).thenThrow(new GrpcException(GrpcStatus.UNAVAILABLE, "down"));

            assertNull(fetcherWithClient(peer, clientWith(serviceClient)).getRangedNodeAddressBookHistory());
            verify(serviceClient, never()).serverStatusDetail(any());
        }
    }
}
