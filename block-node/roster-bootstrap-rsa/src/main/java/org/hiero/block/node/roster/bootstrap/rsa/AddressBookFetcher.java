// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.internal.BlockNodeSource;
import org.hiero.block.internal.BlockNodeSourceConfig;
import org.hiero.block.node.base.client.BlockNodeClient;
import org.hiero.block.node.roster.bootstrap.rsa.RsaRosterBootstrapPlugin.MetricsHolder;

/// Fetches a `NodeAddressBook` from one or more configured peer block nodes via gRPC.
///
/// Each peer is queried in order via `serverStatusDetail`. The first peer that returns a
/// valid address book (at least one entry with a non-blank RSA public key) is accepted and
/// its address book is returned. If all peers fail or return an unusable book, `null` is
/// returned and the caller falls through to the Mirror Node fetch.
///
/// Connection pooling: a `BlockNodeClient` is created per peer on first use and cached for
/// subsequent calls. If a client becomes unreachable it is removed from the pool so it can
/// be recreated on the next attempt.
public class AddressBookFetcher implements AutoCloseable {

    private static final System.Logger LOGGER = System.getLogger(AddressBookFetcher.class.getName());

    private final BlockNodeSource blockNodeSource;
    /// Global timeout in milliseconds for gRPC calls to block nodes (used as fallback).
    private final int grpcOverallTimeout;
    private final boolean enableTls;
    private final int maxIncomingBufferSize;
    private final MetricsHolder metrics;

    /// Package-private for testing — allows tests to inject mock clients.
    final ConcurrentHashMap<BlockNodeSourceConfig, BlockNodeClient> nodeClientMap = new ConcurrentHashMap<>();

    /// Constructs a fetcher for the given peer list and config.
    ///
    /// @param blockNodeSource the list of peer block nodes to query
    /// @param config the RSA bootstrap configuration (TLS and buffer-size settings)
    /// @param metrics holder for peer-query metrics counters
    public AddressBookFetcher(
            @NonNull BlockNodeSource blockNodeSource,
            @NonNull RsaRosterBootstrapConfig config,
            @NonNull MetricsHolder metrics) {
        this.blockNodeSource = blockNodeSource;
        this.grpcOverallTimeout = config.grpcOverallTimeout();
        this.enableTls = config.enableTLS();
        this.maxIncomingBufferSize = config.maxIncomingBufferSize();
        this.metrics = metrics;
        for (BlockNodeSourceConfig node : blockNodeSource.nodes()) {
            LOGGER.log(INFO, "Loaded peer block node: {0}", node);
        }
    }

    /// Queries each configured peer in order and returns the first valid `NodeAddressBook`.
    ///
    /// A `NodeAddressBook` is considered valid if it contains at least one `NodeAddress` with
    /// a non-blank RSA public key. If no peer returns a usable book, `null` is returned.
    ///
    /// @return a valid `NodeAddressBook`, or `null` if all peers fail or return empty books
    public NodeAddressBook getNodeAddressBook() {
        for (BlockNodeSourceConfig node : blockNodeSource.nodes()) {
            BlockNodeClient client = getNodeClient(node);
            if (client == null || !client.isNodeReachable()) {
                LOGGER.log(DEBUG, "Peer [{0}] is not reachable, skipping", node.address());
                continue;
            }

            try {
                final ServerStatusDetailResponse response =
                        client.getBlockNodeServiceClient().serverStatusDetail(new ServerStatusRequest());
                final NodeAddressBook book = response.nodeAddressBook();
                metrics.peerRequests().increment();

                if (isValid(book)) {
                    LOGGER.log(
                            INFO,
                            "Received valid NodeAddressBook with {0} entries from peer [{1}]",
                            book.nodeAddress().size(),
                            node.address());
                    return book;
                }
                LOGGER.log(
                        DEBUG, "Peer [{0}] returned an empty or key-less NodeAddressBook, trying next", node.address());
            } catch (RuntimeException e) {
                LOGGER.log(
                        INFO,
                        "Failed to retrieve NodeAddressBook from peer [{0}]: {1}",
                        node.address(),
                        e.getMessage());
                metrics.peerErrors().increment();
                // Remove so a fresh client is created on the next attempt
                nodeClientMap.remove(node);
            }
        }
        return null;
    }

    /// Returns a cached or newly-created `BlockNodeClient` for the given peer.
    /// If the existing client is unreachable it is evicted and a fresh one is created.
    ///
    /// @param node the peer node configuration
    /// @return a `BlockNodeClient` for the node
    protected BlockNodeClient getNodeClient(BlockNodeSourceConfig node) {
        BlockNodeClient existing = nodeClientMap.get(node);
        if (existing != null && !existing.isNodeReachable()) {
            try {
                nodeClientMap.remove(node).close();
            } catch (IOException e) {
                LOGGER.log(WARNING, "Unable to close BlockNodeClient [{0}]: {1}", node.name(), e);
            }
            LOGGER.log(DEBUG, "Removed unreachable client for peer [{0}], will recreate", node.address());
        }
        return nodeClientMap.computeIfAbsent(node, this::fromBlockNodeSourceConfig);
    }

    /// Returns `true` if the book has at least one entry with a non-blank RSA public key.
    static boolean isValid(NodeAddressBook book) {
        if (book == null || book.nodeAddress().isEmpty()) return false;
        for (NodeAddress nodeAddress : book.nodeAddress()) {
            if (nodeAddress.rsaPubKey() != null && !nodeAddress.rsaPubKey().isBlank()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        for (BlockNodeClient client : nodeClientMap.values()) {
            try {
                client.close();
            } catch (IOException e) {
                LOGGER.log(
                        WARNING,
                        "Unable to close BlockNodeClient [{0}]: {1}",
                        client.getBlockNodeServiceClient().fullName(),
                        e);
            }
        }
    }

    private BlockNodeClient fromBlockNodeSourceConfig(BlockNodeSourceConfig n) {
        return new BlockNodeClient(n, grpcOverallTimeout, enableTls, maxIncomingBufferSize, n.grpcWebclientTuning());
    }
}
