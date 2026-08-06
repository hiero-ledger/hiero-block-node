// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.TssData;
import org.hiero.block.internal.BlockNodeSource;
import org.hiero.block.internal.BlockNodeSourceConfig;
import org.hiero.block.node.base.client.BlockNodeClient;
import org.hiero.block.node.roster.bootstrap.tss.RosterBootstrapTssPlugin.MetricsHolder;

/// Client for fetching TssData from block nodes using gRPC.
/// This client handles fetching TssData from select nodes.
///
/// The client is initialized with a path to a block node preference file, which contains
/// a list of block nodes with their addresses, ports, and priorities.
public class TssDataFetcher implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(TssDataFetcher.class.getName());

    /// Source of block node configurations.
    private final BlockNodeSource blockNodeSource;

    /// Global timeout in milliseconds for gRPC calls to block nodes (used as fallback).
    private final int globalGrpcTimeoutMs;

    /// Enable TLS for secure connections to block nodes.
    private final boolean enableTls;
    /// Maximum incoming buffer size in bytes for the gRPC client.
    private final int maxIncomingBufferSize;
    /// TSS data is small (key material, not full blocks) — 512 KB is a safe ceiling.
    private static final int TSS_DATA_MAX_PROTOBUF_MESSAGE_SIZE_BYTES = 512 * 1024;

    private final RosterBootstrapTssPlugin.MetricsHolder metrics;

    /// Map of BlockNodeSourceConfig to BlockNodeClient instances.
    /// This allows us to reuse clients for the same node configuration.
    /// Package-private for testing.
    final ConcurrentHashMap<BlockNodeSourceConfig, BlockNodeClient> nodeClientMap = new ConcurrentHashMap<>();

    /// Constructor for the fetcher responsible for retrieving TssData from peer block nodes.
    ///
    /// @param blockNodeSource the blocknode source configuration
    /// @param config the blocknode configuration containing retry, timeout, and other settings
    public TssDataFetcher(
            BlockNodeSource blockNodeSource, RosterBootstrapTssConfig config, @NonNull MetricsHolder metrics) {
        this.blockNodeSource = blockNodeSource;
        this.enableTls = config.enableTLS();
        this.globalGrpcTimeoutMs = config.grpcOverallTimeout();
        this.maxIncomingBufferSize = config.maxIncomingBufferSize();
        this.metrics = metrics;
        for (BlockNodeSourceConfig node : blockNodeSource.nodes()) {
            LOGGER.log(INFO, "Loaded backfill source node: {0}", node);
        }
    }

    /// Returns a BlockNodeClient for the given BlockNodeSourceConfig.
    /// If a client for the node already exists, it returns that client.
    /// Otherwise, it creates a new client and stores it in the map.
    ///
    /// Per-node gRPC tuning (timeouts, HTTP/2 settings, buffer sizes) is passed
    /// to the client. When tuning values are 0 or not specified, the global
    /// timeout from BlockNodeConfiguration is used as fallback.
    ///
    /// @param node the BlockNodeSourceConfig to get the client for
    /// @return a BlockNodeClient for the specified node
    protected BlockNodeClient getNodeClient(BlockNodeSourceConfig node) {
        // Atomic check-close-construct: avoids the window a separate get()+remove()+
        // computeIfAbsent() would leave between evicting an unreachable client and
        // constructing its replacement.
        return nodeClientMap.compute(node, (key, current) -> {
            if (current != null) {
                if (current.isNodeReachable()) {
                    return current;
                }
                closeQuietly(key, current);
                LOGGER.log(DEBUG, "Removed unreachable client for node [{0}], will attempt to recreate", key.address());
            }
            return new BlockNodeClient(
                    key,
                    globalGrpcTimeoutMs,
                    enableTls,
                    maxIncomingBufferSize,
                    TSS_DATA_MAX_PROTOBUF_MESSAGE_SIZE_BYTES,
                    key.grpcWebclientTuning());
        });
    }

    /// Perform a serverStatusDetail call per configured node and capture the TssData.
    ///
    /// @return List of the TssData from each of the Nodes
    public List<TssData> getTssData() {
        List<TssData> tssDataList = new ArrayList<>();

        for (BlockNodeSourceConfig node : blockNodeSource.nodes()) {
            BlockNodeClient currentNodeClient = getNodeClient(node);
            if (currentNodeClient == null || !currentNodeClient.isNodeReachable()) {
                continue;
            }

            try {
                final ServerStatusDetailResponse detailResponse =
                        currentNodeClient.getBlockNodeServiceClient().serverStatusDetail(new ServerStatusRequest());
                TssData tssData = detailResponse.tssData();
                if (tssData != null) tssDataList.add(tssData);
                metrics.tssDataRequests().increment();
            } catch (RuntimeException e) {
                final String failedToRetrieveTssData = "Failed to retrieve TssData from node [{0}]: {1}";
                LOGGER.log(WARNING, failedToRetrieveTssData, node, e.getMessage());
                metrics.tssDataErrors().increment();
                // Evict the cached client so a fresh connection is established on the next
                // poll cycle instead of retrying the same dead one forever — isNodeReachable()
                // only reflects initial connect success and is never updated by a later RPC
                // failure, so without this eviction every subsequent poll fails identically
                // (mirrors BackfillFetcher.markFailure()'s client eviction).
                removeAndClose(node);
            }
        }

        return tssDataList;
    }

    /** Removes and closes the cached client for {@code node}, if present. */
    private void removeAndClose(BlockNodeSourceConfig node) {
        BlockNodeClient removed = nodeClientMap.remove(node);
        if (removed != null) {
            closeQuietly(node, removed);
        }
    }

    /** Closes {@code client}, logging (not throwing) if it fails. */
    private void closeQuietly(BlockNodeSourceConfig node, BlockNodeClient client) {
        try {
            client.close();
        } catch (IOException e) {
            LOGGER.log(WARNING, "Unable to close BlockNodeClient [{0}]: {1}", node.address(), e);
        }
    }

    @Override
    public void close() throws IOException {
        for (Entry<BlockNodeSourceConfig, BlockNodeClient> entry : nodeClientMap.entrySet()) {
            closeQuietly(entry.getKey(), entry.getValue());
        }
    }
}
