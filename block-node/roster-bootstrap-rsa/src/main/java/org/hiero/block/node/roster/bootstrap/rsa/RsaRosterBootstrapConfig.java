// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/// Configuration for the RSA roster bootstrap plugin.
///
/// The RSA bootstrap file path is configured via `app.state.rsaBootstrapFilePath` in
/// `ApplicationStateConfig`; file loading and persistence are handled by `BlockNodeApp`.
/// **Startup sequence:**
/// 1. Bootstrap file (loaded by BlockNodeApp before plugins start)
/// 2. Peer block node gRPC query (if `blockNodeSourcesPath` is set)
/// 3. Mirror Node REST API fallback (if `mirrorNodeBaseUrl` is set)
///
/// @param mirrorNodeBaseUrl base URL of the Mirror Node REST API used when no local file or peer
///     is available; leave blank to disable Mirror Node fallback
/// @param mnInitialQueryIntervalMillis The initial period between queries to the mirror node until a node address book
///     is found.
/// @param mnSubsequentQueryIntervalMillis The subsequent period between queries to the mirror node after a node address
///     book is found.
/// @param mirrorNodeConnectTimeoutSeconds TCP connect timeout when calling the Mirror Node
/// @param mirrorNodeReadTimeoutSeconds per-request read timeout when calling the Mirror Node
/// @param mirrorNodePageSize number of nodes requested per paginated Mirror Node call
/// @param blockNodeSourcesPath path to a JSON file listing peer block nodes to query via gRPC;
///     leave blank to skip the peer-query step
/// @param bnInitialQueryIntervalMillis retry interval in ms between peer queries attempts until an address book is
/// found
/// @param bnSubsequentQueryIntervalMillis retry interval in ms between peer queries after an address book is found.
/// @param enableTLS whether to enable TLS for peer gRPC connections
/// @param grpcOverallTimeout the timeout for grpc connections.
/// @param maxIncomingBufferSize maximum incoming gRPC message buffer size in bytes
@ConfigData("roster.bootstrap.rsa")
public record RsaRosterBootstrapConfig(
        // spotless:off
        @Loggable @ConfigProperty(defaultValue = "") String mirrorNodeBaseUrl,
        @Loggable @ConfigProperty(defaultValue = "5000") @Min(100) int mnInitialQueryIntervalMillis,
        @Loggable @ConfigProperty(defaultValue = "60000") @Min(10000) int mnSubsequentQueryIntervalMillis,
        @Loggable @ConfigProperty(defaultValue = "5") int mirrorNodeConnectTimeoutSeconds,
        @Loggable @ConfigProperty(defaultValue = "10") int mirrorNodeReadTimeoutSeconds,
        @Loggable @ConfigProperty(defaultValue = "100") int mirrorNodePageSize,
        @Loggable @ConfigProperty(defaultValue = "") String blockNodeSourcesPath,
        @Loggable @ConfigProperty(defaultValue = "5000") @Min(100) int bnInitialQueryIntervalMillis,
        @Loggable @ConfigProperty(defaultValue = "60000") @Min(10000) int bnSubsequentQueryIntervalMillis,
        @Loggable @ConfigProperty(defaultValue = "false") boolean enableTLS,
        @Loggable @ConfigProperty(defaultValue = "60000") @Min(10000) int grpcOverallTimeout,
        @Loggable @ConfigProperty(defaultValue = "104857600") @Min(1) int maxIncomingBufferSize) {}
// spotless:on
