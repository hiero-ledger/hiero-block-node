// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/// Configuration for the health service plugin. The health and statusz endpoints run on their own
/// dedicated web server, so this configuration carries that server's own socket and connection
/// settings, independent of the general `server.*` configuration.
///
/// The primary value here is smaller buffers, shorter timeouts, and smaller
/// queues because the health API is intended for small requests, small messages
/// and short connection times (a few milliseconds or less).
///
/// @param port the dedicated port the health and statusz HTTP endpoints bind to
///     on their own web server. Defaults to `40983`. Must be a valid port in
///     the range `1024`–`65535`; the range is enforced by the web server
///     when binding.
/// @param socketSendBufferSizeBytes the socket send buffer size in bytes for
///     the health web server
/// @param socketReceiveBufferSizeBytes the socket receive buffer size in bytes
///     for the health web server
/// @param maxTcpConnections the maximum number of TCP connections for the
///     health web server
/// @param idleConnectionPeriodMinutes how often (minutes) the idle-connection
///     reaper runs
/// @param idleConnectionTimeoutMinutes how long (minutes) a connection may sit
///     idle before it is closed
/// @param tcpNoDelay whether to use TCP no delay on the health web server
/// @param backlogSize the maximum length of the queue of incoming connections
///     on the server socket
/// @param writeQueueLength the number of buffers queued for write operations
// spotless:off - long annotations on record components must stay on one line
@ConfigData("health")
public record HealthConfig(
    @Loggable @ConfigProperty(defaultValue = "40983") @Min(1024) @Max(65535) Integer port,
    @Loggable @ConfigProperty(defaultValue = "32_768") @Min(32768) @Max(2_097_152) int socketSendBufferSizeBytes,
    @Loggable @ConfigProperty(defaultValue = "65_536") @Min(32768) @Max(2_097_152) int socketReceiveBufferSizeBytes,
    @Loggable @ConfigProperty(defaultValue = "1_024") @Min(64) @Max(4096) int backlogSize,
    @Loggable @ConfigProperty(defaultValue = "2_048") @Min(64) @Max(4096) int writeQueueLength,
    @Loggable @ConfigProperty(defaultValue = "100") @Min(1) @Max(1024) int maxTcpConnections,
    @Loggable @ConfigProperty(defaultValue = "1") @Min(1) @Max(60) int idleConnectionPeriodMinutes,
    @Loggable @ConfigProperty(defaultValue = "2") @Min(1) @Max(1_440) int idleConnectionTimeoutMinutes,
    @Loggable @ConfigProperty(defaultValue = "true") boolean tcpNoDelay) {}
// spotless:on
