// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Use this configuration across the server features
 *
 * <p>ServerConfig will have settings for the server.
 *
 * @param maxMessageSizeBytes the PBJ max message size in bytes
 * @param socketSendBufferSizeBytes the socket send buffer size in bytes
 * @param socketReceiveBufferSizeBytes the socket receive buffer size in bytes
 * @param port the default port all services listen on; individual plugins may override this by
 *     passing an explicit port to {@link org.hiero.block.node.spi.ServiceBuilder}, or pass
 *     {@code null} to fall back to this value
 * @param shutdownDelayMillis the delay in milliseconds for the service
 * @param maxTcpConnections the maximum number of TCP connections
 * @param idleConnectionPeriodMinutes how often (minutes) the idle-connection reaper runs
 * @param idleConnectionTimeoutMinutes how long (minutes) a connection may sit idle before it is
 *     closed; kept short so idle sockets are reclaimed well before they accumulate to
 *     {@code maxTcpConnections} and the server starts refusing new connections
 * @param tcpNoDelay whether to use TCP no delay
 * @param backlogSize the maximum length of the queue of incoming connections on the server socket.
 * @param writeQueueLength the number of buffers queued for write operations
 */
// spotless:off - long annotations on record components must stay on one line
@ConfigData("server")
public record ServerConfig(
        @Loggable @ConfigProperty(defaultValue = "131_072_000") @Min(1_048_576) @Max(1_610_612_736) int maxMessageSizeBytes,
        @Loggable @ConfigProperty(defaultValue = "131_072") @Min(32768) @Max(Integer.MAX_VALUE) int socketSendBufferSizeBytes,
        @Loggable @ConfigProperty(defaultValue = "8_388_608") @Min(32768) @Max(Integer.MAX_VALUE) int socketReceiveBufferSizeBytes,
        @Loggable @ConfigProperty(defaultValue = "40840") @Min(1024) @Max(65_535) int port,
        @Loggable @ConfigProperty(defaultValue = "500") int shutdownDelayMillis,
        @Loggable @ConfigProperty(defaultValue = "1000") int maxTcpConnections,
        @Loggable @ConfigProperty(defaultValue = "5") @Min(1) @Max(60) int idleConnectionPeriodMinutes,
        @Loggable @ConfigProperty(defaultValue = "30") @Min(1) @Max(1_440) int idleConnectionTimeoutMinutes,
        @Loggable @ConfigProperty(defaultValue = "true") boolean tcpNoDelay,
        @Loggable @ConfigProperty(defaultValue = "8_192") int backlogSize,
        @Loggable @ConfigProperty(defaultValue = "8_192") int writeQueueLength) {}
// spotless:on
