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
 * @param maxMessageSizeBytes the http2 max message/frame size in bytes
 * @param socketSendBufferSizeBytes the socket send buffer size in bytes
 * @param socketReceiveBufferSizeBytes the socket receive buffer size in bytes
 * @param port the port the server will listen on
 * @param shutdownDelayMillis the delay in milliseconds for the service
 * @param maxTcpConnections the maximum number of TCP connections
 * @param idleConnectionPeriodMinutes the period of idle connections check in minutes
 * @param idleConnectionTimeoutMinutes the timeout of idle connections in minutes
 * @param tcpNoDelay whether to use TCP no delay
 * @param backlogSize the maximum length of the queue of incoming connections on the server socket.
 * @param writeQueueLength the number of buffers queued for write operations
 */
@ConfigData("server")
public record ServerConfig(
        @Loggable @ConfigProperty(defaultValue = "4_194_304") @Min(262_144) @Max(16_777_215) int maxMessageSizeBytes,
        @Loggable @ConfigProperty(defaultValue = "131_072") @Min(32768) @Max(Integer.MAX_VALUE)
                int socketSendBufferSizeBytes,
        @Loggable @ConfigProperty(defaultValue = "131_072") @Min(32768) @Max(Integer.MAX_VALUE)
                int socketReceiveBufferSizeBytes,
        @Loggable @ConfigProperty(defaultValue = "40840") @Min(1024) @Max(65_535) int port,
        @Loggable @ConfigProperty(defaultValue = "500") int shutdownDelayMillis,
        @Loggable @ConfigProperty(defaultValue = "1000") int maxTcpConnections,
        @Loggable @ConfigProperty(defaultValue = "5") int idleConnectionPeriodMinutes,
        @Loggable @ConfigProperty(defaultValue = "30") int idleConnectionTimeoutMinutes,
        @Loggable @ConfigProperty(defaultValue = "true") boolean tcpNoDelay,
        @Loggable @ConfigProperty(defaultValue = "8_192") int backlogSize,
        @Loggable @ConfigProperty(defaultValue = "8_192") int writeQueueLength) {}
