// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server;

import com.hedera.block.server.config.logging.Loggable;
import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.common.utils.Preconditions;

/**
 * Use this configuration across the server features
 *
 * <p>ServerConfig will have settings for the server.
 *
 * @param maxMessageSizeBytes the http2 max message/frame size in bytes
 * @param port the port the server will listen on
 */
@ConfigData("server")
public record ServerConfig(
        @Loggable
                @ConfigProperty(defaultValue = defaultMaxMessageSizeBytes)
                @Min(minMaxMessageSizeBytes)
                @Max(maxMaxMessageSizeBytes)
                int maxMessageSizeBytes,
        @Loggable
                @ConfigProperty(defaultValue = defaultSocketSendBufferSizeBytes)
                @Min(minSocketSendBufferSizeBytes)
                @Max(Integer.MAX_VALUE)
                int socketSendBufferSizeBytes,
        @Loggable
                @ConfigProperty(defaultValue = defaultSocketReceiveBufferSizeBytes)
                @Min(minSocketReceiveBufferSizeBytes)
                @Max(Integer.MAX_VALUE)
                int socketReceiveBufferSizeBytes,
        @Loggable @ConfigProperty(defaultValue = defaultPort) @Min(minPort) @Max(maxPort) int port) {

    // Constants for maxMessageSizeBytes property
    static final String defaultMaxMessageSizeBytes = "4_194_304";
    static final int minMaxMessageSizeBytes = 10_240;
    static final int maxMaxMessageSizeBytes = 16_777_215;

    // Constants for connectionSendBufferSize property
    static final String defaultSocketSendBufferSizeBytes = "32768";
    static final int minSocketSendBufferSizeBytes = 32768;

    // Constants for connectionReceiveBufferSize property
    static final String defaultSocketReceiveBufferSizeBytes = "32768";
    static final int minSocketReceiveBufferSizeBytes = 32768;

    // Constants for port property
    static final String defaultPort = "8080";
    static final int minPort = 1024;
    static final int maxPort = 65_535;

    private static final String SERVER_CONFIG_PREFIX = "server.";
    private static final String ERROR_MSG_TEMPLATE = " value %d is out of range [%d, %d]";

    /**
     * Validate the configuration.
     *
     * @throws IllegalArgumentException if the configuration is invalid
     */
    public ServerConfig {
        Preconditions.requireInRange(
                maxMessageSizeBytes,
                minMaxMessageSizeBytes,
                maxMaxMessageSizeBytes,
                SERVER_CONFIG_PREFIX + "maxMessageSizeBytes" + ERROR_MSG_TEMPLATE);
        Preconditions.requireInRange(
                socketSendBufferSizeBytes,
                minSocketSendBufferSizeBytes,
                Integer.MAX_VALUE,
                SERVER_CONFIG_PREFIX + "socketSendBufferSizeBytes" + ERROR_MSG_TEMPLATE);
        Preconditions.requireInRange(
                socketReceiveBufferSizeBytes,
                minSocketReceiveBufferSizeBytes,
                Integer.MAX_VALUE,
                SERVER_CONFIG_PREFIX + "socketReceiveBufferSizeBytes" + ERROR_MSG_TEMPLATE);
        Preconditions.requireInRange(port, minPort, maxPort, SERVER_CONFIG_PREFIX + "port" + ERROR_MSG_TEMPLATE);
    }
}
