// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

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
 * @param port the port the server will listen on
 * @param shutdownDelayMillis the delay in milliseconds for the service
 */
@ConfigData("server")
public record ServerConfig(
        @Loggable @ConfigProperty(defaultValue = "4_194_304") @Min(10_240) @Max(16_777_215) int maxMessageSizeBytes,
        @Loggable @ConfigProperty(defaultValue = "32768") @Min(32768) @Max(Integer.MAX_VALUE)
                int socketSendBufferSizeBytes,
        @Loggable @ConfigProperty(defaultValue = "32768") @Min(32768) @Max(Integer.MAX_VALUE)
                int socketReceiveBufferSizeBytes,
        @Loggable @ConfigProperty(defaultValue = "8080") @Min(1024) @Max(65_535) int port,
        @Loggable @ConfigProperty(defaultValue = "500") int shutdownDelayMillis) {}
