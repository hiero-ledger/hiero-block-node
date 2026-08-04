// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.node.base.Loggable;

/**
 * Single source of truth for the optional dedicated port of each plugin sharing the "general" web
 * server ({@link org.hiero.block.node.spi.ServiceBuilder#registerGrpcService}). A {@code null} port
 * (the default) falls back to {@code server.port}; a plugin needing its own dedicated web server
 * (like health) should not be added here.
 * <p>
 * The per-plugin env vars this record replaces (e.g. {@code BLOCK_ACCESS_PORT}) still work; see
 * {@link LegacyPortsEnvironmentConfigSource}.
 *
 * @param publisher the dedicated port the stream-publisher gRPC service binds to
 * @param subscriber the dedicated port the stream-subscriber gRPC service binds to
 * @param blockAccess the dedicated port the block-access gRPC service binds to
 * @param serverStatus the dedicated port the server-status gRPC service binds to
 */
@ConfigData("ports")
public record PortsConfig(
        @Loggable @ConfigProperty(defaultValue = ConfigProperty.NULL_DEFAULT_VALUE)
        Integer publisher,

        @Loggable @ConfigProperty(defaultValue = ConfigProperty.NULL_DEFAULT_VALUE)
        Integer subscriber,

        @Loggable @ConfigProperty(defaultValue = ConfigProperty.NULL_DEFAULT_VALUE)
        Integer blockAccess,

        @Loggable @ConfigProperty(defaultValue = ConfigProperty.NULL_DEFAULT_VALUE)
        Integer serverStatus) {}
