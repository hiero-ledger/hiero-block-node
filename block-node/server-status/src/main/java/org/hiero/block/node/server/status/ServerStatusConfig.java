// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the server-status service plugin.
 *
 * @param port the dedicated port the server-status gRPC service binds to. When {@code null} (the
 *     default) the plugin shares the default {@code server.port}. When set it must be a valid
 *     port in the range {@code 1024}–{@code 65535}. No {@code @Min}/{@code @Max} is declared
 *     because those validators reject a {@code null} value (they would fail when the property is
 *     unset); the range is enforced by the web server when binding.
 * @param heartbeatPeriodSeconds how often the plugin emits the single periodic {@code INFO}
 *     status heartbeat (available block range and next expected block) so operators can follow
 *     block progression from {@code INFO} logs alone. Defaults to {@code 300} (5 minutes) to keep {@code INFO} low-volume. A value of
 *     {@code 0} or less disables the heartbeat.
 */
@ConfigData("server.status")
public record ServerStatusConfig(
        @Loggable @ConfigProperty(defaultValue = ConfigProperty.NULL_DEFAULT_VALUE)
        Integer port,

        @Loggable @ConfigProperty(defaultValue = "300") int heartbeatPeriodSeconds) {}
