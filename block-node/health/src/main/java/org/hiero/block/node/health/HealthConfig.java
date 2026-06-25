// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the health service plugin.
 *
 * @param port the dedicated port the health HTTP endpoints bind to. When {@code null} (the
 *     default) the plugin shares the default {@code server.port}. When set it must be a valid
 *     port in the range {@code 1024}–{@code 65535}. No {@code @Min}/{@code @Max} is declared
 *     because those validators reject a {@code null} value (they would fail when the property is
 *     unset); the range is enforced by the web server when binding.
 */
@ConfigData("health")
public record HealthConfig(
        // spotless:off
        @Loggable @ConfigProperty(defaultValue = ConfigProperty.NULL_DEFAULT_VALUE) Integer port) {
        // spotless:on
}
