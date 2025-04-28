// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.health.HealthServicePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.health {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive org.hiero.block.node.spi;
    requires transitive io.helidon.webserver;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            HealthServicePlugin;
}
