// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.app.state.ApplicationStateConfigExtension;
import org.hiero.block.node.app.state.ApplicationStateFacilityPlugin;

module org.hiero.block.node.app.state {
    // export configuration classes to the config module
    exports org.hiero.block.node.app.state to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires transitive org.hiero.metrics;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.node.base;
    requires java.logging;
    requires static transitive com.github.spotbugs.annotations;

    provides com.swirlds.config.api.ConfigurationExtension with
            ApplicationStateConfigExtension;
    provides org.hiero.block.node.spi.ApplicationStateFacility with
            ApplicationStateFacilityPlugin;
}
