// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.backfill.BackfillPlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.backfill {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    // export configuration classes to the config module and app
    exports org.hiero.block.node.backfill to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.common;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.swirlds.metrics.api;
    requires org.hiero.block.node.base;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            BackfillPlugin;
}
