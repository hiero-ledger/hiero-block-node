// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.verification.VerificationServicePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.verification {
    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.hedera.cryptography.wraps;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.common;
    requires org.hiero.block.node.base;
    requires org.hiero.metrics;
    requires com.github.spotbugs.annotations;

    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;
    // export configuration classes to the config module and app
    exports org.hiero.block.node.verification to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            VerificationServicePlugin;
}
