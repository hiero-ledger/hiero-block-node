// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.verification.VerificationServicePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.verification {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    // export configuration classes to the config module and app
    exports org.hiero.block.node.verification to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;
    exports org.hiero.block.node.verification.session.impl to
            com.swirlds.config.extensions,
            com.swirlds.config.impl,
            org.hiero.block.node.app;
    exports org.hiero.block.node.verification.session to
            com.swirlds.config.extensions,
            com.swirlds.config.impl,
            org.hiero.block.node.app;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.common;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.swirlds.metrics.api;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            VerificationServicePlugin;
}
