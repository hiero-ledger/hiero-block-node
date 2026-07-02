// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.verification.VerificationServicePlugin;

module org.hiero.block.node.verification {
    exports org.hiero.block.node.verification to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.hedera.cryptography.wraps;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.common;
    requires org.hiero.block.node.base;
    requires org.hiero.metrics;
    requires com.github.spotbugs.annotations;

    // spotless:off Spotless is broken at the moment, wants to both add and remove the same line.
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;
    provides org.hiero.block.node.spi.BlockNodePlugin with
            VerificationServicePlugin;
    // spotless:on
}
