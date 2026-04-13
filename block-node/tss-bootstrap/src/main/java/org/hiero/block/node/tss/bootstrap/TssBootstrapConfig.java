// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

// spotless:off

import static com.swirlds.config.api.ConfigProperty.UNDEFINED_DEFAULT_VALUE;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/// Configuration for the tss-bootstrap module.
///
@ConfigData("tss.bootstrap")
public record TssBootstrapConfig(
        @Loggable @ConfigProperty(defaultValue = "") String ledgerId,
        @Loggable @ConfigProperty(defaultValue = "") String wrapsVerificationKey,
        @Loggable @ConfigProperty(defaultValue = "0") long nodeId,
        @Loggable @ConfigProperty(defaultValue = "0") long weight,
        @Loggable @ConfigProperty(defaultValue = "") String schnorrPublicKey,
        @Loggable @ConfigProperty(defaultValue = "0") long validFromBlock,
        @Loggable @ConfigProperty(defaultValue = "0") long rosterValidFromBlock) {}

// spotless:on
