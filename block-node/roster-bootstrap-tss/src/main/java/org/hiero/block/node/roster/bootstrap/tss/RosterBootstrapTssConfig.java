// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

// spotless:off

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.node.base.Loggable;

/// Configuration for the roster-bootstrap-tss module.
///
@ConfigData("roster.bootstrap.tss")
public record RosterBootstrapTssConfig(
        @Loggable @ConfigProperty(defaultValue = "") String ledgerId,
        @Loggable @ConfigProperty(defaultValue = "") String wrapsVerificationKey,
        @Loggable @ConfigProperty(defaultValue = "0") long nodeId,
        @Loggable @ConfigProperty(defaultValue = "0") long weight,
        @Loggable @ConfigProperty(defaultValue = "") String schnorrPublicKey,
        @Loggable @ConfigProperty(defaultValue = "0") long validFromBlock,
        @Loggable @ConfigProperty(defaultValue = "0") long rosterValidFromBlock) {}

// spotless:on
