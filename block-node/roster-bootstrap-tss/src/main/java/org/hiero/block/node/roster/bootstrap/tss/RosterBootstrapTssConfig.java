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
        @Loggable @ConfigProperty(defaultValue = "") String tssDataJsonPath) {}

// spotless:on
