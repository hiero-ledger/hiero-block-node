// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

// spotless:off

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/// Configuration for the roster-bootstrap-tss module.
///
@ConfigData("roster.bootstrap.tss")
public record RosterBootstrapTssConfig(
        @Loggable @ConfigProperty(defaultValue = "") String blockNodeSourcesPath,
        @Loggable @ConfigProperty(defaultValue = "60000") @Min(100) int queryPeerInterval,
        @Loggable @ConfigProperty(defaultValue = "5000") @Min(500) int queryPeerInitialDelay,
        @Loggable @ConfigProperty(defaultValue = "104857600") @Min(10_485_760) @Max(314_572_800) int maxIncomingBufferSize,
        @Loggable @ConfigProperty(defaultValue = "false") boolean enableTLS) {}

// spotless:on
