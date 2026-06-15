// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

// spotless:off

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/// Configuration for the roster-bootstrap-tss module.
/// @param blockNodeSourcesPath File path to the JSON file containing a list of block node servers to query for
/// TSS data.
/// @param queryPeerInterval The amount of time in milliseconds between queries to the Peer Block Nodes for TSS data.
/// @param maxIncomingBufferSize Maximum block size used for the BlockNode Client
/// @param enableTLS Flag indicating whether TLS should be enabled for the BlockNode client.
@ConfigData("roster.bootstrap.tss")
public record RosterBootstrapTssConfig(
        @Loggable @ConfigProperty(defaultValue = "") String blockNodeSourcesPath,
        @Loggable @ConfigProperty(defaultValue = "60_000") @Min(100) int queryPeerInterval,
        @Loggable @ConfigProperty(defaultValue = "104_857_600") @Min(104_857_600) @Max(314_572_800) int maxIncomingBufferSize,
        @Loggable @ConfigProperty(defaultValue = "60_000") @Min(10_000) int grpcOverallTimeout,
        @Loggable @ConfigProperty(defaultValue = "false") boolean enableTLS) {}

// spotless:on
