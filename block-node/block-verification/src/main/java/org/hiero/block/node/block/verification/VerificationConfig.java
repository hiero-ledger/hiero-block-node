// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

// spotless:off - long annotations on record components must stay on one line

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/// Configuration for the verification module.
///
/// @param recentlyVerifiedBlocksBufferSize size of maximum stored recently verified blocks. When full,
///     the oldest one will be removed from the buffer, which is essentially a queue. The queue is used to keep
///     track of the blocks that have been recently verified, and failures of verification for blocks that have
///     been recently verified will result in an informational failure rather than a standard one.
/// @param activeSessionsBufferSize size of maximum allowed active sessions. When full and a new session needs to
///     start, room will be made for it by canceling the one with the lowest block being verified, except when
///     the session we just started is the one with the lowest block being verified.
/// @param allSourcesRequireOrdering if true, strict ordering of the next expected block to verify will be enforced
///     for blocks received from any source. If false, only
///     [org.hiero.block.node.spi.blockmessaging.BlockSource#PUBLISHER] will be strictly ordered.<br/>
///     _NOTE_ that this value should remain `true` in all "Tier 1" Block Nodes.
///     [org.hiero.block.node.spi.blockmessaging.BlockSource#PUBLISHER] will be strictly ordered.
/// @param dumpEnabled whether to write failing block bytes and metadata to disk for diagnostics (off by default)
/// @param dumpDirectoryPath directory where bad-block dump files are written
/// @param dumpRetentionDays how many days to retain dump files before the daily purge removes them
@ConfigData("verification")
public record VerificationConfig(
    @Loggable @ConfigProperty(defaultValue = "100") int recentlyVerifiedBlocksBufferSize,
    @Loggable @ConfigProperty(defaultValue = "100") int activeSessionsBufferSize,
    @Loggable @ConfigProperty(defaultValue = "true") boolean allSourcesRequireOrdering,
    @Loggable @ConfigProperty(defaultValue = "false") boolean dumpEnabled,
    @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/verification/dumps") Path dumpDirectoryPath,
    @Loggable @ConfigProperty(defaultValue = "7") int dumpRetentionDays){}

// spotless:on
