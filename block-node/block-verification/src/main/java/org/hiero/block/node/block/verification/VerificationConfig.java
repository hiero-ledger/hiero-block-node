// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

// spotless:off - long annotations on record components must stay on one line

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/// Configuration for the verification module.
///
/// @param recentlyVerifiedBlocksBufferSize size of maximum stored recently verified blocks. When full,
///     the oldest one will be removed from the buffer, which is essentially a queue. The queue is used to keep
///     track of the blocks that have been recently verified, and failures of verification for blocks that have
///     been recently verified will result in an informational failure rather than a standard one.
/// @param activeSessionsBufferSize the limit of simultaneously active sessions, counted across the high priority
///     lane (the publisher's live stream) and the low priority lane (every other channel) together. Every admission
///     over this limit evicts one session, so the count does not stay above it. The stuck session with the highest
///     block goes first, the low priority one on a tie: a session waiting for order above the first block after the
///     last verified block that no session holds. When no session is stuck, the low priority sessions go before any
///     high priority session. The session the publisher is still streaming is never evicted. Must be at least 13. A
///     prime number is recommended, as the default 101 is; a Mersenne number (2 to the power of n, minus 1) is
///     acceptable.
/// @param firstOrderedBlock the first block number that will require strict ordering. All blocks below this
///     setting will not await order, and successful verification will be reported immediately.
/// @param allSourcesRequireOrdering if true, strict ordering of the next expected block to verify will be enforced
///     for blocks received from any source. If false, only
///     [org.hiero.block.node.spi.blockmessaging.BlockSource#PUBLISHER] will be strictly ordered.<br/>
///     _NOTE_ that this value should remain `true` in all "Tier 1" Block Nodes.
/// @param dumpEnabled whether to write failing block bytes and metadata to disk for diagnostics (off by default)
/// @param dumpDirectoryPath directory where bad-block dump files are written
/// @param dumpRetentionDays how many days to retain dump files before the daily purge removes them
@ConfigData("verification")
public record VerificationConfig(
    @Loggable @ConfigProperty(defaultValue = "100") int recentlyVerifiedBlocksBufferSize,
    @Loggable @ConfigProperty(defaultValue = "101") @Min(13) int activeSessionsBufferSize,
    @Loggable @ConfigProperty(defaultValue = "0") long firstOrderedBlock,
    @Loggable @ConfigProperty(defaultValue = "true") boolean allSourcesRequireOrdering,
    @Loggable @ConfigProperty(defaultValue = "false") boolean dumpEnabled,
    @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/verification/dumps") Path dumpDirectoryPath,
    @Loggable @ConfigProperty(defaultValue = "7") int dumpRetentionDays){}

// spotless:on
