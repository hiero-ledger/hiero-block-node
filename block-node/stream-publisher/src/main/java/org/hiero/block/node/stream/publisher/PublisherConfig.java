// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import java.util.List;
import org.hiero.block.node.base.Loggable;

/// Configuration for a block stream publisher plugin.
///
/// @param batchForwardLimit The maximum number of batches that can be forwarded
/// by a single forwarding task before it needs a refresh.
/// @param publisherUnavailabilityTimeout The time in seconds to wait when we
/// have no active publishers before sending a publisher unavailability timeout
/// status update.
/// @param staleResendPruneBuffer Number of blocks behind {@code lastPersistedBlockNumber}
/// that a {@code blocksToResend} entry may sit before {@code handlePersisted} prunes it.
/// Entries within the buffer remain fillable by a publisher that still has the block;
/// entries older than the buffer are dropped to avoid asking publishers to resend a
/// block they cannot supply (which kills the connection with TOO_FAR_BEHIND).
/// @param flowControlPauseDelayNanos Duration in nanoseconds that `onNext` parks the request-processing
/// thread when a request arrives while the handler is paused. The handler is paused as part of the
/// staged shutdown sequence in `checkMidBlockAndShutdown`, which schedules the actual `shutdown` to
/// run after a fixed delay; parking the next incoming request prevents it from racing that pending
/// shutdown and lets any final outbound messages flush before the connection closes.
/// @param duplicateBlockSkipWindow Number of blocks behind `lastPersistedBlockNumber`
/// for which a duplicate block header is answered with `SkipBlock` instead of
/// `EndOfStream(DUPLICATE_BLOCK)`. A publisher that is only slightly behind can then
/// fast-forward without reconnecting; a publisher that is further behind than the
/// window is still ended so it reconnects from the correct point.
/// @param blockStreamFilterInclude Allow- vs deny-list semantics for the
/// publish-side filter. When `blockStreamFilterItemTypes` is empty the filter
/// is identity (pass-through) regardless of this value. When non-empty:
/// `true` keeps only the listed `BlockItem.item` oneof field numbers;
/// `false` drops those and keeps everything else. `BlockHeader` (1),
/// `BlockProof` (9), and `BlockFooter` (12) are always forwarded.
/// @param blockStreamFilterItemTypes Field numbers of the `BlockItem.item`
/// oneof variants the publish-side filter applies to. Empty = no filtering.
@ConfigData("producer")
public record PublisherConfig(
        // spotless:off
        @Loggable @ConfigProperty(defaultValue = "9_223_372_036_854_775_807") @Min(100_000L) long batchForwardLimit,
        @Loggable @ConfigProperty(defaultValue = "300") @Min(0L) long publisherUnavailabilityTimeout,
        @Loggable @ConfigProperty(defaultValue = "3") @Min(3) @Max(50) int MaxFutureBlocksBeforeStalled,
        @Loggable @ConfigProperty(defaultValue = "100") @Min(0L) @Max(200L) long staleResendPruneBuffer,
        @Loggable @ConfigProperty(defaultValue = "100_000_000") @Min(100_000L) @Max(5_000_000_000L) long flowControlPauseDelayNanos,
        @Loggable @ConfigProperty(defaultValue = "5") @Min(1) @Max(10) int duplicateBlockSkipWindow,
        @Loggable @ConfigProperty(defaultValue = "false") boolean blockStreamFilterInclude,
        @Loggable @ConfigProperty(defaultValue = "") List<Integer> blockStreamFilterItemTypes) {
        // spotless:on
}
