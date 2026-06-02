// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
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
/// @param perHandlerMessageBudget Maximum number of `onNext` calls a single handler
/// may process in one refresh interval before being parked. When the budget reaches
/// zero the PBJ delivery thread for that handler is parked until the next refresh.
/// @param totalMessageBudgetPerInterval Aggregate `onNext` call budget across all
/// handlers per interval. If the total consumed in one interval meets or exceeds this
/// value the refresh withholds all individual budgets until the next interval.
/// @param flowControlRefreshIntervalNanos Duration in nanoseconds between flow-control
/// refresh cycles. The manager resets per-handler budgets and checks aggregate consumption
/// at this cadence.
/// @param consecutiveFullBudgetIntervalsForPenalty Number of consecutive intervals in
/// which a handler fully exhausts its budget before a penalty pause is applied.
/// @param penaltyDurationSeconds Base penalty pause duration in seconds. Each additional
/// penalty in the same reset window multiplies this by the penalty count.
/// @param penaltyResetIntervalSeconds Interval in seconds after which all per-handler
/// penalty counts are reset (tumbling window).
/// @param duplicateBlockSkipWindow Number of blocks behind `lastPersistedBlockNumber`
/// for which a duplicate block header is answered with `SkipBlock` instead of
/// `EndOfStream(DUPLICATE_BLOCK)`. A publisher that is only slightly behind can then
/// fast-forward without reconnecting; a publisher that is further behind than the
/// window is still ended so it reconnects from the correct point.
@ConfigData("producer")
public record PublisherConfig(
        // spotless:off
        @Loggable @ConfigProperty(defaultValue = "9_223_372_036_854_775_807") @Min(100_000L) long batchForwardLimit,
        @Loggable @ConfigProperty(defaultValue = "300") @Min(0L) long publisherUnavailabilityTimeout,
        @Loggable @ConfigProperty(defaultValue = "3") @Min(3) @Max(50) int MaxFutureBlocksBeforeStalled,
        @Loggable @ConfigProperty(defaultValue = "100") @Min(0L) @Max(200L) long staleResendPruneBuffer,
        @Loggable @ConfigProperty(defaultValue = "100") @Min(10) @Max(100_000) int perHandlerMessageBudget,
        @Loggable @ConfigProperty(defaultValue = "1000") @Min(10) int totalMessageBudgetPerInterval,
        @Loggable @ConfigProperty(defaultValue = "10_000_000") @Min(100_000) @Max(100_000_000) long flowControlRefreshIntervalNanos,
        @Loggable @ConfigProperty(defaultValue = "15") @Min(10) int consecutiveFullBudgetIntervalsForPenalty,
        @Loggable @ConfigProperty(defaultValue = "30") @Min(2) @Max(600) int penaltyDurationSeconds,
        @Loggable @ConfigProperty(defaultValue = "3600") @Min(600) @Max(86400) long penaltyResetIntervalSeconds,
        @Loggable @ConfigProperty(defaultValue = "5") @Min(1) @Max(10) int duplicateBlockSkipWindow) {
        // spotless:on
}
