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
@ConfigData("producer")
public record PublisherConfig(
        // spotless:off
        @Loggable @ConfigProperty(defaultValue = "9_223_372_036_854_775_807") @Min(100_000L) long batchForwardLimit,
        @Loggable @ConfigProperty(defaultValue = "300") @Min(0L) long publisherUnavailabilityTimeout,
        @Loggable @ConfigProperty(defaultValue = "3") @Min(3) @Max(50) int MaxFutureBlocksBeforeStalled,
        @Loggable @ConfigProperty(defaultValue = "100") @Min(0L) @Max(200L) long staleResendPruneBuffer) {
        // spotless:on
}
