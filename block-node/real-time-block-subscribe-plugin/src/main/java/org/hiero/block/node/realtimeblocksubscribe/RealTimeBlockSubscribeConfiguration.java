// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.subscribeclient;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

// Spotless uses palantir-java-format which forces line breaks after annotations
// like @ConfigProperty(defaultValue = "..."), making multi-annotation records hard to read.
// Disabling spotless here for readability.
// spotless:off

/**
 * Configuration for the Subscribe Client Plugin. Field defaults and semantics match the
 * design document at docs/design/streaming/subscribe-client-plugin.md (section: Configuration).
 *
 * @param deliveryMode Global delivery mode. {@code FULL_BLOCK} delivers one notification per
 *                     assembled block; {@code IMMEDIATE} forwards each item set as it arrives.
 *                     Strictly on/off per deployment, not per-peer.
 * @param blockNodeSourcesPath File path to the peer-sources JSON (parsed as the shared
 *                             {@code BlockNodeSource} PBJ message, same schema Backfill uses).
 * @param staleThresholdMs Time in milliseconds since the last {@code BlockEnd} before failing over
 *                        to a different peer. Default is 3x the nominal 1s block cadence.
 * @param initialRetryDelayMs Base interval in milliseconds for exponential per-peer backoff.
 * @param maxBackoffMs Cap in milliseconds on per-peer backoff.
 * @param reconnectMinDelayMs Minimum sleep in milliseconds between session iterations.
 * @param grpcOverallTimeout Per-call gRPC deadline, in milliseconds, for pre-flight
 *                           {@code serverStatus} calls.
 * @param peerTipPollInterval Cadence in milliseconds for polling the active peer's
 *                            {@code serverStatus} mid-stream to detect peer-tip lag.
 * @param peerTipLagThresholdBlocks Block-count gap between the active peer's advertised tip and
 *                                  another candidate's tip that triggers failover to the
 *                                  further-ahead candidate.
 * @param enableTLS TLS toggle for peer connections. Follows Backfill's convention.
 * @param maxIncomingBufferSize Helidon client incoming buffer size in bytes.
 */
@ConfigData("subscribe.client")
public record SubscribeClientConfiguration(
        @Loggable @ConfigProperty(defaultValue = "FULL_BLOCK") DeliveryMode deliveryMode,
        @Loggable @ConfigProperty(defaultValue = "") String blockNodeSourcesPath,
        @Loggable @ConfigProperty(defaultValue = "3000") @Min(100) long staleThresholdMs,
        @Loggable @ConfigProperty(defaultValue = "500") @Min(50) long initialRetryDelayMs,
        @Loggable @ConfigProperty(defaultValue = "60000") @Min(1000) long maxBackoffMs,
        @Loggable @ConfigProperty(defaultValue = "250") @Min(50) long reconnectMinDelayMs,
        @Loggable @ConfigProperty(defaultValue = "30000") @Min(1000) int grpcOverallTimeout,
        @Loggable @ConfigProperty(defaultValue = "30000") @Min(1000) long peerTipPollInterval,
        @Loggable @ConfigProperty(defaultValue = "50") @Min(1) @Max(10_000) long peerTipLagThresholdBlocks,
        @Loggable @ConfigProperty(defaultValue = "false") boolean enableTLS,
        @Loggable @ConfigProperty(defaultValue = "4194304") @Min(1_048_576) @Max(314_572_800) int maxIncomingBufferSize) {

    /** Delivery mode for notifications published onto the Unvalidated Blocks ring buffer. */
    public enum DeliveryMode {
        /** Buffer per-block item sets, deliver one notification per assembled block. */
        FULL_BLOCK,
        /** Forward one notification per received item set with no accumulation. */
        IMMEDIATE
    }
}

// restore spotless formatting
// spotless:on
