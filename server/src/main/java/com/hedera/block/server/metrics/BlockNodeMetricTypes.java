// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.metrics;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The BlockNodeMetricNames class contains the names of the metrics used by the BlockNode.
 *
 * <p>These names are used to register the metrics with the metrics service.
 */
public final class BlockNodeMetricTypes {
    private BlockNodeMetricTypes() {}

    /**
     * Add new counting metrics to this enum to automatically register them with the metrics
     * service.
     *
     * <p>Each enum value should have a unique grafana label and meaningful description. These
     * counters can capture data on standard operations or errors.
     */
    public enum Counter implements MetricMetadata {
        // Standard counters
        /** The number of live block items received from a producer. */
        LiveBlockItemsReceived("live_block_items_received", "Live Block Items Received"),

        /** The number of live block items received before publishing to the RingBuffer. */
        LiveBlockItems("live_block_items", "Live BlockItems"),

        /** The number of requests received by the ServerStatus API */
        RequestsReceived("requests_received", "Total Requests Received by ServerStatus API"),

        /** The number of errors encountered while handling requests */
        RequestsFailed("requests_failed", "Total Requests Failed in ServerStatus API"),

        /** The number of PublishStreamResponses generated and published to the subscribers. */
        SuccessfulPubStreamResp("successful_pub_stream_resp", "Successful Publish Stream Responses"),

        /** The number of PublishStreamResponses sent to the producers. */
        SuccessfulPubStreamRespSent("successful_pub_stream_resp_sent", "Successful Publish Stream Responses Sent"),

        /** The number of blocks persisted to storage. */
        BlocksPersisted("blocks_persisted", "Blocks Persisted"),

        /** The number of live block items consumed from the by each consumer observer. */
        LiveBlockItemsConsumed("live_block_items_consumed", "Live Block Items Consumed"),

        /** The number of single blocks retrieved from the singleBlock rpc service. */
        SingleBlocksRetrieved("single_blocks_retrieved", "Single Blocks Retrieved"),

        /** The number of single blocks not found via the singleBlock rpc service. */
        SingleBlocksNotFound("single_blocks_not_found", "Single Blocks Not Found"),

        /** The number of closed range historic blocks retrieved. */
        ClosedRangeHistoricBlocksRetrieved(
                "closed_range_historic_blocks_retrieved", "Closed Range Historic Blocks Retrieved"),

        // Verification counters

        /** The number of blocks received for verification. */
        VerificationBlocksReceived("verification_blocks_received", "Blocks Received for Verification"),

        /** The number of blocks verified successfully. */
        VerificationBlocksVerified("verification_blocks_verified", "Blocks Verified"),

        /** The number of blocks that failed verification. */
        VerificationBlocksFailed("verification_blocks_failed", "Blocks Failed Verification"),

        /** The number of blocks that failed verification due to an error. */
        VerificationBlocksError("verification_blocks_error", "Blocks Verification Error"),

        /** The time in nanoseconds taken to verify a block */
        VerificationBlockTime("verification_block_time", "Block Verification Time"),

        /** The number of historic to live stream transitions */
        HistoricToLiveStreamTransitions("historic_to_live_stream_transitions", "Historic to Live Stream Transitions"),

        /** The number of live to historic stream transitions */
        LiveToHistoricStreamTransitions("live_to_historic_stream_transitions", "Live to Historic Stream Transitions"),

        // Error counters

        /** The number of errors encountered by the live block stream mediator. */
        LiveBlockStreamMediatorError("live_block_stream_mediator_error", "Live Block Stream Mediator Error"),

        /** The number of errors encountered by the stream persistence handler. */
        StreamPersistenceHandlerError("stream_persistence_handler_error", "Stream Persistence Handler Error"),

        /** The number of blocks acked */
        AckedBlocked("acked_blocked", "Count of blocks acked"),

        /** The number of blocks failed to persist. */
        BlockPersistenceError("block_persistence_error", "Blocks Failed to Persist"),

        /** The number of errors streaming block items to clients. */
        OutboundStreamingError("outbound_streaming_error", "Outbound Streaming Error");

        private final String grafanaLabel;
        private final String description;

        Counter(String grafanaLabel, String description) {
            this.grafanaLabel = grafanaLabel;
            this.description = description;
        }

        @Override
        @NonNull
        public String grafanaLabel() {
            return grafanaLabel;
        }

        @Override
        @NonNull
        public String description() {
            return description;
        }
    }

    /**
     * Add new gauge metrics to this enum to automatically register them with the metrics service.
     *
     * <p>Each enum value should have a unique grafana label and meaningful description. These
     * gauges can capture data on standard operations or errors.
     */
    public enum Gauge implements MetricMetadata {

        /** The number of subscribers receiving the live block stream. */
        Consumers("consumers", "Consumers"),

        /** The number of producers publishing block items. */
        Producers("producers", "Producers"),

        /** The block number of the latest block as it enters the system */
        CurrentBlockNumberInbound("current_block_number_inbound", "Current Block Number Inbound"),

        /** The block number of the latest block as it exits the system bound for a consumer */
        CurrentBlockNumberOutbound("current_block_number_outbound", "Current Block Number Outbound"),

        /** The amount of capacity remaining in the mediator ring buffer. */
        MediatorRingBufferRemainingCapacity(
                "mediator_ring_buffer_remaining_capacity", "Mediator Ring Buffer Remaining Capacity"),

        /** The amount of capacity remaining in the notifier ring buffer. */
        NotifierRingBufferRemainingCapacity(
                "notifier_ring_buffer_remaining_capacity", "Notifier Ring Buffer Remaining Capacity");

        private final String grafanaLabel;
        private final String description;

        Gauge(String grafanaLabel, String description) {
            this.grafanaLabel = grafanaLabel;
            this.description = description;
        }

        @Override
        @NonNull
        public String grafanaLabel() {
            return grafanaLabel;
        }

        @Override
        @NonNull
        public String description() {
            return description;
        }
    }

    private interface MetricMetadata {
        String grafanaLabel();

        String description();
    }
}
