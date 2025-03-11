// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.simulator.config.data;

import static java.util.Objects.requireNonNull;
import static org.hiero.block.common.utils.Preconditions.requireWhole;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Represents the status of the stream.
 *
 * @param publishedBlocks the number of published blocks from publish client
 * @param processedBlocks the number of processed blocks from publish server
 * @param consumedBlocks the number of consumed blocks
 * @param lastKnownPublisherClientStatuses the last known statuses from publish client
 * @param lastKnownPublisherServerStatuses the last known statuses from publish server
 * @param lastKnownConsumersStatuses the last known consumers statuses
 */
public record StreamStatus(
        long publishedBlocks,
        long processedBlocks,
        long consumedBlocks,
        Deque<String> lastKnownPublisherClientStatuses,
        Deque<String> lastKnownPublisherServerStatuses,
        Deque<String> lastKnownConsumersStatuses) {

    /**
     * Creates a new {@link Builder} instance for constructing a {@code StreamStatus}.
     *
     * @return a new {@code Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for creating instances of {@link StreamStatus}.
     */
    public static class Builder {
        private long publishedBlocks = 0;
        private long processedBlocks = 0;
        private long consumedBlocks = 0;
        private Deque<String> lastKnownPublisherClientStatuses = new ArrayDeque<>();
        private Deque<String> lastKnownPublisherServerStatuses = new ArrayDeque<>();
        private Deque<String> lastKnownConsumersStatuses = new ArrayDeque<>();

        /**
         * Creates a new instance of the {@code Builder} class with default configuration values.
         */
        public Builder() {
            // Default constructor
        }

        /**
         * Sets the number of published blocks by publish client.
         *
         * @param publishedBlocks the number of published blocks
         * @return the builder instance
         */
        public Builder publishedBlocks(long publishedBlocks) {
            requireWhole(publishedBlocks);
            this.publishedBlocks = publishedBlocks;
            return this;
        }

        /**
         * Sets the number of processed blocks by publish server.
         *
         * @param processedBlocks the number of processed blocks by publish server.
         * @return the builder instance
         */
        public Builder processedBlocks(long processedBlocks) {
            requireWhole(processedBlocks);
            this.processedBlocks = processedBlocks;
            return this;
        }

        /**
         * Sets the number of consumed blocks.
         *
         * @param consumedBlocks the number of consumed blocks
         * @return the builder instance
         */
        public Builder consumedBlocks(long consumedBlocks) {
            requireWhole(consumedBlocks);
            this.consumedBlocks = consumedBlocks;
            return this;
        }

        /**
         * Sets the last known publisher client statuses.
         *
         * @param lastKnownPublisherClientStatuses the last known publisher statuses from publish client
         * @return the builder instance
         */
        public Builder lastKnownPublisherClientStatuses(List<String> lastKnownPublisherClientStatuses) {
            requireNonNull(lastKnownPublisherClientStatuses);
            this.lastKnownPublisherClientStatuses = new ArrayDeque<>(lastKnownPublisherClientStatuses);
            return this;
        }

        /**
         * Sets the last known publisher server statuses.
         *
         * @param lastKnownPublisherServerStatuses the last known publisher statuses from publish server
         * @return the builder instance
         */
        public Builder lastKnownPublisherServerStatuses(List<String> lastKnownPublisherServerStatuses) {
            requireNonNull(lastKnownPublisherServerStatuses);
            this.lastKnownPublisherServerStatuses = new ArrayDeque<>(lastKnownPublisherServerStatuses);
            return this;
        }

        /**
         * Sets the last known consumers statuses.
         *
         * @param lastKnownConsumersStatuses the last known consumers statuses
         * @return the builder instance
         */
        public Builder lastKnownConsumersStatuses(List<String> lastKnownConsumersStatuses) {
            requireNonNull(lastKnownConsumersStatuses);
            this.lastKnownConsumersStatuses = new ArrayDeque<>(lastKnownConsumersStatuses);
            return this;
        }

        /**
         * Builds a new {@link StreamStatus} instance.
         *
         * @return a new {@link StreamStatus} instance
         */
        public StreamStatus build() {
            return new StreamStatus(
                    publishedBlocks,
                    processedBlocks,
                    consumedBlocks,
                    lastKnownPublisherClientStatuses,
                    lastKnownPublisherServerStatuses,
                    lastKnownConsumersStatuses);
        }
    }
}
