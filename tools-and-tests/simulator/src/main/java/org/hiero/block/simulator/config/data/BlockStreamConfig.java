// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.simulator.config.logging.Loggable;
import org.hiero.block.simulator.config.types.EndStreamMode;
import org.hiero.block.simulator.config.types.MidBlockFailType;
import org.hiero.block.simulator.config.types.SimulatorMode;
import org.hiero.block.simulator.config.types.StreamingMode;

/**
 * Defines the configuration data for the block stream in the Hedera Block Simulator.
 *
 * @param simulatorMode the mode of the simulator, in terms of publishing, consuming or both
 * @param lastKnownStatusesCapacity the capacity of the last known statuses
 * @param delayBetweenBlockItems the delay in microseconds between streaming each block item
 * @param maxBlockItemsToStream the maximum number of block items to stream before stopping
 * @param streamingMode the mode of streaming for the block stream (e.g., time-based, count-based)
 * @param millisecondsPerBlock the duration in milliseconds for each block when using time-based streaming
 * @param blockItemsBatchSize the number of block items to stream in each batch
 * @param midBlockFailType the type of failure to occur while streaming
 * @param midBlockFailOffset the index of the failing block
 * @param endStreamMode the mode for ending the stream
 */
@ConfigData("blockStream")
public record BlockStreamConfig(
        @Loggable @ConfigProperty(defaultValue = "PUBLISHER_SERVER") SimulatorMode simulatorMode,
        @Loggable @ConfigProperty(defaultValue = "10") int lastKnownStatusesCapacity,
        @Loggable @ConfigProperty(defaultValue = "1_500_000") int delayBetweenBlockItems,
        @Loggable @ConfigProperty(defaultValue = "100_000") int maxBlockItemsToStream,
        @Loggable @ConfigProperty(defaultValue = "MILLIS_PER_BLOCK") StreamingMode streamingMode,
        @Loggable @ConfigProperty(defaultValue = "1000") int millisecondsPerBlock,
        @Loggable @ConfigProperty(defaultValue = "1000") int blockItemsBatchSize,
        @Loggable @ConfigProperty(defaultValue = "NONE") MidBlockFailType midBlockFailType,
        @Loggable @ConfigProperty(defaultValue = "0") long midBlockFailOffset,
        @Loggable @ConfigProperty(defaultValue = "NONE") EndStreamMode endStreamMode) {

    /**
     * Creates a new {@link Builder} instance for constructing a {@code BlockStreamConfig}.
     *
     * @return a new {@code Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for creating instances of {@link BlockStreamConfig}.
     */
    public static class Builder {
        private SimulatorMode simulatorMode = SimulatorMode.PUBLISHER_CLIENT;
        private int lastKnownStatusesCapacity = 10;
        private int delayBetweenBlockItems = 1_500_000;
        private int maxBlockItemsToStream = 10_000;
        private StreamingMode streamingMode = StreamingMode.MILLIS_PER_BLOCK;
        private int millisecondsPerBlock = 1000;
        private int blockItemsBatchSize = 1000;
        private MidBlockFailType midBlockFailType = MidBlockFailType.NONE;
        private long midBlockFailOffset = 0;
        EndStreamMode endStreamMode = EndStreamMode.NONE;

        /**
         * Creates a new instance of the {@code Builder} class with default configuration values.
         */
        public Builder() {
            // Default constructor
        }

        /**
         * Sets the simulator mode for the block stream.
         *
         * @param simulatorMode the {@link SimulatorMode} to use
         * @return this {@code Builder} instance
         */
        public Builder simulatorMode(SimulatorMode simulatorMode) {
            this.simulatorMode = simulatorMode;
            return this;
        }

        /**
         * Sets the capacity of the last known statuses.
         *
         * @param lastKnownStatusesCapacity the capacity
         * @return this {@code Builder} instance
         */
        public Builder lastKnownStatusesCapacity(int lastKnownStatusesCapacity) {
            this.lastKnownStatusesCapacity = lastKnownStatusesCapacity;
            return this;
        }

        /**
         * Sets the delay between streaming each block item.
         *
         * @param delayBetweenBlockItems the delay in microseconds
         * @return this {@code Builder} instance
         */
        public Builder delayBetweenBlockItems(int delayBetweenBlockItems) {
            this.delayBetweenBlockItems = delayBetweenBlockItems;
            return this;
        }

        /**
         * Sets the maximum number of block items to stream.
         *
         * @param maxBlockItemsToStream the maximum number of items
         * @return this {@code Builder} instance
         */
        public Builder maxBlockItemsToStream(int maxBlockItemsToStream) {
            this.maxBlockItemsToStream = maxBlockItemsToStream;
            return this;
        }

        /**
         * Sets the streaming mode for the block stream.
         *
         * @param streamingMode the {@link StreamingMode} to use
         * @return this {@code Builder} instance
         */
        public Builder streamingMode(StreamingMode streamingMode) {
            this.streamingMode = streamingMode;
            return this;
        }

        /**
         * Sets the duration for each block when using time-based streaming.
         *
         * @param millisecondsPerBlock the duration in milliseconds
         * @return this {@code Builder} instance
         */
        public Builder millisecondsPerBlock(int millisecondsPerBlock) {
            this.millisecondsPerBlock = millisecondsPerBlock;
            return this;
        }

        /**
         * Sets the number of block items to stream in each batch.
         *
         * @param blockItemsBatchSize the batch size
         * @return this {@code Builder} instance
         */
        public Builder blockItemsBatchSize(int blockItemsBatchSize) {
            this.blockItemsBatchSize = blockItemsBatchSize;
            return this;
        }

        /**
         * Sets a failure type to occur while streaming.
         *
         * @param midBlockFailType the failure type
         * @return this {@code Builder} instance
         */
        public Builder midBlockFailType(MidBlockFailType midBlockFailType) {
            this.midBlockFailType = midBlockFailType;
            return this;
        }

        /**
         * Sets the index of the failing block.
         *
         * @param midBlockFailOffset the index of the failing block
         * @return this {@code Builder} instance
         */
        public Builder midBlockFailOffset(long midBlockFailOffset) {
            this.midBlockFailOffset = midBlockFailOffset;
            return this;
        }

        public Builder endStreamMode(EndStreamMode endStreamMode) {
            this.endStreamMode = endStreamMode;
            return this;
        }

        /**
         * Builds a new {@link BlockStreamConfig} instance with the configured values.
         *
         * @return a new {@code BlockStreamConfig}
         */
        public BlockStreamConfig build() {
            return new BlockStreamConfig(
                    simulatorMode,
                    lastKnownStatusesCapacity,
                    delayBetweenBlockItems,
                    maxBlockItemsToStream,
                    streamingMode,
                    millisecondsPerBlock,
                    blockItemsBatchSize,
                    midBlockFailType,
                    midBlockFailOffset,
                    endStreamMode);
        }
    }
}
