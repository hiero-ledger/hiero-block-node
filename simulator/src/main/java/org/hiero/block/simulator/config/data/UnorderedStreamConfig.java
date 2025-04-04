// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import static org.hiero.block.simulator.config.util.ParsingUtils.parseRangeSet;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.util.LinkedHashSet;
import java.util.Set;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.simulator.config.logging.Loggable;

/**
 * Defines the configuration data for the unordered block streaming in the Hedera Block Simulator.
 *
 * @param enabled                indicates whether to enable the unordered streaming mode
 * @param availableBlocks        the numbers of the blocks which will be considered available for sending
 * @param sequenceScrambleLevel  the coefficient used to randomly scramble the stream
 * @param fixedStreamingSequence the numbers of the blocks which will form the steam (could be from availableBlocks or not)
 */
@ConfigData("unorderedStream")
public record UnorderedStreamConfig(
        @Loggable @ConfigProperty(defaultValue = "false") boolean enabled,
        @Loggable @ConfigProperty(defaultValue = "[1-10]") String availableBlocks,
        @Loggable @ConfigProperty(defaultValue = "0") int sequenceScrambleLevel,
        @Loggable @ConfigProperty(defaultValue = "1,2,4,3") String fixedStreamingSequence) {

    public UnorderedStreamConfig {
        if (enabled) {
            Preconditions.requireInRange(
                    sequenceScrambleLevel, 0, 10, "sequenceScrambleLevel must be between 0 and 10");
            if (sequenceScrambleLevel == 0) {
                Preconditions.requireNotBlank(
                        fixedStreamingSequence,
                        "fixedStreamingSequence must not be blank when sequenceScrambleLevel is 0");
            } else {
                Preconditions.requireNotBlank(
                        availableBlocks,
                        "availableBlocks must not be blank when sequenceScrambleLevel is larger than 0");
            }
        }
    }

    public Set<Long> availableBlocksAsSet() {
        return parseRangeSet(availableBlocks);
    }

    public LinkedHashSet<Long> fixedStreamingSequenceAsSet() {
        return parseRangeSet(fixedStreamingSequence);
    }

    /**
     * Creates a new {@link Builder} instance for constructing a {@code UnorderedStreamConfig}.
     *
     * @return a new {@code Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for creating instances of {@link UnorderedStreamConfig}.
     */
    public static class Builder {
        private boolean enabled = false;
        private String availableBlocks = "";
        private int sequenceScrambleLevel = 0;
        private String fixedStreamingSequence = "";

        /**
         * Creates a new instance of the {@code Builder} class with default configuration values.
         */
        public Builder() {
            // Default constructor
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder availableBlocks(String availableBlocks) {
            this.availableBlocks = availableBlocks;
            return this;
        }

        public Builder sequenceScrambleLevel(int sequenceScrambleLevel) {
            this.sequenceScrambleLevel = sequenceScrambleLevel;
            return this;
        }

        public Builder fixedStreamingSequence(String fixedStreamingSequence) {
            this.fixedStreamingSequence = fixedStreamingSequence;
            return this;
        }

        /**
         * Builds a new {@link UnorderedStreamConfig} instance with the configured values.
         *
         * @return a new {@code UnorderedStreamConfig}
         */
        public UnorderedStreamConfig build() {
            return new UnorderedStreamConfig(enabled, availableBlocks, sequenceScrambleLevel, fixedStreamingSequence);
        }
    }
}
