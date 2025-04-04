// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
            if (sequenceScrambleLevel < 0 || sequenceScrambleLevel > 10) {
                throw new IllegalArgumentException("sequenceScrambleLevel must be between 0 and 10");
            }
            if (sequenceScrambleLevel == 0 && fixedStreamingSequence.isBlank()) {
                throw new IllegalArgumentException(
                        "fixedStreamingSequence must not be blank when sequenceScrambleLevel is 0");
            }
            if (sequenceScrambleLevel != 0 && availableBlocks.isBlank()) {
                throw new IllegalArgumentException(
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

    private static LinkedHashSet<Long> parseRangeSet(String input) {
        LinkedHashSet<Long> result = new LinkedHashSet<>();
        if (input != null && !input.isBlank()) {
            input = input.trim();
            Pattern pattern = Pattern.compile("^(\\[(\\d+)-(\\d+)\\]|\\d+)(,\\s*(\\[(\\d+)-(\\d+)\\]|\\d+))*$");
            if (!pattern.matcher(input).matches()) {
                throw new IllegalArgumentException(input + " does not match the expected format " + pattern.pattern());
            }
            try {
                Matcher matcher = Pattern.compile("\\[(\\d+)-(\\d+)\\]|(\\d+)").matcher(input);
                while (matcher.find()) {
                    if (matcher.group(1) != null && matcher.group(2) != null) {
                        long start = Long.parseLong(matcher.group(1));
                        long end = Long.parseLong(matcher.group(2));
                        if (start <= 0 || end <= 0) {
                            throw new IllegalArgumentException(input
                                    + " does not match the expected format. Range values must be positive and non-zero: ["
                                    + start + "-" + end + "]");
                        }
                        if (start >= end) {
                            throw new IllegalArgumentException(
                                    input + " does not match the expected format. Invalid range: start >= end in ["
                                            + start + "-" + end + "]");
                        }
                        for (long i = start; i <= end; i++) {
                            result.add(i);
                        }
                    } else if (matcher.group(3) != null) {
                        long value = Long.parseLong(matcher.group(3));
                        if (value <= 0) {
                            throw new IllegalArgumentException("Values must be positive and non-zero: " + value);
                        }
                        result.add(value);
                    }
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Exception in parsing input: " + input, e);
            }
        }
        return result;
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
