// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.util.regex.Pattern;
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
        availableBlocks = availableBlocks.trim();
        fixedStreamingSequence = fixedStreamingSequence.trim();
        final Pattern pattern = Pattern.compile("^(\\[(\\d+)-(\\d+)\\]|\\d+)(,\\s*(\\[(\\d+)-(\\d+)\\]|\\d+))*$");

        Preconditions.requireInRange(sequenceScrambleLevel, 0, 10, "sequenceScrambleLevel must be between 0 and 10");
        Preconditions.requireNotBlank(
                fixedStreamingSequence, "fixedStreamingSequence must not be blank when sequenceScrambleLevel is 0");
        if (!pattern.matcher(fixedStreamingSequence).matches()) {
            throw new IllegalArgumentException(fixedStreamingSequence + " does not match the expected format");
        }
        Preconditions.requireNotBlank(
                availableBlocks, "availableBlocks must not be blank when sequenceScrambleLevel is larger than 0");
        if (!pattern.matcher(availableBlocks).matches()) {
            throw new IllegalArgumentException(availableBlocks + " does not match the expected format");
        }
    }
}
