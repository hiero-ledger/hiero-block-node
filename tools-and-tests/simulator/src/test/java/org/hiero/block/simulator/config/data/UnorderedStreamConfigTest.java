// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link UnorderedStreamConfig} class.
 * <p>
 * This test suite verifies configuration parsing, validation logic, and
 * correct transformation of input strings into ordered sets of block numbers.
 */
class UnorderedStreamConfigTest {

    /**
     * Verifies that scramble levels outside the accepted range (0–10)
     * throw an {@link IllegalArgumentException}.
     */
    @Test
    void testInvalidScrambleLevelInvalid() {
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, "[1-3]", -1, ""));
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, "[1-3]", 11, ""));
    }

    /**
     * Ensures that a fixed streaming sequence is required when
     * scramble level is 0; an empty or blank input should fail.
     */
    @Test
    void testMissingFixedStreamingSequenceWhenLevel0() {
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, "", 0, "   "));
    }

    /**
     * Ensures that available blocks must be provided when scrambling is enabled.
     */
    @Test
    void testMissingAvailableBlocksWhenScrambleEnabled() {
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, "   ", 5, ""));
    }

    /**
     * Ensures that an empty string for available blocks throws an exception
     * if scrambling is enabled.
     */
    @Test
    void testAvailableBlocksEmptyInputThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, " ", 5, ""));
    }

    /**
     * Verifies that improperly formatted {@code availableBlocks} values
     * result in an {@link IllegalArgumentException} when scrambling is enabled.
     * These inputs do not conform to the expected format and should trigger validation failures.
     */
    @Test
    void testInvalidAvailableBlocksFormatThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, "abc", 5, "1,2,3"));
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, "1--3", 5, "1,2,3"));
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, "[1-]", 5, "1,2,3"));
    }

    /**
     * Ensures that a correctly formatted configuration does not throw any exception.
     * <p>
     * This test verifies that a combination of valid range and single values in
     * both {@code availableBlocks} and {@code fixedStreamingSequence} passes validation.
     */
    @Test
    void testValidConfigurationDoesNotThrow() {
        assertDoesNotThrow(() -> new UnorderedStreamConfig(true, "[1-3],5,7", 5, "1,2,[3-4]"));
    }
}
