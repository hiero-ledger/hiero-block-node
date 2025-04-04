// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link UnorderedStreamConfig} class.
 *
 * This test suite verifies configuration parsing, validation logic, and
 * correct transformation of input strings into ordered sets of block numbers.
 */
class UnorderedStreamConfigTest {

    /**
     * Tests that a configuration with sequence scramble level 0 and a valid
     * fixed streaming sequence is parsed correctly into a set.
     */
    @Test
    void testValidConfigWithSequenceScrambleLevel0() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "", 0, "[1-3], 7, 9");

        LinkedHashSet<Long> expected = new LinkedHashSet<>(Set.of(1L, 2L, 3L, 7L, 9L));
        assertEquals(expected, config.fixedStreamingSequenceAsSet());
    }

    /**
     * Tests that a valid scramble configuration correctly parses the
     * available blocks string into a set.
     */
    @Test
    void testValidConfigWithScramble() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "[10-12], 99", 5, "");

        LinkedHashSet<Long> expected = new LinkedHashSet<>(Set.of(10L, 11L, 12L, 99L));
        assertEquals(expected, config.availableBlocksAsSet());
    }

    /**
     * Tests that the builder correctly constructs an {@link UnorderedStreamConfig}
     * object with all provided parameters.
     */
    @Test
    void testBuilderCreatesCorrectObject() {
        UnorderedStreamConfig config = UnorderedStreamConfig.builder()
                .enabled(true)
                .availableBlocks("3, [5-6]")
                .sequenceScrambleLevel(2)
                .fixedStreamingSequence("1, 2")
                .build();

        assertTrue(config.enabled());
        assertEquals(2, config.sequenceScrambleLevel());
        assertEquals("1, 2", config.fixedStreamingSequence());

        LinkedHashSet<Long> expectedAvailable = new LinkedHashSet<>(Set.of(3L, 5L, 6L));
        assertEquals(expectedAvailable, config.availableBlocksAsSet());
    }

    /**
     * Tests that the fixed streaming sequence retains insertion order.
     */
    @Test
    void testFixedStreamingSequencePreservesOrder() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "", 0, "3, 1, 2");
        LinkedHashSet<Long> expected = new LinkedHashSet<>();
        expected.add(3L);
        expected.add(1L);
        expected.add(2L);
        assertEquals(expected, config.fixedStreamingSequenceAsSet());
    }

    /**
     * Tests that duplicate values in the fixed sequence are removed
     * but the original order of first occurrence is preserved.
     */
    @Test
    void testDuplicateValuesAreIgnoredButOrderIsPreserved() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "", 0, "3, 1, 2, 3, 2");
        LinkedHashSet<Long> expected = new LinkedHashSet<>();
        expected.add(3L);
        expected.add(1L);
        expected.add(2L);
        assertEquals(expected, config.fixedStreamingSequenceAsSet());
    }

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
     * Ensures that malformed block input strings throw {@link IllegalArgumentException}
     * when parsed.
     */
    @Test
    void testMalformedInputThrowsException() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "[1-3],, 7", 5, "");
        assertThrows(IllegalArgumentException.class, config::availableBlocksAsSet);
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
     * Ensures that ranges where the start is greater than or equal to the end
     * throw {@link IllegalArgumentException}.
     */
    @Test
    void testRangeWithStartGreaterThanOrEqualToEndThrows() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "[5-3]", 5, "");
        assertThrows(IllegalArgumentException.class, config::availableBlocksAsSet);
    }

    /**
     * Verifies that negative block numbers are not allowed and throw exceptions.
     */
    @Test
    void testNegativeValueThrows() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "-3, 5", 5, "");
        assertThrows(IllegalArgumentException.class, config::availableBlocksAsSet);
    }

    /**
     * Verifies that zero is not an acceptable block number and throws an exception.
     */
    @Test
    void testZeroValueThrows() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "0, 5", 5, "");
        assertThrows(IllegalArgumentException.class, config::availableBlocksAsSet);
    }

    /**
     * Verifies that a range starting at zero is invalid and throws an exception.
     */
    @Test
    void testRangeWithZeroThrows() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "[0-5]", 5, "");
        assertThrows(IllegalArgumentException.class, config::availableBlocksAsSet);
    }
}
