// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class UnorderedStreamConfigTest {

    @Test
    void testValidConfigWithSequenceScrambleLevel0() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "", 0, "[1-3], 7, 9");

        LinkedHashSet<Long> expected = new LinkedHashSet<>(Set.of(1L, 2L, 3L, 7L, 9L));
        assertEquals(expected, config.fixedStreamingSequenceAsSet());
    }

    @Test
    void testValidConfigWithScramble() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "[10-12], 99", 5, "");

        LinkedHashSet<Long> expected = new LinkedHashSet<>(Set.of(10L, 11L, 12L, 99L));
        assertEquals(expected, config.availableBlocksAsSet());
    }

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

    @Test
    void testFixedStreamingSequencePreservesOrder() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "", 0, "3, 1, 2");
        LinkedHashSet<Long> expected = new LinkedHashSet<>();
        expected.add(3L);
        expected.add(1L);
        expected.add(2L);
        assertEquals(expected, config.fixedStreamingSequenceAsSet());
    }

    @Test
    void testDuplicateValuesAreIgnoredButOrderIsPreserved() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "", 0, "3, 1, 2, 3, 2");
        LinkedHashSet<Long> expected = new LinkedHashSet<>();
        expected.add(3L);
        expected.add(1L);
        expected.add(2L);
        assertEquals(expected, config.fixedStreamingSequenceAsSet());
    }

    @Test
    void testInvalidScrambleLevelInvalid() {
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, "[1-3]", -1, ""));
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, "[1-3]", 11, ""));
    }

    @Test
    void testMissingFixedStreamingSequenceWhenLevel0() {
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, "", 0, "   "));
    }

    @Test
    void testMissingAvailableBlocksWhenScrambleEnabled() {
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, "   ", 5, ""));
    }

    @Test
    void testMalformedInputThrowsException() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "[1-3],, 7", 5, "");
        assertThrows(IllegalArgumentException.class, config::availableBlocksAsSet);
    }

    @Test
    void testAvailableBlocksEmptyInputThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new UnorderedStreamConfig(true, " ", 5, ""));
    }

    @Test
    void testRangeWithStartGreaterThanOrEqualToEndThrows() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "[5-3]", 5, "");
        assertThrows(IllegalArgumentException.class, config::availableBlocksAsSet);
    }

    @Test
    void testNegativeValueThrows() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "-3, 5", 5, "");
        assertThrows(IllegalArgumentException.class, config::availableBlocksAsSet);
    }

    @Test
    void testZeroValueThrows() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "0, 5", 5, "");
        assertThrows(IllegalArgumentException.class, config::availableBlocksAsSet);
    }

    @Test
    void testRangeWithZeroThrows() {
        UnorderedStreamConfig config = new UnorderedStreamConfig(true, "[0-5]", 5, "");
        assertThrows(IllegalArgumentException.class, config::availableBlocksAsSet);
    }
}
