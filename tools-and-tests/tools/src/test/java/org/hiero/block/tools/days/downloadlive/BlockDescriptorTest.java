// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BlockDescriptor}.
 */
@DisplayName("BlockDescriptor Tests")
public class BlockDescriptorTest {

    @Test
    @DisplayName("Should create BlockDescriptor with all fields")
    void testConstructor() {
        long blockNumber = 12345L;
        String filename = "2025-12-01T00_00_04.319226458Z.rcd";
        String timestampIso = "2025-12-01T00:00:04.319226458Z";
        String expectedHash = "0x1234567890abcdef";

        BlockDescriptor descriptor = new BlockDescriptor(blockNumber, filename, timestampIso, expectedHash);

        assertEquals(blockNumber, descriptor.getBlockNumber());
        assertEquals(filename, descriptor.getFilename());
        assertEquals(timestampIso, descriptor.getTimestampIso());
        assertEquals(expectedHash, descriptor.getExpectedHash());
    }

    @Test
    @DisplayName("Should handle null expected hash")
    void testConstructorWithNullHash() {
        long blockNumber = 12345L;
        String filename = "test.rcd";
        String timestampIso = "2025-12-01T00:00:04.319226458Z";

        BlockDescriptor descriptor = new BlockDescriptor(blockNumber, filename, timestampIso, null);

        assertNull(descriptor.getExpectedHash());
        assertEquals(blockNumber, descriptor.getBlockNumber());
    }

    @Test
    @DisplayName("Should produce readable toString output")
    void testToString() {
        long blockNumber = 12345L;
        String filename = "test.rcd";
        String timestampIso = "2025-12-01T00:00:04.319226458Z";
        String expectedHash = "0xabc";

        BlockDescriptor descriptor = new BlockDescriptor(blockNumber, filename, timestampIso, expectedHash);
        String result = descriptor.toString();

        assertTrue(result.contains("12345"), "Should contain block number");
        assertTrue(result.contains("test.rcd"), "Should contain filename");
        assertTrue(result.contains("2025-12-01T00:00:04.319226458Z"), "Should contain timestamp");
    }

    @Test
    @DisplayName("Should handle large block numbers")
    void testLargeBlockNumber() {
        long largeBlockNumber = Long.MAX_VALUE;

        BlockDescriptor descriptor = new BlockDescriptor(largeBlockNumber, "file.rcd", "2025-12-01T00:00:00Z", "0x123");

        assertEquals(largeBlockNumber, descriptor.getBlockNumber());
    }

    @Test
    @DisplayName("Should handle zero block number")
    void testZeroBlockNumber() {
        long zeroBlockNumber = 0L;

        BlockDescriptor descriptor = new BlockDescriptor(zeroBlockNumber, "genesis.rcd", "2019-09-13T00:00:00Z", null);

        assertEquals(0L, descriptor.getBlockNumber());
    }
}
