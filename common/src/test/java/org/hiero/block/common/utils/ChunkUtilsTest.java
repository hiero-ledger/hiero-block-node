// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ChunkUtilsTest {

    @Test
    public void testEmptyCollection() {
        List<Integer> emptyList = Collections.emptyList();
        List<List<Integer>> chunks = ChunkUtils.chunkify(emptyList, 3);
        assertTrue(chunks.isEmpty(), "Chunks of empty collection should be empty");
    }

    @Test
    public void testChunkSizeZero() {
        List<Integer> list = Arrays.asList(1, 2, 3);
        Exception exception = assertThrows(IllegalArgumentException.class, () -> ChunkUtils.chunkify(list, 0));
        assertEquals("Chunk size must be greater than 0", exception.getMessage());
    }

    @Test
    public void testChunkSizeNegative() {
        List<Integer> list = Arrays.asList(1, 2, 3);
        Exception exception = assertThrows(IllegalArgumentException.class, () -> ChunkUtils.chunkify(list, -1));
        assertEquals("Chunk size must be greater than 0", exception.getMessage());
    }

    @Test
    public void testChunkifyCollectionSmallerThanChunkSize() {
        List<Integer> list = Arrays.asList(1, 2);
        List<List<Integer>> chunks = ChunkUtils.chunkify(list, 5);
        assertEquals(1, chunks.size(), "Should return one chunk");
        assertEquals(list, chunks.get(0), "Chunk should contain the entire collection");
    }

    @Test
    public void testChunkifyCollectionExactlyDivisible() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        List<List<Integer>> chunks = ChunkUtils.chunkify(list, 2);
        assertEquals(2, chunks.size(), "Should return two chunks");
        assertEquals(Arrays.asList(1, 2), chunks.get(0), "First chunk mismatch");
        assertEquals(Arrays.asList(3, 4), chunks.get(1), "Second chunk mismatch");
    }

    @Test
    public void testChunkifyCollectionNotExactlyDivisible() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        List<List<Integer>> chunks = ChunkUtils.chunkify(list, 2);
        assertEquals(3, chunks.size(), "Should return three chunks");
        assertEquals(Arrays.asList(1, 2), chunks.get(0), "First chunk mismatch");
        assertEquals(Arrays.asList(3, 4), chunks.get(1), "Second chunk mismatch");
        assertEquals(Arrays.asList(5), chunks.get(2), "Third chunk mismatch");
    }

    @Test
    public void testNullCollection() {
        assertThrows(NullPointerException.class, () -> ChunkUtils.chunkify(null, 3));
    }
}
