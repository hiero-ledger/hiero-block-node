// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.hiero.block.tools.utils.Sha384;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link BlockStreamBlockHashRegistry}. */
class BlockStreamBlockHashRegistryTest {

    @TempDir
    Path tempDir;

    /** Create a 48-byte hash filled with the given byte value. */
    private static byte[] fakeHash(int value) {
        byte[] hash = new byte[Sha384.SHA_384_HASH_SIZE];
        Arrays.fill(hash, (byte) value);
        return hash;
    }

    // ===== Constructor / fresh start tests =====

    @Test
    void freshRegistry_hasNoBlocks() throws Exception {
        try (var registry = new BlockStreamBlockHashRegistry(tempDir.resolve("empty.bin"))) {
            assertEquals(-1, registry.highestBlockNumberStored());
            assertArrayEquals(EMPTY_TREE_HASH, registry.mostRecentBlockHash());
        }
    }

    @Test
    void existingEmptyFileDoesNotThrow() throws Exception {
        // Create an empty file first (simulates the bug scenario where file exists but is empty)
        Path emptyFile = tempDir.resolve("existing-empty.bin");
        Files.createFile(emptyFile);
        assertEquals(0, Files.size(emptyFile), "File should be empty");

        // Opening should not throw - this was the bug
        assertDoesNotThrow(() -> {
            try (var registry = new BlockStreamBlockHashRegistry(emptyFile)) {
                assertEquals(-1, registry.highestBlockNumberStored());
                assertArrayEquals(EMPTY_TREE_HASH, registry.mostRecentBlockHash());
            }
        });
    }

    @Test
    void freshRegistry_getBlockHash_throwsForNegative() throws Exception {
        try (var registry = new BlockStreamBlockHashRegistry(tempDir.resolve("empty.bin"))) {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> registry.getBlockHash(-1));
            assertEquals("Block number -1 is out of range. Highest block stored is -1", ex.getMessage());
        }
    }

    @Test
    void freshRegistry_getBlockHash_throwsForZero() throws Exception {
        try (var registry = new BlockStreamBlockHashRegistry(tempDir.resolve("empty.bin"))) {
            assertThrows(IllegalArgumentException.class, () -> registry.getBlockHash(0));
        }
    }

    // ===== addBlock + getBlockHash round-trip tests =====

    @Test
    void addBlock_thenGetBlockHash_returnsCorrectHash() throws Exception {
        byte[] hash0 = fakeHash(0xAA);
        try (var registry = new BlockStreamBlockHashRegistry(tempDir.resolve("test.bin"))) {
            registry.addBlock(0, hash0);
            assertArrayEquals(hash0, registry.getBlockHash(0));
            assertEquals(0, registry.highestBlockNumberStored());
            assertArrayEquals(hash0, registry.mostRecentBlockHash());
        }
    }

    @Test
    void addMultipleBlocks_getBlockHash_returnsEachCorrectly() throws Exception {
        byte[] hash0 = fakeHash(0x01);
        byte[] hash1 = fakeHash(0x02);
        byte[] hash2 = fakeHash(0x03);
        try (var registry = new BlockStreamBlockHashRegistry(tempDir.resolve("test.bin"))) {
            registry.addBlock(0, hash0);
            registry.addBlock(1, hash1);
            registry.addBlock(2, hash2);

            assertArrayEquals(hash0, registry.getBlockHash(0));
            assertArrayEquals(hash1, registry.getBlockHash(1));
            assertArrayEquals(hash2, registry.getBlockHash(2));
            assertEquals(2, registry.highestBlockNumberStored());
            assertArrayEquals(hash2, registry.mostRecentBlockHash());
        }
    }

    // ===== getBlockHash boundary / error tests =====

    @Test
    void getBlockHash_negativeBlockNumber_throws() throws Exception {
        try (var registry = new BlockStreamBlockHashRegistry(tempDir.resolve("test.bin"))) {
            registry.addBlock(0, fakeHash(0x01));
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> registry.getBlockHash(-1));
            assertEquals("Block number -1 is out of range. Highest block stored is 0", ex.getMessage());
        }
    }

    @Test
    void getBlockHash_beyondHighest_throws() throws Exception {
        try (var registry = new BlockStreamBlockHashRegistry(tempDir.resolve("test.bin"))) {
            registry.addBlock(0, fakeHash(0x01));
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> registry.getBlockHash(1));
            assertEquals("Block number 1 is out of range. Highest block stored is 0", ex.getMessage());
        }
    }

    @Test
    void getBlockHash_farBeyondHighest_throws() throws Exception {
        try (var registry = new BlockStreamBlockHashRegistry(tempDir.resolve("test.bin"))) {
            registry.addBlock(0, fakeHash(0x01));
            assertThrows(IllegalArgumentException.class, () -> registry.getBlockHash(100));
        }
    }

    // ===== addBlock ordering tests =====

    @Test
    void addBlock_skippingNumber_throws() throws Exception {
        try (var registry = new BlockStreamBlockHashRegistry(tempDir.resolve("test.bin"))) {
            registry.addBlock(0, fakeHash(0x01));
            assertThrows(IllegalArgumentException.class, () -> registry.addBlock(2, fakeHash(0x02)));
        }
    }

    @Test
    void addBlock_duplicateNumber_throws() throws Exception {
        try (var registry = new BlockStreamBlockHashRegistry(tempDir.resolve("test.bin"))) {
            registry.addBlock(0, fakeHash(0x01));
            assertThrows(IllegalArgumentException.class, () -> registry.addBlock(0, fakeHash(0x02)));
        }
    }

    @Test
    void addBlock_notStartingAtZero_throws() throws Exception {
        try (var registry = new BlockStreamBlockHashRegistry(tempDir.resolve("test.bin"))) {
            assertThrows(IllegalArgumentException.class, () -> registry.addBlock(5, fakeHash(0x05)));
        }
    }

    // ===== Persistence / reload tests =====

    @Test
    void reloadFromFile_restoresBlockHashes() throws Exception {
        Path file = tempDir.resolve("persist.bin");
        byte[] hash0 = fakeHash(0xAA);
        byte[] hash1 = fakeHash(0xBB);
        byte[] hash2 = fakeHash(0xCC);

        // Write blocks
        try (var registry = new BlockStreamBlockHashRegistry(file)) {
            registry.addBlock(0, hash0);
            registry.addBlock(1, hash1);
            registry.addBlock(2, hash2);
        }

        // Reload and verify
        try (var registry = new BlockStreamBlockHashRegistry(file)) {
            assertEquals(2, registry.highestBlockNumberStored());
            assertArrayEquals(hash2, registry.mostRecentBlockHash());
            assertArrayEquals(hash0, registry.getBlockHash(0));
            assertArrayEquals(hash1, registry.getBlockHash(1));
            assertArrayEquals(hash2, registry.getBlockHash(2));
        }
    }

    @Test
    void reloadFromFile_canContinueAddingBlocks() throws Exception {
        Path file = tempDir.resolve("resume.bin");
        byte[] hash0 = fakeHash(0x10);
        byte[] hash1 = fakeHash(0x20);

        try (var registry = new BlockStreamBlockHashRegistry(file)) {
            registry.addBlock(0, hash0);
        }

        // Reopen and add next block
        try (var registry = new BlockStreamBlockHashRegistry(file)) {
            assertEquals(0, registry.highestBlockNumberStored());
            registry.addBlock(1, hash1);
            assertEquals(1, registry.highestBlockNumberStored());
            assertArrayEquals(hash0, registry.getBlockHash(0));
            assertArrayEquals(hash1, registry.getBlockHash(1));
            assertArrayEquals(hash1, registry.mostRecentBlockHash());
        }
    }
}
