// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.InMemoryTreeHasher;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.utils.Sha384;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link HasherStateFiles} covering the atomic save, fallback load, and
 * hasher reconciliation patterns used by {@code ToWrappedBlocksCommand}.
 *
 * <p>No real block data is required: tests use synthetic 48-byte hashes created by
 * {@link #fakeHash(int)}.
 */
@DisplayName("HasherStateFiles")
class HasherStateFilesTest {

    @TempDir
    Path tempDir;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Creates a deterministic 48-byte block hash filled with {@code value}.
     * Good enough to exercise Merkle tree logic without real block data.
     */
    private static byte[] fakeHash(int value) {
        byte[] hash = new byte[Sha384.SHA_384_HASH_SIZE];
        Arrays.fill(hash, (byte) value);
        return hash;
    }

    // -------------------------------------------------------------------------
    // saveAtomically
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("saveAtomically")
    class SaveAtomicallyTests {

        @Test
        @DisplayName("First save creates the primary file with no backup or temp leftover")
        void firstSave_createsPrimaryOnly() throws Exception {
            StreamingHasher hasher = new StreamingHasher();
            hasher.addNodeByHash(fakeHash(1));
            hasher.addNodeByHash(fakeHash(2));

            Path primary = tempDir.resolve("hasher.bin");
            HasherStateFiles.saveAtomically(primary, hasher::save);

            assertTrue(Files.exists(primary), "Primary file should exist");
            assertFalse(Files.exists(Path.of(primary + ".bak")), "No .bak on first save");
            assertFalse(Files.exists(Path.of(primary + ".tmp")), ".tmp cleaned up after save");
        }

        @Test
        @DisplayName("Second save rotates previous primary to .bak")
        void secondSave_rotatesBackup() throws Exception {
            StreamingHasher first = new StreamingHasher();
            first.addNodeByHash(fakeHash(1)); // 1 leaf

            StreamingHasher second = new StreamingHasher();
            second.addNodeByHash(fakeHash(1));
            second.addNodeByHash(fakeHash(2)); // 2 leaves

            Path primary = tempDir.resolve("hasher.bin");
            Path bak = Path.of(primary + ".bak");

            HasherStateFiles.saveAtomically(primary, first::save);
            HasherStateFiles.saveAtomically(primary, second::save);

            assertTrue(Files.exists(primary));
            assertTrue(Files.exists(bak), ".bak should exist after second save");

            // Primary holds the second state (2 leaves), .bak holds the first (1 leaf)
            StreamingHasher loadedPrimary = new StreamingHasher();
            loadedPrimary.load(primary);
            assertEquals(2, loadedPrimary.leafCount(), "Primary should have 2 leaves");

            StreamingHasher loadedBak = new StreamingHasher();
            loadedBak.load(bak);
            assertEquals(1, loadedBak.leafCount(), ".bak should have 1 leaf");
        }

        @Test
        @DisplayName("Primary content after save matches the original hasher root hash")
        void savedContent_matchesOriginalRootHash() throws Exception {
            StreamingHasher hasher = new StreamingHasher();
            for (int i = 0; i < 7; i++) {
                hasher.addNodeByHash(fakeHash(i));
            }
            byte[] expectedRoot = hasher.computeRootHash();

            Path primary = tempDir.resolve("hasher.bin");
            HasherStateFiles.saveAtomically(primary, hasher::save);

            StreamingHasher reloaded = new StreamingHasher();
            reloaded.load(primary);
            assertArrayEquals(expectedRoot, reloaded.computeRootHash(), "Root hash should round-trip");
        }
    }

    // -------------------------------------------------------------------------
    // loadWithFallback
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("loadWithFallback")
    class LoadWithFallbackTests {

        @Test
        @DisplayName("Good primary: loads successfully and leaf count matches")
        void goodPrimary_loadsFromPrimary() throws Exception {
            StreamingHasher saved = new StreamingHasher();
            saved.addNodeByHash(fakeHash(1));
            saved.addNodeByHash(fakeHash(2));
            saved.addNodeByHash(fakeHash(3));

            Path primary = tempDir.resolve("hasher.bin");
            HasherStateFiles.saveAtomically(primary, saved::save);

            StreamingHasher loaded = new StreamingHasher();
            HasherStateFiles.loadWithFallback(primary, loaded::load);

            assertEquals(3, loaded.leafCount());
            assertArrayEquals(saved.computeRootHash(), loaded.computeRootHash());
        }

        @Test
        @DisplayName("Missing primary, no backup: hasher stays at zero leaves (starts fresh)")
        void missingPrimary_noBackup_startsFresh() {
            Path primary = tempDir.resolve("hasher.bin");
            // Neither primary nor .bak exists

            StreamingHasher loaded = new StreamingHasher();
            HasherStateFiles.loadWithFallback(primary, loaded::load);

            assertEquals(0, loaded.leafCount(), "Should start fresh when no file exists");
        }

        @Test
        @DisplayName("Missing primary, good backup: loads from .bak")
        void missingPrimary_goodBackup_loadsFromBackup() throws Exception {
            StreamingHasher saved = new StreamingHasher();
            saved.addNodeByHash(fakeHash(42));

            // Manually create only the .bak file (simulates primary lost after crash between rename steps)
            Path primary = tempDir.resolve("hasher.bin");
            Path bak = Path.of(primary + ".bak");
            saved.save(bak);

            StreamingHasher loaded = new StreamingHasher();
            HasherStateFiles.loadWithFallback(primary, loaded::load);

            assertEquals(1, loaded.leafCount(), "Should load 1 leaf from backup");
            assertArrayEquals(saved.computeRootHash(), loaded.computeRootHash());
        }

        @Test
        @DisplayName("Corrupt primary, good backup: falls back to .bak state")
        void corruptPrimary_goodBackup_loadsFromBackup() throws Exception {
            // First save 2 leaves (will become .bak after the second save)
            StreamingHasher twoLeaf = new StreamingHasher();
            twoLeaf.addNodeByHash(fakeHash(1));
            twoLeaf.addNodeByHash(fakeHash(2));

            // Second save 3 leaves (will be primary)
            StreamingHasher threeLeaf = new StreamingHasher();
            threeLeaf.addNodeByHash(fakeHash(1));
            threeLeaf.addNodeByHash(fakeHash(2));
            threeLeaf.addNodeByHash(fakeHash(3));

            Path primary = tempDir.resolve("hasher.bin");

            HasherStateFiles.saveAtomically(primary, twoLeaf::save); // primary=2, no .bak
            HasherStateFiles.saveAtomically(primary, threeLeaf::save); // primary=3, .bak=2

            // Corrupt the primary file (1 byte — too short for a valid long read)
            Files.write(primary, new byte[] {0x00});

            StreamingHasher loaded = new StreamingHasher();
            HasherStateFiles.loadWithFallback(primary, loaded::load);

            assertEquals(2, loaded.leafCount(), "Should fall back to 2-leaf .bak state");
            assertArrayEquals(twoLeaf.computeRootHash(), loaded.computeRootHash());
        }

        @Test
        @DisplayName("Corrupt primary, no backup: hasher stays at zero leaves (starts fresh)")
        void corruptPrimary_noBackup_startsFresh() throws Exception {
            Path primary = tempDir.resolve("hasher.bin");
            Files.write(primary, new byte[] {0x00}); // too short to parse

            StreamingHasher loaded = new StreamingHasher();
            HasherStateFiles.loadWithFallback(primary, loaded::load);

            assertEquals(0, loaded.leafCount(), "Should start fresh when both files unreadable");
        }

        @Test
        @DisplayName("Corrupt primary, corrupt backup: hasher stays at zero leaves (starts fresh)")
        void corruptPrimary_corruptBackup_startsFresh() throws Exception {
            Path primary = tempDir.resolve("hasher.bin");
            Path bak = Path.of(primary + ".bak");
            Files.write(primary, new byte[] {0x00});
            Files.write(bak, new byte[] {0x00});

            StreamingHasher loaded = new StreamingHasher();
            HasherStateFiles.loadWithFallback(primary, loaded::load);

            assertEquals(0, loaded.leafCount(), "Should start fresh when both files are corrupt");
        }
    }

    // -------------------------------------------------------------------------
    // saveStateCheckpoint (both hashers together)
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("saveStateCheckpoint")
    class SaveStateCheckpointTests {

        @Test
        @DisplayName("Both hashers saved and reloadable with matching root hashes")
        void bothHashersSavedAndReloaded() throws Exception {
            StreamingHasher streaming = new StreamingHasher();
            InMemoryTreeHasher inMemory = new InMemoryTreeHasher();
            for (int i = 0; i < 5; i++) {
                streaming.addNodeByHash(fakeHash(i));
                inMemory.addNodeByHash(fakeHash(i));
            }

            Path streamingFile = tempDir.resolve("streaming.bin");
            Path inMemoryFile = tempDir.resolve("inMemory.bin");

            HasherStateFiles.saveStateCheckpoint(streamingFile, streaming, inMemoryFile, inMemory);

            StreamingHasher restoredStreaming = new StreamingHasher();
            restoredStreaming.load(streamingFile);
            InMemoryTreeHasher restoredInMemory = new InMemoryTreeHasher();
            restoredInMemory.load(inMemoryFile);

            assertEquals(5, restoredStreaming.leafCount());
            assertEquals(5, restoredInMemory.leafCount());
            assertArrayEquals(streaming.computeRootHash(), restoredStreaming.computeRootHash());
            assertArrayEquals(inMemory.computeRootHash(), restoredInMemory.computeRootHash());
        }

        @Test
        @DisplayName("Second checkpoint rotates both .bak files")
        void secondCheckpoint_rotatesBothBaks() throws Exception {
            Path streamingFile = tempDir.resolve("streaming.bin");
            Path inMemoryFile = tempDir.resolve("inMemory.bin");

            // First checkpoint: 3 leaves
            StreamingHasher s1 = new StreamingHasher();
            InMemoryTreeHasher m1 = new InMemoryTreeHasher();
            for (int i = 0; i < 3; i++) {
                s1.addNodeByHash(fakeHash(i));
                m1.addNodeByHash(fakeHash(i));
            }
            HasherStateFiles.saveStateCheckpoint(streamingFile, s1, inMemoryFile, m1);

            // Second checkpoint: 6 leaves
            StreamingHasher s2 = new StreamingHasher();
            InMemoryTreeHasher m2 = new InMemoryTreeHasher();
            for (int i = 0; i < 6; i++) {
                s2.addNodeByHash(fakeHash(i));
                m2.addNodeByHash(fakeHash(i));
            }
            HasherStateFiles.saveStateCheckpoint(streamingFile, s2, inMemoryFile, m2);

            assertTrue(Files.exists(Path.of(streamingFile + ".bak")));
            assertTrue(Files.exists(Path.of(inMemoryFile + ".bak")));

            // .bak holds the first checkpoint (3 leaves)
            StreamingHasher fromBak = new StreamingHasher();
            fromBak.load(Path.of(streamingFile + ".bak"));
            assertEquals(3, fromBak.leafCount());
        }
    }

    // -------------------------------------------------------------------------
    // Hasher reconciliation (replay logic mirrors ToWrappedBlocksCommand.run())
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Hasher reconciliation from registry")
    class HasherReconciliationTests {

        @Test
        @DisplayName("Stale hashers (checkpoint at block 4) catch up to registry (block 9)")
        void staleHashers_catchUpFromRegistry() throws Exception {
            // 10 fake block hashes (blocks 0..9)
            byte[][] hashes = new byte[10][];
            for (int i = 0; i < 10; i++) {
                hashes[i] = fakeHash(i);
            }

            // Reference: all 10 blocks processed
            StreamingHasher refStreaming = new StreamingHasher();
            InMemoryTreeHasher refInMemory = new InMemoryTreeHasher();
            for (byte[] h : hashes) {
                refStreaming.addNodeByHash(h);
                refInMemory.addNodeByHash(h);
            }

            // Checkpoint after 5 blocks (blocks 0..4)
            StreamingHasher checkpoint = new StreamingHasher();
            InMemoryTreeHasher checkpointInMemory = new InMemoryTreeHasher();
            for (int i = 0; i < 5; i++) {
                checkpoint.addNodeByHash(hashes[i]);
                checkpointInMemory.addNodeByHash(hashes[i]);
            }
            Path streamingFile = tempDir.resolve("streaming.bin");
            Path inMemoryFile = tempDir.resolve("inMemory.bin");
            checkpoint.save(streamingFile);
            checkpointInMemory.save(inMemoryFile);

            // Registry contains all 10 blocks (written synchronously per block before checkpoint)
            Path registryFile = tempDir.resolve("registry.bin");
            try (var registry = new BlockStreamBlockHashRegistry(registryFile)) {
                for (int i = 0; i < 10; i++) {
                    registry.addBlock(i, hashes[i]);
                }

                // Load stale hashers from checkpoint
                StreamingHasher restored = new StreamingHasher();
                restored.load(streamingFile);
                InMemoryTreeHasher restoredInMemory = new InMemoryTreeHasher();
                restoredInMemory.load(inMemoryFile);

                // Replay missing blocks — same logic as in ToWrappedBlocksCommand.run()
                final long registryHighest = registry.highestBlockNumberStored();
                for (long bn = restored.leafCount(); bn <= registryHighest; bn++) {
                    restored.addNodeByHash(registry.getBlockHash(bn));
                }
                for (long bn = restoredInMemory.leafCount(); bn <= registryHighest; bn++) {
                    restoredInMemory.addNodeByHash(registry.getBlockHash(bn));
                }

                assertEquals(10, restored.leafCount(), "Streaming hasher should have all 10 leaves");
                assertEquals(10, restoredInMemory.leafCount(), "In-memory hasher should have all 10 leaves");
                assertArrayEquals(
                        refStreaming.computeRootHash(),
                        restored.computeRootHash(),
                        "Streaming root should match fully-built reference");
                assertArrayEquals(
                        refInMemory.computeRootHash(),
                        restoredInMemory.computeRootHash(),
                        "In-memory root should match fully-built reference");
            }
        }

        @Test
        @DisplayName("Hashers already in sync with registry: no replay needed, roots unchanged")
        void hashersInSync_noReplayNeeded() throws Exception {
            byte[][] hashes = new byte[5][];
            for (int i = 0; i < 5; i++) {
                hashes[i] = fakeHash(i);
            }

            StreamingHasher streaming = new StreamingHasher();
            InMemoryTreeHasher inMemory = new InMemoryTreeHasher();
            for (byte[] h : hashes) {
                streaming.addNodeByHash(h);
                inMemory.addNodeByHash(h);
            }
            byte[] expectedStreamingRoot = streaming.computeRootHash();
            byte[] expectedInMemoryRoot = inMemory.computeRootHash();

            Path registryFile = tempDir.resolve("registry.bin");
            try (var registry = new BlockStreamBlockHashRegistry(registryFile)) {
                for (int i = 0; i < 5; i++) {
                    registry.addBlock(i, hashes[i]);
                }

                // Replay with hashers already at leafCount == 5 == registryHighest + 1
                final long registryHighest = registry.highestBlockNumberStored();
                for (long bn = streaming.leafCount(); bn <= registryHighest; bn++) {
                    streaming.addNodeByHash(registry.getBlockHash(bn));
                }
                for (long bn = inMemory.leafCount(); bn <= registryHighest; bn++) {
                    inMemory.addNodeByHash(registry.getBlockHash(bn));
                }

                assertEquals(5, streaming.leafCount(), "Leaf count should be unchanged");
                assertArrayEquals(expectedStreamingRoot, streaming.computeRootHash(), "Root should be unchanged");
                assertArrayEquals(expectedInMemoryRoot, inMemory.computeRootHash(), "Root should be unchanged");
            }
        }

        @Test
        @DisplayName("Hashers have different stale leaf counts: each replays independently")
        void inconsistentHasherLeafCounts_eachCatchesUpIndependently() throws Exception {
            // Simulates a crash that saved streaming.bin (at block 3) but not inMemory.bin
            // so inMemory fell back to its .bak at block 1
            byte[][] hashes = new byte[8][];
            for (int i = 0; i < 8; i++) {
                hashes[i] = fakeHash(i);
            }

            StreamingHasher refStreaming = new StreamingHasher();
            InMemoryTreeHasher refInMemory = new InMemoryTreeHasher();
            for (byte[] h : hashes) {
                refStreaming.addNodeByHash(h);
                refInMemory.addNodeByHash(h);
            }

            // streaming at block 3 (4 leaves), inMemory at block 1 (2 leaves)
            StreamingHasher staleSt = new StreamingHasher();
            for (int i = 0; i < 4; i++) staleSt.addNodeByHash(hashes[i]);
            InMemoryTreeHasher staleIm = new InMemoryTreeHasher();
            for (int i = 0; i < 2; i++) staleIm.addNodeByHash(hashes[i]);

            Path registryFile = tempDir.resolve("registry.bin");
            try (var registry = new BlockStreamBlockHashRegistry(registryFile)) {
                for (int i = 0; i < 8; i++) {
                    registry.addBlock(i, hashes[i]);
                }

                final long registryHighest = registry.highestBlockNumberStored();
                for (long bn = staleSt.leafCount(); bn <= registryHighest; bn++) {
                    staleSt.addNodeByHash(registry.getBlockHash(bn));
                }
                for (long bn = staleIm.leafCount(); bn <= registryHighest; bn++) {
                    staleIm.addNodeByHash(registry.getBlockHash(bn));
                }

                assertEquals(8, staleSt.leafCount());
                assertEquals(8, staleIm.leafCount());
                assertArrayEquals(refStreaming.computeRootHash(), staleSt.computeRootHash(), "Streaming root correct");
                assertArrayEquals(refInMemory.computeRootHash(), staleIm.computeRootHash(), "In-memory root correct");
            }
        }
    }
}
