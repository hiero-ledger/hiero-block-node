// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

/**
 * Tests for {@link DayBlockWrapper} covering watermark operations, validation,
 * and state independence from download state.
 */
@Execution(ExecutionMode.SAME_THREAD)
class DayBlockWrapperTest {

    @TempDir
    Path tempDir;

    // ===== Watermark operations (reused from DayBlockWrapper static methods) =====

    @Nested
    @DisplayName("Watermark file operations")
    class WatermarkTests {

        @Test
        @DisplayName("loadWatermark returns -1 for missing file")
        void testLoadWatermarkMissingFile() {
            long result = DayBlockWrapper.loadWatermark(tempDir.resolve("nonexistent.bin"));
            assertEquals(-1, result);
        }

        @Test
        @DisplayName("saveWatermark + loadWatermark round-trips correctly")
        void testWatermarkRoundTrip() {
            Path wf = tempDir.resolve("wrap-commit.bin");
            DayBlockWrapper.saveWatermark(wf, 42L);
            assertEquals(42L, DayBlockWrapper.loadWatermark(wf));
        }

        @Test
        @DisplayName("saveWatermark overwrites previous value")
        void testWatermarkOverwrite() {
            Path wf = tempDir.resolve("wrap-commit.bin");
            DayBlockWrapper.saveWatermark(wf, 100L);
            DayBlockWrapper.saveWatermark(wf, 200L);
            assertEquals(200L, DayBlockWrapper.loadWatermark(wf));
        }

        @Test
        @DisplayName("saveWatermark with -1 is a no-op")
        void testWatermarkNegativeIsNoop() {
            Path wf = tempDir.resolve("wrap-commit.bin");
            DayBlockWrapper.saveWatermark(wf, -1);
            assertFalse(Files.exists(wf));
        }

        @Test
        @DisplayName("loadWatermark returns -1 for truncated file")
        void testLoadWatermarkTruncatedFile() throws IOException {
            Path wf = tempDir.resolve("wrap-commit.bin");
            Files.write(wf, new byte[3]); // too short for a long
            assertEquals(-1, DayBlockWrapper.loadWatermark(wf));
        }

        @Test
        @DisplayName("saveWatermark is atomic (tmp file cleaned up)")
        void testWatermarkAtomicity() {
            Path wf = tempDir.resolve("wrap-commit.bin");
            DayBlockWrapper.saveWatermark(wf, 999L);
            assertFalse(Files.exists(tempDir.resolve("wrap-commit.bin.tmp")));
            assertTrue(Files.exists(wf));
        }
    }

    // ===== validateDayArchive tests =====

    @Nested
    @DisplayName("validateDayArchive")
    class ValidateDayArchiveTests {

        @Test
        @DisplayName("Throws for non-existent archive")
        void testThrowsForNonExistentArchive() {
            Path archive = tempDir.resolve("missing.tar.zstd");
            DayBlockInfo info = new DayBlockInfo(2025, 3, 15, 0, null, 99, null);

            assertThrows(IllegalStateException.class, () -> DayBlockWrapper.validateDayArchive(archive, info));
        }

        @Test
        @DisplayName("Throws for corrupt archive (empty file)")
        void testThrowsForCorruptArchive() throws IOException {
            Path archive = tempDir.resolve("corrupt.tar.zstd");
            Files.write(archive, new byte[] {0, 1, 2, 3}); // Not valid tar.zstd
            DayBlockInfo info = new DayBlockInfo(2025, 3, 15, 0, null, 99, null);

            IllegalStateException ex =
                    assertThrows(IllegalStateException.class, () -> DayBlockWrapper.validateDayArchive(archive, info));
            assertTrue(ex.getMessage().contains("FATAL"));
            assertTrue(ex.getMessage().contains("corrupt"));
        }
    }

    // ===== State independence tests =====

    @Nested
    @DisplayName("State independence")
    class StateIndependenceTests {

        @Test
        @DisplayName("Wrap state directory is independent of download state directory")
        void testWrapStateIsIndependent() throws IOException {
            Path downloadDir = tempDir.resolve("downloadState");
            Path wrapDir = tempDir.resolve("wrapState");
            Files.createDirectories(downloadDir);
            Files.createDirectories(wrapDir);

            // Create download state file
            Files.writeString(downloadDir.resolve("validateCmdStatus.json"), "{\"blockNumber\": 100}");

            // Create wrap state file
            DayBlockWrapper.saveWatermark(wrapDir.resolve("wrap-commit.bin"), 50L);

            // Verify state files are in separate directories
            assertTrue(Files.exists(downloadDir.resolve("validateCmdStatus.json")));
            assertTrue(Files.exists(wrapDir.resolve("wrap-commit.bin")));
            assertFalse(Files.exists(downloadDir.resolve("wrap-commit.bin")));
            assertFalse(Files.exists(wrapDir.resolve("validateCmdStatus.json")));
        }

        @Test
        @DisplayName("Wrap watermark and download state track independently")
        void testWrapAndDownloadStateAreIndependent() throws IOException {
            Path downloadDir = tempDir.resolve("download");
            Path wrapDir = tempDir.resolve("wrap");
            Files.createDirectories(downloadDir);
            Files.createDirectories(wrapDir);

            // Download state at block 200
            Files.writeString(downloadDir.resolve("validateCmdStatus.json"), "{\"blockNumber\": 200}");

            // Wrap state at block 100
            DayBlockWrapper.saveWatermark(wrapDir.resolve("wrap-commit.bin"), 100L);

            // Modifying one doesn't affect the other
            DayBlockWrapper.saveWatermark(wrapDir.resolve("wrap-commit.bin"), 150L);
            assertEquals(150L, DayBlockWrapper.loadWatermark(wrapDir.resolve("wrap-commit.bin")));

            // Download state unchanged
            String downloadState = Files.readString(downloadDir.resolve("validateCmdStatus.json"));
            assertTrue(downloadState.contains("200"));
        }
    }

    // ===== Checkpoint save/restore tests =====

    @Nested
    @DisplayName("Checkpoint save/restore")
    class CheckpointTests {

        @Test
        @DisplayName("Watermark persists across separate load calls")
        void testWatermarkPersistsAcrossLoads() {
            Path wf = tempDir.resolve("wrap-commit.bin");
            DayBlockWrapper.saveWatermark(wf, 42L);

            // Simulate process restart by loading again
            long restored = DayBlockWrapper.loadWatermark(wf);
            assertEquals(42L, restored);

            // Update and verify again
            DayBlockWrapper.saveWatermark(wf, 100L);
            assertEquals(100L, DayBlockWrapper.loadWatermark(wf));
        }

        @Test
        @DisplayName("Block zero watermark is saved correctly")
        void testBlockZeroWatermark() {
            Path wf = tempDir.resolve("wrap-commit.bin");
            DayBlockWrapper.saveWatermark(wf, 0L);
            assertEquals(0L, DayBlockWrapper.loadWatermark(wf));
        }
    }
}
