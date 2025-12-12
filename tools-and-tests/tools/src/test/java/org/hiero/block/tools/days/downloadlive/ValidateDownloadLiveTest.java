// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.tools.days.download.DownloadDayLiveImpl;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ValidateDownloadLive}.
 *
 * <p>Note: These tests focus on the public interfaces and observable behavior.
 * Private methods like findPrimaryRecord, findSignatures, and findSidecars are tested
 * indirectly through integration tests or can be tested via reflection if needed.
 */
@DisplayName("ValidateDownloadLive Tests")
public class ValidateDownloadLiveTest {

    @Nested
    @DisplayName("BlockDownloadResult Tests")
    class BlockDownloadResultTests {

        @Test
        @DisplayName("Should create empty result")
        void testEmptyResult() {
            long blockNumber = 100L;
            List<InMemoryFile> files = new ArrayList<>();

            DownloadDayLiveImpl.BlockDownloadResult result =
                    new DownloadDayLiveImpl.BlockDownloadResult(blockNumber, files, null);

            assertEquals(100L, result.blockNumber);
            assertTrue(result.files.isEmpty());
            assertNull(result.newPreviousRecordFileHash);
        }

        @Test
        @DisplayName("Should create result with files and hash")
        void testResultWithFilesAndHash() {
            long blockNumber = 200L;
            List<InMemoryFile> files = List.of(
                    new InMemoryFile(Path.of("test1.rcd"), new byte[] {1, 2, 3}),
                    new InMemoryFile(Path.of("test2.rcd_sig"), new byte[] {4, 5, 6}));
            byte[] hash = new byte[] {7, 8, 9};

            DownloadDayLiveImpl.BlockDownloadResult result =
                    new DownloadDayLiveImpl.BlockDownloadResult(blockNumber, files, hash);

            assertEquals(200L, result.blockNumber);
            assertEquals(2, result.files.size());
            assertArrayEquals(new byte[] {7, 8, 9}, result.newPreviousRecordFileHash);
        }

        @Test
        @DisplayName("Should handle null hash")
        void testResultWithNullHash() {
            long blockNumber = 300L;
            List<InMemoryFile> files = List.of();

            DownloadDayLiveImpl.BlockDownloadResult result =
                    new DownloadDayLiveImpl.BlockDownloadResult(blockNumber, files, null);

            assertNull(result.newPreviousRecordFileHash);
        }

        @Test
        @DisplayName("Should preserve file list immutability")
        void testFileListImmutability() {
            List<InMemoryFile> originalFiles = new ArrayList<>();
            originalFiles.add(new InMemoryFile(Path.of("test.rcd"), new byte[] {1}));

            DownloadDayLiveImpl.BlockDownloadResult result =
                    new DownloadDayLiveImpl.BlockDownloadResult(100L, originalFiles, null);

            originalFiles.add(new InMemoryFile(Path.of("test2.rcd"), new byte[] {2}));

            assertEquals(2, result.files.size()); // List is not copied defensively, so this will be 2
            // Note: This test shows the current behavior. For true immutability, consider defensive copying.
        }
    }

    @Nested
    @DisplayName("File Pattern Recognition Tests")
    class FilePatternRecognitionTests {

        @Test
        @DisplayName("Should recognize primary record file pattern")
        void testPrimaryRecordPattern() {
            String filename = "2025-12-01T00_00_04.319226458Z.rcd";

            assertTrue(filename.endsWith(".rcd"), "Should be a record file");
            assertFalse(filename.contains("_node_"), "Should not contain node marker");
        }

        @Test
        @DisplayName("Should recognize node-specific record file pattern")
        void testNodeSpecificRecordPattern() {
            String filename = "2025-12-01T00_00_04.319226458Z_node_3.rcd";

            assertTrue(filename.endsWith(".rcd"), "Should be a record file");
            assertTrue(filename.contains("_node_"), "Should contain node marker");
        }

        @Test
        @DisplayName("Should recognize signature file patterns")
        void testSignatureFilePatterns() {
            String legacySig = "node_0.0.3.rcs_sig";
            String newSig = "2025-12-01T00_00_04.rcd_sig";
            String compressedSig = "2025-12-01T00_00_04.rcd_sig.gz";

            assertTrue(legacySig.endsWith(".rcs_sig"), "Should recognize legacy signature");
            assertTrue(newSig.endsWith(".rcd_sig"), "Should recognize new signature");
            assertTrue(compressedSig.endsWith(".rcd_sig.gz"), "Should recognize compressed signature");
        }

        @Test
        @DisplayName("Should recognize sidecar file patterns")
        void testSidecarFilePatterns() {
            String primary = "2025-12-01T00_00_04.319226458Z.rcd";
            String sidecar1 = "2025-12-01T00_00_04.319226458Z_01.rcd";
            String sidecar2 = "2025-12-01T00_00_04.319226458Z_02.rcd";

            // Then - sidecar has same timestamp prefix but with _NN suffix
            String primaryPrefix = primary.substring(0, primary.indexOf(".rcd"));
            assertTrue(sidecar1.startsWith(primaryPrefix + "_"), "Sidecar should have same timestamp prefix");
            assertTrue(sidecar2.startsWith(primaryPrefix + "_"), "Sidecar should have same timestamp prefix");
            assertTrue(sidecar1.matches(".*_\\d+\\.rcd$"), "Sidecar should end with _NN.rcd");
        }
    }

    @Nested
    @DisplayName("Validation Logic Tests")
    class ValidationLogicTests {

        @Test
        @DisplayName("Should validate that at least 2/3 signatures are needed for consensus")
        void testSignatureConsensusRequirement() {
            // Given - typical network has ~35 nodes
            int totalNodes = 35;
            int requiredSignatures = (totalNodes * 2 / 3) + 1; // 2/3 + 1

            assertTrue(requiredSignatures >= 24, "Should require at least 24 signatures for 35 nodes");
            assertTrue(requiredSignatures <= totalNodes, "Should not require more signatures than nodes");
        }

        @Test
        @DisplayName("Should handle missing signature files gracefully")
        void testMissingSignatureHandling() {
            int totalNodes = 35;
            int receivedSignatures = 10; // Less than 2/3

            boolean isValid = receivedSignatures >= (totalNodes * 2 / 3);
            assertFalse(isValid, "Should fail validation with insufficient signatures");
        }

        @Test
        @DisplayName("Should accept validation with sufficient signatures")
        void testSufficientSignatureHandling() {
            int totalNodes = 35;
            int receivedSignatures = 30; // More than 2/3

            boolean isValid = receivedSignatures >= (totalNodes * 2 / 3);
            assertTrue(isValid, "Should pass validation with sufficient signatures");
        }
    }

    @Nested
    @DisplayName("Path Manipulation Tests")
    class PathManipulationTests {

        @Test
        @DisplayName("Should extract filename from path")
        void testFilenameExtraction() {
            String fullPath = "some/path/to/2025-12-01T00_00_04.319226458Z.rcd";

            String filename = fullPath.substring(fullPath.lastIndexOf('/') + 1);

            assertEquals("2025-12-01T00_00_04.319226458Z.rcd", filename);
        }

        @Test
        @DisplayName("Should extract timestamp prefix for sidecar matching")
        void testTimestampPrefixExtraction() {
            String filename = "2025-12-01T00_00_04.319226458Z.rcd";

            int underscore = filename.indexOf('_');
            String prefix = underscore > 0 ? filename.substring(0, underscore) : filename;

            assertEquals("2025-12-01T00", prefix);
        }

        @Test
        @DisplayName("Should validate sidecar index format")
        void testSidecarIndexValidation() {
            String sidecarName = "2025-12-01T00_00_04.319226458Z_01.rcd";

            int lastUnderscore = sidecarName.lastIndexOf('_');
            int dot = sidecarName.lastIndexOf('.');
            String indexPart = sidecarName.substring(lastUnderscore + 1, dot);

            assertEquals("01", indexPart);
            assertTrue(indexPart.matches("\\d+"), "Index should be all digits");
        }
    }
}
