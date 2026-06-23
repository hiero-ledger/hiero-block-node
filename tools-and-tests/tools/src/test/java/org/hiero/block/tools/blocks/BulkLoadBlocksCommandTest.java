// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/** Unit tests for {@link BulkLoadBlocksCommand}. */
class BulkLoadBlocksCommandTest {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    Path sourceDir;
    Path destDir;

    @BeforeEach
    void setUp() throws IOException {
        sourceDir = tempDir.resolve("source");
        destDir = tempDir.resolve("dest");
        Files.createDirectories(sourceDir);
        Files.createDirectories(destDir);
    }

    @Nested
    @DisplayName("Command Line Interface")
    class CommandLineInterface {

        @Test
        @DisplayName("Should show help when --help is specified")
        void helpOption() {
            StringWriter out = new StringWriter();
            CommandLine cmd = new CommandLine(new BulkLoadBlocksCommand());
            cmd.setOut(new PrintWriter(out));

            int exitCode = cmd.execute("--help");

            assertEquals(0, exitCode);
            String output = out.toString();
            assertTrue(output.contains("bulk-load"));
            assertTrue(output.contains("--source"));
            assertTrue(output.contains("--dest"));
        }

        @Test
        @DisplayName("Should fail when source directory is missing")
        void missingSourceDirectory() {
            Path nonExistent = tempDir.resolve("nonexistent");

            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", nonExistent.toString(), "--dest", destDir.toString());

            assertEquals(1, exitCode);
        }

        @Test
        @DisplayName("Should create destination directory if it doesn't exist")
        void createsDestinationDirectory() {
            Path newDest = tempDir.resolve("newdest");

            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", newDest.toString());

            assertEquals(0, exitCode);
            assertTrue(Files.isDirectory(newDest));
        }
    }

    @Nested
    @DisplayName("File Copying")
    class FileCopying {

        @Test
        @DisplayName("Should copy zip files from source to destination")
        void copiesZipFiles() throws IOException {
            // Create test zip files with directory structure
            Path srcSubdir = sourceDir.resolve("000/123");
            Files.createDirectories(srcSubdir);
            Path zipFile1 = srcSubdir.resolve("00000s.zip");
            Path zipFile2 = srcSubdir.resolve("10000s.zip");
            Files.writeString(zipFile1, "test content 1");
            Files.writeString(zipFile2, "test content 2");

            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString());

            assertEquals(0, exitCode);

            // Verify files were copied with same directory structure
            Path destFile1 = destDir.resolve("000/123/00000s.zip");
            Path destFile2 = destDir.resolve("000/123/10000s.zip");
            assertTrue(Files.exists(destFile1));
            assertTrue(Files.exists(destFile2));
            assertEquals("test content 1", Files.readString(destFile1));
            assertEquals("test content 2", Files.readString(destFile2));
        }

        @Test
        @DisplayName("Should skip non-zip files")
        void skipsNonZipFiles() throws IOException {
            Path txtFile = sourceDir.resolve("test.txt");
            Path jsonFile = sourceDir.resolve("test.json");
            Files.writeString(txtFile, "text content");
            Files.writeString(jsonFile, "json content");

            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString());

            assertEquals(0, exitCode);
            assertFalse(Files.exists(destDir.resolve("test.txt")));
            assertFalse(Files.exists(destDir.resolve("test.json")));
        }

        @Test
        @DisplayName("Should skip staging, links, and zipwork directories")
        void skipsInternalDirectories() throws IOException {
            Path stagingDir = sourceDir.resolve("staging");
            Path linksDir = sourceDir.resolve("links");
            Path zipworkDir = sourceDir.resolve("zipwork");
            Files.createDirectories(stagingDir);
            Files.createDirectories(linksDir);
            Files.createDirectories(zipworkDir);

            Files.writeString(stagingDir.resolve("test.zip"), "staging content");
            Files.writeString(linksDir.resolve("test.zip"), "links content");
            Files.writeString(zipworkDir.resolve("test.zip"), "zipwork content");

            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString());

            assertEquals(0, exitCode);
            assertFalse(Files.exists(destDir.resolve("staging")));
            assertFalse(Files.exists(destDir.resolve("links")));
            assertFalse(Files.exists(destDir.resolve("zipwork")));
        }

        @Test
        @DisplayName("Should skip already-existing files with same size")
        void skipsExistingFiles() throws IOException {
            Path zipFile = sourceDir.resolve("00000s.zip");
            Files.writeString(zipFile, "test content");

            // First run - copy the file
            int exitCode1 = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString());
            assertEquals(0, exitCode1);

            Path destFile = destDir.resolve("00000s.zip");
            assertTrue(Files.exists(destFile));
            long originalModifiedTime = Files.getLastModifiedTime(destFile).toMillis();

            // Wait a bit to ensure modified time would change
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Second run - should skip the file
            int exitCode2 = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString());
            assertEquals(0, exitCode2);

            // File should not have been recopied (modified time unchanged)
            long newModifiedTime = Files.getLastModifiedTime(destFile).toMillis();
            assertEquals(originalModifiedTime, newModifiedTime);
        }
    }

    @Nested
    @DisplayName("State File Management")
    class StateFileManagement {

        @Test
        @DisplayName("Should create state file after successful copy")
        void createsStateFile() throws IOException {
            Path zipFile = sourceDir.resolve("00000s.zip");
            Files.writeString(zipFile, "test content");

            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString());

            assertEquals(0, exitCode);

            Path stateFile = destDir.resolve("historic-plugin-bulk-load-state.json");
            assertTrue(Files.exists(stateFile));

            BulkLoadBlocksCommand.LoadState state =
                    JSON_MAPPER.readValue(stateFile.toFile(), BulkLoadBlocksCommand.LoadState.class);
            assertNotNull(state);
            assertEquals(sourceDir.toAbsolutePath().toString(), state.sourceDir);
            assertEquals(destDir.toAbsolutePath().toString(), state.destDir);
            assertEquals(1, state.totalFilesCopied);
            assertTrue(state.copiedFiles.contains("00000s.zip"));
            assertEquals(9999, state.lastCopiedBlock); // 00000s.zip contains blocks 0-9999
        }

        @Test
        @DisplayName("Should resume from state file on subsequent runs")
        void resumesFromStateFile() throws IOException {
            // Create multiple zip files
            Files.writeString(sourceDir.resolve("00000s.zip"), "content 1");
            Files.writeString(sourceDir.resolve("10000s.zip"), "content 2");
            Files.writeString(sourceDir.resolve("20000s.zip"), "content 3");

            // First run - copy first file only by manually creating state
            BulkLoadBlocksCommand.LoadState initialState = new BulkLoadBlocksCommand.LoadState();
            initialState.sourceDir = sourceDir.toAbsolutePath().toString();
            initialState.destDir = destDir.toAbsolutePath().toString();
            initialState.copiedFiles = new HashSet<>(List.of("00000s.zip"));
            initialState.lastCopiedBlock = 9999;
            initialState.totalFilesCopied = 1;
            initialState.totalBytesCopied = 100;

            Path stateFile = destDir.resolve("historic-plugin-bulk-load-state.json");
            JSON_MAPPER.writeValue(stateFile.toFile(), initialState);

            // Also copy the first file to destination
            Files.writeString(destDir.resolve("00000s.zip"), "content 1");

            // Second run - should only copy remaining files
            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString());

            assertEquals(0, exitCode);

            // Verify state was updated
            BulkLoadBlocksCommand.LoadState finalState =
                    JSON_MAPPER.readValue(stateFile.toFile(), BulkLoadBlocksCommand.LoadState.class);
            assertEquals(3, finalState.totalFilesCopied);
            assertTrue(finalState.copiedFiles.contains("10000s.zip"));
            assertTrue(finalState.copiedFiles.contains("20000s.zip"));
            assertEquals(29999, finalState.lastCopiedBlock); // 20000s.zip contains blocks 20000-29999
        }

        @Test
        @DisplayName("Should track last copied block number correctly")
        void tracksLastCopiedBlock() throws IOException {
            Files.writeString(sourceDir.resolve("00000s.zip"), "content 0");
            Files.writeString(sourceDir.resolve("50000s.zip"), "content 50k");
            Files.writeString(sourceDir.resolve("100000s.zip"), "content 100k");

            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString());

            assertEquals(0, exitCode);

            Path stateFile = destDir.resolve("historic-plugin-bulk-load-state.json");
            BulkLoadBlocksCommand.LoadState state =
                    JSON_MAPPER.readValue(stateFile.toFile(), BulkLoadBlocksCommand.LoadState.class);

            // Last copied block should be the last block in 100000s.zip (100000 + 9999)
            assertEquals(109999, state.lastCopiedBlock);
        }
    }

    @Nested
    @DisplayName("Start Block Filtering")
    class StartBlockFiltering {

        @Test
        @DisplayName("Should skip zip files before start block")
        void skipsFilesBeforeStartBlock() throws IOException {
            // Create zip files for blocks 0-9999, 10000-19999, 20000-29999
            Files.writeString(sourceDir.resolve("00000s.zip"), "content 0");
            Files.writeString(sourceDir.resolve("10000s.zip"), "content 10k");
            Files.writeString(sourceDir.resolve("20000s.zip"), "content 20k");

            // Start from block 15000 (should skip 00000s.zip, copy others)
            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString(), "--start-block", "15000");

            assertEquals(0, exitCode);

            assertFalse(Files.exists(destDir.resolve("00000s.zip")));
            assertTrue(Files.exists(destDir.resolve("10000s.zip")));
            assertTrue(Files.exists(destDir.resolve("20000s.zip")));
        }

        @Test
        @DisplayName("Should use state's last block over manual start block if higher")
        void usesHigherOfStateAndManualStartBlock() throws IOException {
            Files.writeString(sourceDir.resolve("00000s.zip"), "content 0");
            Files.writeString(sourceDir.resolve("10000s.zip"), "content 10k");
            Files.writeString(sourceDir.resolve("20000s.zip"), "content 20k");

            // Create state showing we've already copied up to block 19999
            BulkLoadBlocksCommand.LoadState state = new BulkLoadBlocksCommand.LoadState();
            state.sourceDir = sourceDir.toAbsolutePath().toString();
            state.destDir = destDir.toAbsolutePath().toString();
            state.copiedFiles = new HashSet<>(List.of("00000s.zip", "10000s.zip"));
            state.lastCopiedBlock = 19999;
            state.totalFilesCopied = 2;

            Path stateFile = destDir.resolve("historic-plugin-bulk-load-state.json");
            JSON_MAPPER.writeValue(stateFile.toFile(), state);

            // Try to start from block 5000 (state's 19999 is higher, so should start from 20000)
            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString(), "--start-block", "5000");

            assertEquals(0, exitCode);

            // Should only copy the last zip
            assertFalse(Files.exists(destDir.resolve("00000s.zip")));
            assertFalse(Files.exists(destDir.resolve("10000s.zip")));
            assertTrue(Files.exists(destDir.resolve("20000s.zip")));
        }
    }

    @Nested
    @DisplayName("Dry Run Mode")
    class DryRunMode {

        @Test
        @DisplayName("Should not copy files in dry-run mode")
        void doesNotCopyInDryRun() throws IOException {
            Path zipFile = sourceDir.resolve("00000s.zip");
            Files.writeString(zipFile, "test content");

            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString(), "--dry-run");

            assertEquals(0, exitCode);

            // File should not have been copied
            assertFalse(Files.exists(destDir.resolve("00000s.zip")));

            // State file should not have been created
            assertFalse(Files.exists(destDir.resolve("historic-plugin-bulk-load-state.json")));
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("Should handle empty source directory")
        void handlesEmptySourceDirectory() {
            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString());

            assertEquals(0, exitCode);
        }

        @Test
        @DisplayName("Should handle zip files with non-standard names")
        void handlesNonStandardZipNames() throws IOException {
            Path normalZip = sourceDir.resolve("00000s.zip");
            Path weirdZip = sourceDir.resolve("weird-name.zip");
            Files.writeString(normalZip, "normal content");
            Files.writeString(weirdZip, "weird content");

            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString());

            assertEquals(0, exitCode);

            // Both should be copied
            assertTrue(Files.exists(destDir.resolve("00000s.zip")));
            assertTrue(Files.exists(destDir.resolve("weird-name.zip")));

            // But only the standard-named one should update lastCopiedBlock
            Path stateFile = destDir.resolve("historic-plugin-bulk-load-state.json");
            BulkLoadBlocksCommand.LoadState state =
                    JSON_MAPPER.readValue(stateFile.toFile(), BulkLoadBlocksCommand.LoadState.class);
            assertEquals(9999, state.lastCopiedBlock); // Only from 00000s.zip
        }

        @Test
        @DisplayName("Should handle corrupt state file gracefully")
        void handlesCorruptStateFile() throws IOException {
            Path stateFile = destDir.resolve("historic-plugin-bulk-load-state.json");
            Files.writeString(stateFile, "{ corrupt json");

            Path zipFile = sourceDir.resolve("00000s.zip");
            Files.writeString(zipFile, "test content");

            // Should not fail, just start fresh
            int exitCode = new CommandLine(new BulkLoadBlocksCommand())
                    .execute("--source", sourceDir.toString(), "--dest", destDir.toString());

            assertEquals(0, exitCode);
            assertTrue(Files.exists(destDir.resolve("00000s.zip")));
        }
    }
}
