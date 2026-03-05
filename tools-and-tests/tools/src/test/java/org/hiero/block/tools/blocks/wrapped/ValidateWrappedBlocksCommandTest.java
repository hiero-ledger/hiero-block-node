// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import org.hiero.block.tools.blocks.ToWrappedBlocksCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import picocli.CommandLine;

/**
 * End-to-end test that converts mainnet record blocks to wrapped format and then validates them.
 *
 * <p>Uses test resources containing {@code .tar.zstd} day files (mainnet record data). All
 * {@code .tar.zstd} files found in the test resources directory are included automatically,
 * so users can drop additional day files for extended coverage without code changes.
 *
 * <p>Unit tests for individual validations have been moved to the
 * {@code org.hiero.block.tools.blocks.validation} package.
 */
@Execution(ExecutionMode.SAME_THREAD)
class ValidateWrappedBlocksCommandTest {

    @TempDir
    Path tempDir;

    /**
     * End-to-end test: convert mainnet record blocks to wrapped format using
     * {@link ToWrappedBlocksCommand}, then validate with {@link ValidateWrappedBlocksCommand}.
     * Both commands are invoked via picocli exactly as they would be from the CLI.
     *
     * <p>Dynamically scans test resources for all {@code .tar.zstd} files so users can add more
     * day files for extended coverage without code changes.
     */
    @Test
    void testConvertAndValidateFirstDayBlocks() throws Exception {
        assumeTrue(isZstdAvailable(), "Skipping test: zstd command not available");

        // Locate required test resources
        final Path resourceDir = Path.of(Objects.requireNonNull(getClass().getResource("/2019-09-13.tar.zstd"))
                        .toURI())
                .getParent();
        final Path blockTimesFile = Path.of(Objects.requireNonNull(getClass().getResource("/metadata/block_times.bin"))
                .toURI());
        final Path dayBlocksFile = Path.of(Objects.requireNonNull(getClass().getResource("/metadata/day_blocks.json"))
                .toURI());

        // Scan for all .tar.zstd files in test resources
        final List<Path> tarZstdFiles;
        try (var stream = Files.list(resourceDir)) {
            tarZstdFiles = stream.filter(p -> p.getFileName().toString().endsWith(".tar.zstd"))
                    .sorted()
                    .toList();
        }
        assumeFalse(tarZstdFiles.isEmpty(), "No .tar.zstd files found in test resources");
        System.out.println("Found " + tarZstdFiles.size() + " day file(s): "
                + tarZstdFiles.stream().map(p -> p.getFileName().toString()).toList());

        // Set up the input directory with copies of day files (zstd CLI ignores symlinks)
        final Path inputDir = tempDir.resolve("input");
        Files.createDirectories(inputDir);
        for (Path tarZstd : tarZstdFiles) {
            Files.copy(tarZstd, inputDir.resolve(tarZstd.getFileName()));
        }

        final Path outputDir = tempDir.resolve("output");

        // ===== Phase 1: Convert using ToWrappedBlocksCommand via picocli =====

        int wrapExitCode = new CommandLine(new ToWrappedBlocksCommand())
                .execute(
                        "-i",
                        inputDir.toString(),
                        "-o",
                        outputDir.toString(),
                        "-b",
                        blockTimesFile.toString(),
                        "-d",
                        dayBlocksFile.toString(),
                        "-u");
        assertEquals(0, wrapExitCode, "Wrap command should exit with code 0");

        // ===== Phase 2: Validate using ValidateWrappedBlocksCommand via picocli =====

        // Capture stderr to verify no validation errors occurred
        final PrintStream originalErr = System.err;
        final ByteArrayOutputStream errCapture = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errCapture));
        int validateExitCode;
        try {
            // Disable balance validation for this test (no GCP access in test environment)
            validateExitCode = new CommandLine(new ValidateWrappedBlocksCommand())
                    .execute(outputDir.toString(), "--validate-balances=false");
        } finally {
            System.setErr(originalErr);
        }

        // Echo captured stderr so it's visible in test output, then assert no errors
        final String errorOutput = errCapture.toString();
        if (!errorOutput.isEmpty()) {
            System.err.print(errorOutput);
        }
        assertFalse(errorOutput.contains("Blockchain is not valid"), "Chain validation failed: " + errorOutput);
        assertFalse(errorOutput.contains("HBAR supply mismatch"), "50 billion HBAR check failed: " + errorOutput);
        assertFalse(
                errorOutput.contains("Address book file not found"),
                "Balance validation should be disabled: " + errorOutput);
        assertEquals(0, validateExitCode, "Validation should pass for all blocks. Errors: " + errorOutput);
    }

    // ===== Helpers =====

    private static boolean isZstdAvailable() {
        try {
            Process p = new ProcessBuilder("which", "zstd").start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
