// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Bulk-load wrapped blocks from CLI output into a Block Node's historic storage.
 *
 * <p>This command copies wrapped block zip files from the CLI's output directory (as produced by
 * the {@code wrap} command) directly into a Block Node's historic storage directory. The Block Node
 * MUST be stopped before running this command.
 *
 * <p>On Block Node startup, the {@code BlockFileHistoricPlugin} will detect all copied blocks and
 * make them available for serving.
 *
 * <h2>Storage Format Compatibility</h2>
 * <p>Both the CLI's {@code wrap} command and the Block Node's {@code BlockFileHistoricPlugin} use
 * the same storage format (as defined by {@code BlockWriter}), so zip files can be copied directly
 * without any conversion or restructuring.
 *
 * <h2>Resumable</h2>
 * <p>This command is resumable: if run multiple times, it will only copy files that don't already
 * exist in the destination. Files are compared by path and size.
 *
 * <h2>Safety</h2>
 * <p>The Block Node MUST be stopped before running this command to avoid concurrent writes to the
 * same storage location.
 *
 * <h2>Example Usage</h2>
 * <pre>
 * # Stop the Block Node first!
 * # systemctl stop hiero-block-node
 *
 * # Bulk-load wrapped blocks
 * blocks bulk-load --source wrappedBlocks --dest /opt/hiero/block-node/data/historic
 *
 * # Start the Block Node
 * # systemctl start hiero-block-node
 * </pre>
 *
 * <h2>Directory Structure</h2>
 * <p>The command copies the entire directory tree from the source to destination, preserving the
 * nested directory structure required by the Block Node:
 * <pre>
 * source/
 *   000/123/456/789/012/345/
 *     60000s.zip
 *     70000s.zip
 *
 * dest/
 *   000/123/456/789/012/345/
 *     60000s.zip  (copied)
 *     70000s.zip  (copied)
 * </pre>
 */
@Command(
        name = "bulk-load",
        description = "Bulk-load wrapped blocks from CLI output into Block Node historic storage (BN must be stopped)",
        mixinStandardHelpOptions = true)
public class BulkLoadBlocksCommand implements Callable<Integer> {

    @Option(
            names = {"-s", "--source"},
            description = "Source directory containing wrapped block zip files (from CLI wrap command)",
            required = true)
    private Path sourceDir;

    @Option(
            names = {"-d", "--dest"},
            description =
                    "Destination directory (Block Node historic root path, e.g., /opt/hiero/block-node/data/historic)",
            required = true)
    private Path destDir;

    @Option(
            names = {"--dry-run"},
            description = "Show what would be copied without actually copying")
    private boolean dryRun = false;

    @Option(
            names = {"--start-block"},
            description = "Start copying from this block number onwards (for resuming interrupted loads)")
    private long startBlock = -1;

    private static final String STATE_FILE_NAME = "historic-plugin-bulk-load-state.json";
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    /**
     * State tracking for resume functionality.
     */
    public static class LoadState {
        @JsonProperty
        public String sourceDir;

        @JsonProperty
        public String destDir;

        @JsonProperty
        public List<String> copiedFiles = new ArrayList<>();

        @JsonProperty
        public long lastCopiedBlock = -1;

        @JsonProperty
        public long totalBytesCopied = 0;

        @JsonProperty
        public long totalFilesCopied = 0;
    }

    @Override
    public Integer call() throws Exception {
        // Validate source directory
        if (!Files.isDirectory(sourceDir)) {
            System.err.println("Error: Source directory does not exist: " + sourceDir);
            return 1;
        }

        // Warn if destination already exists
        if (Files.exists(destDir)) {
            System.out.println(
                    Ansi.AUTO.string("@|yellow Warning:|@ Destination directory already exists: " + destDir));
            System.out.println("This command will only copy files that don't exist in the destination.");
            System.out.println();
        } else {
            // Create destination directory
            try {
                Files.createDirectories(destDir);
                System.out.println(Ansi.AUTO.string("@|yellow Created destination directory:|@ " + destDir));
            } catch (IOException e) {
                System.err.println("Error: Failed to create destination directory: " + e.getMessage());
                return 1;
            }
        }

        // Load or create state file
        Path stateFilePath = destDir.resolve(STATE_FILE_NAME);
        LoadState state = loadState(stateFilePath);

        // Check if resuming from a different source
        if (state.sourceDir != null
                && !state.sourceDir.equals(sourceDir.toAbsolutePath().toString())) {
            System.out.println(
                    Ansi.AUTO.string("@|yellow Warning:|@ State file indicates previous source: " + state.sourceDir));
            System.out.println("Current source: " + sourceDir.toAbsolutePath());
            System.out.println("Continuing with current source (state will be updated).");
        }

        // Update state with current run parameters
        state.sourceDir = sourceDir.toAbsolutePath().toString();
        state.destDir = destDir.toAbsolutePath().toString();

        System.out.println(Ansi.AUTO.string("@|yellow Bulk-loading wrapped blocks:|@"));
        System.out.println("  Source: " + sourceDir.toAbsolutePath());
        System.out.println("  Destination: " + destDir.toAbsolutePath());
        if (state.lastCopiedBlock >= 0) {
            System.out.println(Ansi.AUTO.string("  @|yellow Resuming from block:|@ " + (state.lastCopiedBlock + 1)));
            System.out.println("  Previously copied: " + state.totalFilesCopied + " files ("
                    + formatBytes(state.totalBytesCopied) + ")");
        }
        if (startBlock >= 0) {
            System.out.println("  Manual start block: " + startBlock);
        }
        if (dryRun) {
            System.out.println(Ansi.AUTO.string("  @|yellow DRY RUN MODE - no files will be copied|@"));
        }
        System.out.println();

        // Determine effective start block (use state's last copied block if higher)
        long effectiveStartBlock = startBlock;
        if (state.lastCopiedBlock >= 0) {
            effectiveStartBlock = Math.max(effectiveStartBlock, state.lastCopiedBlock + 1);
        }

        // Copy files
        AtomicLong copiedFiles = new AtomicLong(state.totalFilesCopied);
        AtomicLong skippedFiles = new AtomicLong(0);
        AtomicLong copiedBytes = new AtomicLong(state.totalBytesCopied);

        try {
            long finalEffectiveStartBlock = effectiveStartBlock;
            Files.walkFileTree(sourceDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    // Only copy .zip files
                    if (!file.toString().endsWith(".zip")) {
                        return FileVisitResult.CONTINUE;
                    }

                    // Check if already copied (tracked in state)
                    Path relativePath = sourceDir.relativize(file);
                    if (state.copiedFiles.contains(relativePath.toString())) {
                        skippedFiles.incrementAndGet();
                        return FileVisitResult.CONTINUE;
                    }

                    // Check if this file is before our start block (for resume functionality)
                    if (finalEffectiveStartBlock >= 0 && shouldSkipBasedOnBlockNumber(file, finalEffectiveStartBlock)) {
                        skippedFiles.incrementAndGet();
                        return FileVisitResult.CONTINUE;
                    }

                    // Compute destination path preserving directory structure
                    Path destFile = destDir.resolve(relativePath);

                    // Check if file already exists with same size
                    if (Files.exists(destFile)) {
                        long destSize = Files.size(destFile);
                        long sourceSize = attrs.size();
                        if (destSize == sourceSize) {
                            skippedFiles.incrementAndGet();
                            if (dryRun) {
                                System.out.println("SKIP (exists): " + relativePath);
                            }
                            // Add to state even if skipped
                            if (!dryRun) {
                                state.copiedFiles.add(relativePath.toString());
                                updateBlockNumber(state, file);
                            }
                            return FileVisitResult.CONTINUE;
                        } else {
                            System.out.println(Ansi.AUTO.string(
                                    "@|yellow WARNING:|@ Size mismatch for " + relativePath + " (source: " + sourceSize
                                            + " bytes, dest: " + destSize + " bytes) - will overwrite"));
                        }
                    }

                    if (dryRun) {
                        System.out.println("COPY: " + relativePath + " (" + formatBytes(attrs.size()) + ")");
                    } else {
                        // Create parent directories
                        Files.createDirectories(destFile.getParent());

                        // Copy file
                        Files.copy(
                                file,
                                destFile,
                                StandardCopyOption.REPLACE_EXISTING,
                                StandardCopyOption.COPY_ATTRIBUTES);

                        // Update state
                        state.copiedFiles.add(relativePath.toString());
                        state.totalFilesCopied++;
                        state.totalBytesCopied += attrs.size();
                        updateBlockNumber(state, file);

                        // Save state periodically (every 10 files)
                        if (state.totalFilesCopied % 10 == 0) {
                            saveState(stateFilePath, state);
                            System.out.printf(
                                    "Copied %d files (%s), last block: %d%n",
                                    state.totalFilesCopied, formatBytes(state.totalBytesCopied), state.lastCopiedBlock);
                        }
                    }

                    copiedFiles.set(state.totalFilesCopied);
                    copiedBytes.set(state.totalBytesCopied);

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    // Skip staging, links, and zipwork directories if they exist in source
                    String dirName = dir.getFileName().toString();
                    if (dirName.equals("staging") || dirName.equals("links") || dirName.equals("zipwork")) {
                        System.out.println(Ansi.AUTO.string("@|yellow Skipping directory:|@ " + dirName));
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            System.err.println("Error during bulk load: " + e.getMessage());
            e.printStackTrace();
            return 1;
        }

        // Save final state
        if (!dryRun) {
            saveState(stateFilePath, state);
        }

        // Print summary
        System.out.println();
        System.out.println(Ansi.AUTO.string("@|yellow === Bulk Load Summary ===|@"));
        System.out.println("Total files copied: " + state.totalFilesCopied);
        System.out.println("Files skipped (already exist or before start block): " + skippedFiles.get());
        System.out.println("Total bytes copied: " + formatBytes(state.totalBytesCopied));
        System.out.println("Last copied block: " + state.lastCopiedBlock);
        System.out.println("State file: " + stateFilePath);

        if (dryRun) {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|yellow DRY RUN - No files were actually copied.|@"));
            System.out.println("Re-run without --dry-run to perform the copy.");
        } else {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|green Bulk load complete!|@"));
            System.out.println();
            System.out.println("Next steps:");
            System.out.println("1. Start the Block Node");
            System.out.println("2. The BlockFileHistoricPlugin will detect and serve the loaded blocks");
            System.out.println();
            System.out.println("To resume an interrupted load, simply run the same command again.");
        }

        return 0;
    }

    private LoadState loadState(Path stateFilePath) {
        if (!Files.exists(stateFilePath)) {
            return new LoadState();
        }

        try {
            return JSON_MAPPER.readValue(stateFilePath.toFile(), LoadState.class);
        } catch (IOException e) {
            System.err.println("Warning: Failed to load state file, starting fresh: " + e.getMessage());
            return new LoadState();
        }
    }

    private void saveState(Path stateFilePath, LoadState state) {
        try {
            JSON_MAPPER.writeValue(stateFilePath.toFile(), state);
        } catch (IOException e) {
            System.err.println("Warning: Failed to save state file: " + e.getMessage());
        }
    }

    /**
     * Update the lastCopiedBlock in the state based on the zip file being copied.
     */
    private void updateBlockNumber(LoadState state, Path zipFile) {
        String fileName = zipFile.getFileName().toString();
        if (!fileName.matches("\\d+s\\.zip")) {
            return;
        }

        try {
            // Extract the first block number from the zip file name
            String numStr = fileName.substring(0, fileName.length() - 5); // Remove "s.zip"
            long zipFirstBlock = Long.parseLong(numStr);

            // Each zip contains 10,000 blocks
            long zipLastBlock = zipFirstBlock + 10_000 - 1;

            // Update last copied block
            state.lastCopiedBlock = Math.max(state.lastCopiedBlock, zipLastBlock);
        } catch (NumberFormatException e) {
            // Invalid format, ignore
        }
    }

    /**
     * Check if a zip file should be skipped based on the start block number.
     * Zip files are named like "00000s.zip", "10000s.zip", etc., where the number
     * indicates the first block in that zip (for powersOfTen=4, each zip contains 10K blocks).
     *
     * @param zipFile the zip file path
     * @param startBlock the starting block number
     * @return true if this zip file contains only blocks before the start block
     */
    private boolean shouldSkipBasedOnBlockNumber(Path zipFile, long startBlock) {
        String fileName = zipFile.getFileName().toString();
        if (!fileName.matches("\\d+s\\.zip")) {
            // Not a standard zip file name, don't skip
            return false;
        }

        try {
            // Extract the first block number from the zip file name
            // e.g., "10000s.zip" -> 10000
            String numStr = fileName.substring(0, fileName.length() - 5); // Remove "s.zip"
            long zipFirstBlock = Long.parseLong(numStr);

            // Determine the last block in this zip
            // Assume powersOfTen=4 (10,000 blocks per zip) as that's the default
            long blocksPerZip = 10_000;
            long zipLastBlock = zipFirstBlock + blocksPerZip - 1;

            // Skip if the entire zip is before the start block
            return zipLastBlock < startBlock;
        } catch (NumberFormatException e) {
            // Invalid format, don't skip
            return false;
        }
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }
}
