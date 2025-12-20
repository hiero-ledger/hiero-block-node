// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hiero.block.tools.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.hiero.block.tools.utils.ConcurrentTarZstdWriter;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Command to fix signature file names by renaming .rcs.sig to .rcd.sig
 */
@Command(
        name = "fix-sig-names",
        description = "Command to fix signature file names by renaming .rcs.sig to .rcd.sig",
        mixinStandardHelpOptions = true)
public class FixSignatureFileNames implements Runnable {

    @Option(
            names = {"-b", "--backed-up-dir"},
            description = "Directory containing backed up day files (default: compressedDays-BACKED_UP)")
    private File backedUpDir = new File("compressedDays-BACKED_UP");

    @Option(
            names = {"-f", "--fixed-backup-dir"},
            description = "Directory containing already fixed day files (default: compressedDays-FIXED_BACKUP)")
    private File fixedBackupDir = new File("compressedDays-FIXED_BACKUP");

    @Option(
            names = {"-o", "--output-dir"},
            description = "Directory for output fixed day files (default: compressedDays-FIXED)")
    private File outputDir = new File("compressedDays-FIXED");

    @Override
    public void run() {
        if (!validateDirectories()) {
            return;
        }

        List<Path> backedUpFiles = getBackedUpFiles();
        if (backedUpFiles.isEmpty()) {
            System.out.println(Ansi.AUTO.string(
                    "@|yellow No .tar.zstd files found in backed up directory at: " + backedUpDir + "|@"));
            return;
        }

        List<String> fixedBackupFileNames = getFixedBackupFileNames();

        System.out.println(Ansi.AUTO.string("@|green Found " + backedUpFiles.size() + " backed up files|@"));
        System.out.println(Ansi.AUTO.string("@|green Found " + fixedBackupFileNames.size() + " already fixed files|@"));

        ProcessingStats stats = processAllFiles(backedUpFiles, fixedBackupFileNames);

        System.out.println(Ansi.AUTO.string("@|green ✓ Complete! Copied " + stats.copiedCount + " files, processed "
                + stats.processedCount + " files|@"));
    }

    private boolean validateDirectories() {
        if (!backedUpDir.exists() || !backedUpDir.isDirectory()) {
            System.out.println(
                    Ansi.AUTO.string("@|red Error: backed up directory not found at: " + backedUpDir + "|@"));
            return false;
        }

        if (fixedBackupDir.exists() && !fixedBackupDir.isDirectory()) {
            System.out.println(Ansi.AUTO.string(
                    "@|red Error: fixed backup directory is not a directory at: " + fixedBackupDir + "|@"));
            return false;
        }

        if (!outputDir.exists()) {
            if (outputDir.mkdirs()) {
                System.out.println(Ansi.AUTO.string("@|white Created output directory at: " + outputDir + "|@"));
            } else {
                System.out.println(
                        Ansi.AUTO.string("@|red Error: could not create output directory at: " + outputDir + "|@"));
                return false;
            }
        } else if (!outputDir.isDirectory()) {
            System.out.println(
                    Ansi.AUTO.string("@|red Error: output directory is not a directory at: " + outputDir + "|@"));
            return false;
        }

        return true;
    }

    private List<Path> getBackedUpFiles() {
        try (var stream = Files.list(backedUpDir.toPath())) {
            return stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".tar.zstd"))
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Error reading backed up directory", e);
        }
    }

    private List<String> getFixedBackupFileNames() {
        if (!fixedBackupDir.exists()) {
            return List.of();
        }

        try (var stream = Files.list(fixedBackupDir.toPath())) {
            return stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".tar.zstd"))
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Error reading fixed backup directory", e);
        }
    }

    private ProcessingStats processAllFiles(List<Path> backedUpFiles, List<String> fixedBackupFileNames) {
        int copiedCount = 0;
        int processedCount = 0;
        int skippedCount = 0;

        final long startNanos = System.nanoTime();
        final int totalFiles = backedUpFiles.size();
        int currentFileIndex = 0;

        for (Path backedUpFile : backedUpFiles) {
            String fileName = backedUpFile.getFileName().toString();
            Path outputFile = outputDir.toPath().resolve(fileName);

            // Check if output file already exists and skip if so (resume capability)
            if (Files.exists(outputFile)) {
                try {
                    // Only skip if output file is same size or larger than input
                    long outputSize = Files.size(outputFile);
                    long inputSize = Files.size(backedUpFile);
                    if (outputSize >= inputSize) {
                        skippedCount++;
                        currentFileIndex++;
                        continue;
                    } else {
                        // Output file is smaller, likely incomplete - delete and reprocess
                        PrettyPrint.clearProgress();
                        System.out.println(
                                Ansi.AUTO.string("@|yellow Deleting incomplete output file: " + fileName + "|@"));
                        Files.delete(outputFile);
                    }
                } catch (IOException e) {
                    System.err.println("Error checking output file " + fileName + ": " + e.getMessage());
                }
            }

            if (fixedBackupFileNames.contains(fileName)) {
                PrettyPrint.clearProgress();
                System.out.println(Ansi.AUTO.string("@|cyan Copying already fixed file: " + fileName + "|@"));
                copyAlreadyFixedFile(fileName, outputFile);
                copiedCount++;
            } else {
                PrettyPrint.clearProgress();
                System.out.println(Ansi.AUTO.string("@|yellow Processing file: " + fileName + "|@"));
                processFile(backedUpFile, outputFile);
                processedCount++;
            }

            currentFileIndex++;

            // Update progress with ETA
            final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
            final double percent = ((double) currentFileIndex / (double) totalFiles) * 100.0;
            final long remainingMillis =
                    PrettyPrint.computeRemainingMilliseconds(currentFileIndex, totalFiles, elapsedMillis);
            final String progressString = String.format("File %d of %d: %s", currentFileIndex, totalFiles, fileName);
            PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
        }

        PrettyPrint.clearProgress();

        if (skippedCount > 0) {
            System.out.println(Ansi.AUTO.string("@|cyan Skipped " + skippedCount + " already completed files|@"));
        }

        return new ProcessingStats(copiedCount, processedCount);
    }

    private void copyAlreadyFixedFile(String fileName, Path outputFile) {
        Path fixedBackupFile = fixedBackupDir.toPath().resolve(fileName);
        try {
            Files.copy(fixedBackupFile, outputFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Error copying file: " + fileName, e);
        }
    }

    private record ProcessingStats(int copiedCount, int processedCount) {}

    private void processFile(Path inputFile, Path outputFile) {
        try (Stream<UnparsedRecordBlock> stream = TarZstdDayReaderUsingExec.streamTarZstd(inputFile);
                ConcurrentTarZstdWriter writer = new ConcurrentTarZstdWriter(outputFile)) {

            stream.forEach(block -> {
                // Write primary record file
                writer.putEntry(block.primaryRecordFile());

                // Process and write signature files
                for (InMemoryFile sigFile : block.signatureFiles()) {
                    String fileName = sigFile.path().getFileName().toString();

                    // Check if the file has .rcs.sig extension and rename to .rcd.sig
                    if (fileName.endsWith(".rcs.sig")) {
                        String newFileName = fileName.replace(".rcs.sig", ".rcd.sig");
                        Path newPath = sigFile.path().getParent() != null
                                ? sigFile.path().getParent().resolve(newFileName)
                                : Path.of(newFileName);
                        InMemoryFile renamedSigFile = new InMemoryFile(newPath, sigFile.data());
                        writer.putEntry(renamedSigFile);
                    } else {
                        writer.putEntry(sigFile);
                    }
                }

                // Write other record files
                block.otherRecordFiles().forEach(writer::putEntry);

                // Write primary sidecar files
                block.primarySidecarFiles().forEach(writer::putEntry);

                // Write other sidecar files
                block.otherSidecarFiles().forEach(writer::putEntry);
            });

        } catch (Exception e) {
            throw new RuntimeException("Error processing file: " + inputFile, e);
        }
    }
}
