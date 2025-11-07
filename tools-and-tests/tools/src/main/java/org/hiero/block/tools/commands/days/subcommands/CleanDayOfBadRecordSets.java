// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import static org.hiero.block.tools.utils.PrettyPrint.computeRemainingMilliseconds;
import static org.hiero.block.tools.utils.PrettyPrint.printProgress;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.hiero.block.tools.commands.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.commands.days.model.TarZstdDayUtils;
import org.hiero.block.tools.records.RecordFileBlock;
import org.hiero.block.tools.utils.ConcurrentTarZstdWriter;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Clean day files by removing record file blocks that have only 1 signature file.
 */
@Command(name = "clean", description = "Clean all day .tar.zstd files or day files in directories passed in")
public class CleanDayOfBadRecordSets implements Runnable {
    @Parameters(index = "0..*", description = "Files or directories to process")
    private final File[] compressedDayOrDaysDirs = new File[0];

    @Option(
        names = {"-d", "--bad-files-dir"},
        description = "Directory where bad day files are stored")
    private Path badDayDir = Path.of("compressedDays-BAD");
    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        // If no inputs are provided, print usage help for this subcommand
        if (compressedDayOrDaysDirs.length == 0) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }
        // create badDayDir if it does not exist
        if (!Files.exists(badDayDir)) {
            try {
                Files.createDirectories(badDayDir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // first scan all input files and build list of bad day files to process
        final List<Path> inputDayPaths = TarZstdDayUtils.sortedDayPaths(compressedDayOrDaysDirs);
        final List<Path> badDayPaths = new java.util.ArrayList<>();
        for (int i = 0; i < inputDayPaths.size(); i++) {
            Path dayFile = inputDayPaths.get(i);
            printProgress(i, inputDayPaths.size(),"Scanning for bad day files: " + dayFile+
                " found "+badDayPaths.size()+" so far");
            try (final Stream<RecordFileBlock> stream = TarZstdDayReaderUsingExec.streamTarZstd(dayFile)) {
                boolean hasBadRecordSet = stream
                    // filter out bad record sets with only 1 signature file
                    .anyMatch((RecordFileBlock block) -> block.signatureFiles().size() <= 1);
                if (hasBadRecordSet) {
                    badDayPaths.add(dayFile);
                }
            } catch (Exception e) {
                PrettyPrint.clearProgress();
                System.err.printf("Skipping, Error scanning day file %s: %s%n", dayFile, e);
            }
        }
        PrettyPrint.clearProgress();
        // print how many bad day files found
        System.out.printf("Found %d bad day files to clean out of %d total day files%n",
            badDayPaths.size(), inputDayPaths.size());
        // move all the bad day files to badDayDir
        final List<Path> movedBadDayPaths = new java.util.ArrayList<>();
        final Map<Path,Path> badToGoodMap = new java.util.HashMap<>();
        for (Path badDayFile : badDayPaths) {
            final Path movedBadFilePath = badDayDir.resolve(badDayFile.getFileName());
            movedBadDayPaths.add(movedBadFilePath);
            badToGoodMap.put(movedBadFilePath, badDayFile);
            try {
                Files.move(badDayFile, movedBadFilePath);
                System.out.printf("    - Moved bad day file %s to %s%n", badDayFile, movedBadFilePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // process all moved bad day files and write cleaned day files to original location
        final AtomicLong lastReportedMinute = new AtomicLong(Long.MIN_VALUE);
        final long INITIAL_ESTIMATE_PER_DAY = (24L * 60L * 60L) / 2L; // one every 2 seconds for a whole day
        final long dayCount = movedBadDayPaths.size();
        final AtomicLong totalProgress = new AtomicLong(dayCount * INITIAL_ESTIMATE_PER_DAY);
        final AtomicLong progress = new AtomicLong(0);
        final long startNanos = System.nanoTime();
        for (Path badDayFile : movedBadDayPaths) {
            final Path finalOutFile = badToGoodMap.get(badDayFile);
            try (final Stream<RecordFileBlock> stream = TarZstdDayReaderUsingExec.streamTarZstd(badDayFile);
                    final ConcurrentTarZstdWriter writer = new ConcurrentTarZstdWriter(finalOutFile)) {
                stream
                    // filter out bad record sets with only 1 signature file
                    .filter((RecordFileBlock block) -> block.signatureFiles().size() > 1)
                    // write good record sets to cleaned day file
                    .forEach((RecordFileBlock block) -> {
                        writer.putEntry(block.primaryRecordFile());
                        block.signatureFiles().forEach(writer::putEntry);
                        block.otherRecordFiles().forEach(writer::putEntry);
                        block.primarySidecarFiles().forEach(writer::putEntry);
                        block.otherSidecarFiles().forEach(writer::putEntry);
                        // Build progress string showing time and hashes (shortened to 8 chars for readability)
                        long blockMinute = block.recordFileTime().getEpochSecond() / 60L;
                        final String progressString = String.format("%s :: %s -> %s",
                            block.recordFileTime(),badDayFile, finalOutFile);
                        // Estimate totals and ETA
                        final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                        // Progress percent and remaining
                        final long processedSoFarAcrossAll = progress.incrementAndGet();
                        final long totalProgressFinal = totalProgress.get();
                        double percent = ((double) processedSoFarAcrossAll / (double) totalProgressFinal) * 100.0;
                        long remainingMillis =
                            computeRemainingMilliseconds(processedSoFarAcrossAll, totalProgressFinal, elapsedMillis);

                        if (blockMinute != lastReportedMinute.getAndSet(blockMinute)) {
                            PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
                        }
                    });
            } catch (Exception e) {
                PrettyPrint.clearProgress();
                throw new RuntimeException(e);
            }
        }
    }
}
