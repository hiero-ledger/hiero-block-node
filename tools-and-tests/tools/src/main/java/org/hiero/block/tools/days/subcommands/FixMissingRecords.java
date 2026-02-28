// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.records.RecordFileDates.convertInstantToStringWithPadding;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.hiero.block.tools.utils.ConcurrentTarZstdWriter;
import org.hiero.block.tools.utils.PrettyPrint;
import org.hiero.block.tools.utils.gcp.MainNetBucket;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Command to fix missing primary record files in downloaded days by downloading them from GCP.
 *
 * <p>When building tar.zstd day archives from GCP, occasionally a primary record file fails to
 * download. The archive still contains the directory with signature files, sidecars, and
 * node-specific copies — but the primary .rcd is missing. This command fixes that by downloading
 * the missing record file from a node that has it.
 *
 * <p>The command writes a new fixed archive to a separate output directory. The old archive is
 * NOT deleted — a warning is printed telling the operator to delete it manually.
 */
@SuppressWarnings({"FieldCanBeLocal", "UnusedAssignment"})
@Command(
        name = "fix-missing-records",
        description = "Command to fix missing primary record files in downloaded days",
        mixinStandardHelpOptions = true)
public class FixMissingRecords implements Runnable {
    /** Estimated number of blocks per day (used for progress reporting) */
    private static final int ESTIMATE_BLOCKS_PER_DAY = 24 * 60 * 30; // 1 block every 2 seconds

    @Option(
            names = {"--min-node"},
            description = "Minimum node account ID (default: 3)")
    private int minNodeAccountId = 3;

    @Option(
            names = {"--max-node"},
            description = "Maximum node account ID (default: 37)")
    private int maxNodeAccountId = 37;

    @Option(
            names = {"-d", "--downloaded-days-dir"},
            description = "Directory where downloaded days are stored")
    private File compressedDaysDir = new File("compressedDays");

    @Option(
            names = {"-f", "--fixed-days-dir"},
            description = "Directory where fixed downloaded days are stored")
    private File fixedCompressedDaysDir = new File("compressedDays-FIXED");

    @Option(
            names = {"-p", "--user-project"},
            description = "GCP project to bill for requester-pays bucket access (default: from GCP_PROJECT_ID env var)")
    private String userProject = DownloadConstants.GCP_PROJECT_ID;

    @Option(
            names = {"--single-day"},
            description = "Process only a single day (format: YYYY-MM-DD, e.g., 2025-10-09)")
    private String singleDay = null;

    @SuppressWarnings("BusyWait")
    @Override
    public void run() {
        // Check compressedDaysDir
        if (!compressedDaysDir.exists() || !compressedDaysDir.isDirectory()) {
            System.out.println(Ansi.AUTO.string(
                    "@|red Error: compressedDays directory not found at: " + compressedDaysDir + "|@"));
            return;
        }
        // find first and last days in compressedDaysDir
        List<LocalDate> days;
        try (var stream = Files.list(compressedDaysDir.toPath())) {
            days = stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".tar.zstd"))
                    .map(path -> {
                        final String fileName = path.getFileName().toString();
                        return LocalDate.parse(fileName.substring(0, fileName.indexOf(".")));
                    })
                    .sorted()
                    .collect(Collectors.toList()); // need modifiable list
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (days.isEmpty()) {
            System.out.println(Ansi.AUTO.string(
                    "@|red Error: no days found in compressedDays directory at: " + compressedDaysDir + "|@"));
            return;
        }
        // Handle --single-day option: override the days list to process only the specified day
        if (singleDay != null) {
            LocalDate targetDay;
            try {
                targetDay = LocalDate.parse(singleDay);
            } catch (Exception e) {
                System.out.println(Ansi.AUTO.string("@|red Error: Invalid date format for --single-day: " + singleDay
                        + ". Expected format: YYYY-MM-DD (e.g., 2025-10-09)|@"));
                return;
            }
            if (!days.contains(targetDay)) {
                System.out.println(Ansi.AUTO.string("@|red Error: Specified day " + singleDay
                        + " not found in compressedDays directory. Available range: " + days.getFirst() + " to "
                        + days.getLast() + "|@"));
                return;
            }
            days.clear();
            days.add(targetDay);
            System.out.println(Ansi.AUTO.string("@|cyan Processing single day: " + targetDay + "|@"));
        }
        // Check fixedCompressedDaysDir
        if (!fixedCompressedDaysDir.exists()) {
            if (fixedCompressedDaysDir.mkdirs()) {
                System.out.println(Ansi.AUTO.string("@|white created fixedCompressedDaysDir directory |@"));
            } else {
                System.out.println(
                        Ansi.AUTO.string("@|red Error: could not create fixedCompressedDaysDir directory at: "
                                + fixedCompressedDaysDir + "|@"));
                return;
            }
        } else if (!fixedCompressedDaysDir.isDirectory()) {
            System.out.println(Ansi.AUTO.string(
                    "@|red Error: fixedCompressedDaysDir is not a directory at: " + fixedCompressedDaysDir + "|@"));
            return;
        } else {
            // check for already existing fixed day files and skip those days
            try (var stream = Files.list(fixedCompressedDaysDir.toPath())) {
                stream.filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().endsWith(".tar.zstd"))
                        .sorted()
                        .forEach(fixedDayPath -> {
                            final String fileName = fixedDayPath.getFileName().toString();
                            final LocalDate day = LocalDate.parse(fileName.substring(0, fileName.indexOf(".")));
                            final Path originalDayPath =
                                    compressedDaysDir.toPath().resolve(fileName);
                            // check the fixed files is same size or larger than original
                            try {
                                if (Files.size(fixedDayPath) >= Files.size(originalDayPath)) {
                                    System.out.println(
                                            Ansi.AUTO.string("@|yellow Skipping already fixed day: " + day + "|@"));
                                    days.remove(day);
                                } else {
                                    Files.delete(fixedDayPath);
                                    System.out.println(Ansi.AUTO.string(
                                            "@|yellow Deleting partial fixed day (fixed file smaller than original): "
                                                    + day + "|@"));
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        // collect range of days
        final int numOfDays = days.size();
        System.out.println(Ansi.AUTO.string(
                "@|green Found source date range: " + days.getFirst() + " to " + days.getLast() + "|@\n"));
        // Set up GCP bucket access
        final MainNetBucket bucket = new MainNetBucket(
                false, Path.of("metadata/gcp-cache"), minNodeAccountId, maxNodeAccountId, userProject);

        // Producer-consumer queue with bounded capacity for backpressure
        final BlockingQueue<DayWork> dayListingsQueue = new LinkedBlockingQueue<>(10);
        // start a background thread to fetch day bucket record file listings
        Thread fetcherThread = new Thread(() -> {
            try {
                for (LocalDate day : days) {
                    dayListingsQueue.put(new DayWork(day, bucket.listRecordFilesForDay(day.toString())));
                }
            } catch (Exception e) {
                //noinspection CallToPrintStackTrace
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
        fetcherThread.start();

        // ETA/speed tracking state
        final long startNanos = System.nanoTime();
        final AtomicLong totalProgress = new AtomicLong((long) numOfDays * ESTIMATE_BLOCKS_PER_DAY);
        final AtomicLong progress = new AtomicLong(0);
        // Track speed calculation over last 10 seconds of wall clock time
        final AtomicReference<Instant> lastSpeedCalcBlockTime = new AtomicReference<>();
        final AtomicLong lastSpeedCalcRealTimeNanos = new AtomicLong(0);

        // process each day
        int dayCount = 0;
        long progressAtStartOfDay = 0L;
        long totalBlocksFixed = 0;
        while (!dayListingsQueue.isEmpty() || fetcherThread.isAlive()) {
            DayWork dayWork = dayListingsQueue.poll();
            if (dayWork == null) {
                // wait a bit and retry
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            progressAtStartOfDay = progress.get();
            totalBlocksFixed += fixDayRecords(
                    numOfDays,
                    dayCount,
                    dayWork.day,
                    bucket,
                    dayWork.bucketRecords,
                    startNanos,
                    totalProgress,
                    progress,
                    lastSpeedCalcBlockTime,
                    lastSpeedCalcRealTimeNanos);
            dayCount++;
            // After finishing the day, update aggregates
            final long progressAtEndOfDay = progress.get();
            final long blocksInDay = progressAtEndOfDay - progressAtStartOfDay;
            final long remainingDays = numOfDays - dayCount;
            // update total estimate based on actual blocks processed in this day
            totalProgress.set(progressAtEndOfDay + (remainingDays * blocksInDay));
        }
        // clear the progress line once done and print a summary
        PrettyPrint.clearProgress();
        System.out.println("Fix records complete. Days processed: " + dayCount + ", Blocks processed: " + progress.get()
                + ", Blocks fixed: " + totalBlocksFixed);
    }

    private record DayWork(LocalDate day, Map<Instant, Set<String>> bucketRecords) {}

    private long fixDayRecords(
            int numOfDays,
            int dayIndex,
            LocalDate day,
            MainNetBucket bucket,
            final Map<Instant, Set<String>> bucketRecords,
            long startNanos,
            AtomicLong totalProgress,
            AtomicLong progress,
            AtomicReference<Instant> lastSpeedCalcBlockTime,
            AtomicLong lastSpeedCalcRealTimeNanos) {
        final String progressPrefix = String.format("Day %d of %d (%s)", dayIndex + 1, numOfDays, day);
        // Compute the path to the day file
        final String dayFileName = day.toString() + ".tar.zstd";
        final Path srcDayPath = compressedDaysDir.toPath().resolve(dayFileName);
        final Path dstDayPath = fixedCompressedDaysDir.toPath().resolve(dayFileName);
        final AtomicLong blocksFixed = new AtomicLong(0);

        try (Stream<UnparsedRecordBlock> stream = TarZstdDayReaderUsingExec.streamTarZstd(srcDayPath);
                ConcurrentTarZstdWriter writer = new ConcurrentTarZstdWriter(dstDayPath)) {
            // Track the last consensus-minute for progress reporting
            final AtomicLong lastReportedMinute = new AtomicLong(Long.MIN_VALUE);
            stream.forEach((UnparsedRecordBlock block) -> {
                final Instant blockTime = block.recordFileTime();
                final String blockTimeFileStr = convertInstantToStringWithPadding(blockTime);

                // Check if primary record file is present (Step 1 promotion may have already
                // handled the case where a node copy existed in the archive)
                InMemoryFile primaryRecord = block.primaryRecordFile();
                if (primaryRecord == null) {
                    // Primary is missing and no node copies were in the archive either.
                    // Download from GCP using the bucket listing to find a node that has it.
                    Set<String> nodesWithRecord = bucketRecords.get(blockTime);
                    if (nodesWithRecord == null || nodesWithRecord.isEmpty()) {
                        PrettyPrint.clearProgress();
                        System.out.println(Ansi.AUTO.string(
                                "@|red Error: No record files found in bucket for block time: " + blockTime + "|@"));
                        throw new RuntimeException("No record files found in bucket for block time: " + blockTime);
                    }
                    // Pick the first available node and download
                    String nodeId = nodesWithRecord.iterator().next();
                    String recordBucketPath = "recordstreams/record" + nodeId + "/" + blockTimeFileStr + ".rcd";
                    byte[] recordData;
                    try {
                        recordData = bucket.download(recordBucketPath);
                    } catch (Exception e) {
                        // Try .rcd.gz variant
                        recordData = bucket.download(recordBucketPath + ".gz");
                    }
                    primaryRecord =
                            new InMemoryFile(Path.of(blockTimeFileStr + "/" + blockTimeFileStr + ".rcd"), recordData);
                    blocksFixed.incrementAndGet();
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string("@|green Fixed missing record for block " + blockTime
                            + " (downloaded from node " + nodeId + ")|@"));
                }

                // write the block to the new day file
                writer.putEntry(primaryRecord);
                block.signatureFiles().forEach(writer::putEntry);
                block.otherRecordFiles().forEach(writer::putEntry);
                block.primarySidecarFiles().forEach(writer::putEntry);
                block.otherSidecarFiles().forEach(writer::putEntry);

                // update progress counter
                progress.incrementAndGet();

                // Calculate processing speed over last 10 seconds of wall clock time
                final long currentRealTimeNanos = System.nanoTime();
                final long tenSecondsInNanos = 10_000_000_000L;
                String speedString = "";

                // Initialize tracking on the first block
                if (lastSpeedCalcBlockTime.get() == null) {
                    lastSpeedCalcBlockTime.set(blockTime);
                    lastSpeedCalcRealTimeNanos.set(currentRealTimeNanos);
                }

                // Update the tracking window if more than 10 seconds of real time has elapsed
                long realTimeSinceLastCalc = currentRealTimeNanos - lastSpeedCalcRealTimeNanos.get();
                if (realTimeSinceLastCalc >= tenSecondsInNanos) {
                    lastSpeedCalcBlockTime.set(blockTime);
                    lastSpeedCalcRealTimeNanos.set(currentRealTimeNanos);
                }

                // Calculate speed if we have at least 1 second of real time elapsed since tracking point
                if (realTimeSinceLastCalc >= 1_000_000_000L) { // At least 1 second
                    long dataTimeElapsedMillis = blockTime.toEpochMilli()
                            - lastSpeedCalcBlockTime.get().toEpochMilli();
                    long realTimeElapsedMillis = realTimeSinceLastCalc / 1_000_000L;
                    double speedMultiplier = (double) dataTimeElapsedMillis / (double) realTimeElapsedMillis;
                    speedString = String.format(" speed %.1fx", speedMultiplier);
                }

                // Build progress string showing time
                final String progressString = String.format("%s %s%s", progressPrefix, blockTime, speedString);

                // Estimate totals and ETA
                final long elapsedMillis = (currentRealTimeNanos - startNanos) / 1_000_000L;
                // Progress percent and remaining
                final long processedSoFarAcrossAll = progress.get();
                final long totalProgressFinal = totalProgress.get();
                double percent = ((double) processedSoFarAcrossAll / (double) totalProgressFinal) * 100.0;
                long remainingMillis = PrettyPrint.computeRemainingMilliseconds(
                        processedSoFarAcrossAll, totalProgressFinal, elapsedMillis);

                // Only print progress once per consensus-minute
                long blockMinute = blockTime.getEpochSecond() / 60L;
                if (blockMinute != lastReportedMinute.get()) {
                    PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
                    lastReportedMinute.set(blockMinute);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Print warning about old archive
        PrettyPrint.clearProgress();
        System.out.println(
                Ansi.AUTO.string("@|yellow \u26a0 Old archive should be manually deleted: " + srcDayPath + "|@"));

        return blocksFixed.get();
    }
}
