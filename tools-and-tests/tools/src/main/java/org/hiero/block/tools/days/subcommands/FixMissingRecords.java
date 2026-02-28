// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.records.RecordFileDates.convertInstantToStringWithPadding;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
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
@SuppressWarnings("FieldCanBeLocal")
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

    /** Mutable progress state shared across day processing. */
    private record ProgressState(
            long startNanos,
            AtomicLong totalProgress,
            AtomicLong progress,
            AtomicReference<Instant> lastSpeedCalcBlockTime,
            AtomicLong lastSpeedCalcRealTimeNanos) {
        static ProgressState create(int numOfDays) {
            return new ProgressState(
                    System.nanoTime(),
                    new AtomicLong((long) numOfDays * ESTIMATE_BLOCKS_PER_DAY),
                    new AtomicLong(0),
                    new AtomicReference<>(),
                    new AtomicLong(0));
        }
    }

    private record DayWork(LocalDate day, Map<Instant, Set<String>> bucketRecords) {}

    @Override
    public void run() {
        if (!compressedDaysDir.exists() || !compressedDaysDir.isDirectory()) {
            System.out.println(Ansi.AUTO.string(
                    "@|red Error: compressedDays directory not found at: " + compressedDaysDir + "|@"));
            return;
        }
        List<LocalDate> days = scanDays();
        if (days.isEmpty()) {
            System.out.println(Ansi.AUTO.string(
                    "@|red Error: no days found in compressedDays directory at: " + compressedDaysDir + "|@"));
            return;
        }
        if (singleDay != null && !filterToSingleDay(days)) {
            return;
        }
        if (!prepareOutputDirectory()) {
            return;
        }
        skipAlreadyFixedDays(days);
        if (days.isEmpty()) {
            System.out.println(Ansi.AUTO.string("@|yellow All days already fixed, nothing to do.|@"));
            return;
        }

        final int numOfDays = days.size();
        System.out.println(Ansi.AUTO.string(
                "@|green Found source date range: " + days.getFirst() + " to " + days.getLast() + "|@\n"));
        final MainNetBucket bucket = new MainNetBucket(
                false, Path.of("metadata/gcp-cache"), minNodeAccountId, maxNodeAccountId, userProject);

        processDays(days, numOfDays, bucket);
    }

    /** Scan the compressed days directory for .tar.zstd files and return sorted dates. */
    private List<LocalDate> scanDays() {
        try (var stream = Files.list(compressedDaysDir.toPath())) {
            return stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".tar.zstd"))
                    .map(path -> {
                        final String fileName = path.getFileName().toString();
                        return LocalDate.parse(fileName.substring(0, fileName.indexOf(".")));
                    })
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to scan compressed days directory", e);
        }
    }

    /** Filter days list to a single day. Returns false if the day is invalid or not found. */
    private boolean filterToSingleDay(List<LocalDate> days) {
        LocalDate targetDay;
        try {
            targetDay = LocalDate.parse(singleDay);
        } catch (Exception e) {
            System.out.println(Ansi.AUTO.string("@|red Error: Invalid date format for --single-day: " + singleDay
                    + ". Expected format: YYYY-MM-DD (e.g., 2025-10-09)|@"));
            return false;
        }
        if (!days.contains(targetDay)) {
            System.out.println(Ansi.AUTO.string("@|red Error: Specified day " + singleDay
                    + " not found in compressedDays directory. Available range: " + days.getFirst() + " to "
                    + days.getLast() + "|@"));
            return false;
        }
        days.clear();
        days.add(targetDay);
        System.out.println(Ansi.AUTO.string("@|cyan Processing single day: " + targetDay + "|@"));
        return true;
    }

    /** Prepare the output directory. Returns false if it cannot be created. */
    private boolean prepareOutputDirectory() {
        if (!fixedCompressedDaysDir.exists()) {
            if (fixedCompressedDaysDir.mkdirs()) {
                System.out.println(Ansi.AUTO.string("@|white created fixedCompressedDaysDir directory |@"));
            } else {
                System.out.println(
                        Ansi.AUTO.string("@|red Error: could not create fixedCompressedDaysDir directory at: "
                                + fixedCompressedDaysDir + "|@"));
                return false;
            }
        } else if (!fixedCompressedDaysDir.isDirectory()) {
            System.out.println(Ansi.AUTO.string(
                    "@|red Error: fixedCompressedDaysDir is not a directory at: " + fixedCompressedDaysDir + "|@"));
            return false;
        }
        return true;
    }

    /** Remove days from the list that have already been fixed (fixed file >= original size). */
    private void skipAlreadyFixedDays(List<LocalDate> days) {
        if (!fixedCompressedDaysDir.exists()) return;
        try (var stream = Files.list(fixedCompressedDaysDir.toPath())) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".tar.zstd"))
                    .sorted()
                    .forEach(fixedDayPath -> {
                        final String fileName = fixedDayPath.getFileName().toString();
                        final LocalDate day = LocalDate.parse(fileName.substring(0, fileName.indexOf(".")));
                        final Path originalDayPath = compressedDaysDir.toPath().resolve(fileName);
                        try {
                            if (Files.size(fixedDayPath) >= Files.size(originalDayPath)) {
                                System.out.println(
                                        Ansi.AUTO.string("@|yellow Skipping already fixed day: " + day + "|@"));
                                days.remove(day);
                            } else {
                                Files.delete(fixedDayPath);
                                System.out.println(Ansi.AUTO.string(
                                        "@|yellow Deleting partial fixed day (fixed file smaller than original): " + day
                                                + "|@"));
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to scan fixed days directory", e);
        }
    }

    /** Main processing loop: fetches bucket listings in background, processes each day. */
    @SuppressWarnings("BusyWait")
    private void processDays(List<LocalDate> days, int numOfDays, MainNetBucket bucket) {
        final BlockingQueue<DayWork> dayListingsQueue = new LinkedBlockingQueue<>(10);
        Thread fetcherThread = new Thread(() -> {
            try {
                for (LocalDate day : days) {
                    dayListingsQueue.put(new DayWork(day, bucket.listRecordFilesForDay(day.toString())));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        fetcherThread.start();

        final ProgressState progressState = ProgressState.create(numOfDays);
        int dayCount = 0;
        long totalBlocksFixed = 0;

        while (!dayListingsQueue.isEmpty() || fetcherThread.isAlive()) {
            DayWork dayWork = dayListingsQueue.poll();
            if (dayWork == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            long progressAtStart = progressState.progress().get();
            totalBlocksFixed += fixDayRecords(dayCount, dayWork, numOfDays, bucket, progressState);
            dayCount++;
            long blocksInDay = progressState.progress().get() - progressAtStart;
            long remaining = numOfDays - dayCount;
            progressState.totalProgress().set(progressState.progress().get() + (remaining * blocksInDay));
        }

        PrettyPrint.clearProgress();
        System.out.println("Fix records complete. Days processed: " + dayCount + ", Blocks processed: "
                + progressState.progress().get() + ", Blocks fixed: " + totalBlocksFixed);
    }

    private long fixDayRecords(int dayIndex, DayWork dayWork, int numOfDays, MainNetBucket bucket, ProgressState ps) {
        final String progressPrefix = String.format("Day %d of %d (%s)", dayIndex + 1, numOfDays, dayWork.day());
        final String dayFileName = dayWork.day().toString() + ".tar.zstd";
        final Path srcDayPath = compressedDaysDir.toPath().resolve(dayFileName);
        final Path dstDayPath = fixedCompressedDaysDir.toPath().resolve(dayFileName);
        final AtomicLong blocksFixed = new AtomicLong(0);

        try (Stream<UnparsedRecordBlock> stream = TarZstdDayReaderUsingExec.streamTarZstd(srcDayPath);
                ConcurrentTarZstdWriter writer = new ConcurrentTarZstdWriter(dstDayPath)) {
            final AtomicLong lastReportedMinute = new AtomicLong(Long.MIN_VALUE);
            stream.forEach((UnparsedRecordBlock block) -> {
                InMemoryFile primaryRecord = block.primaryRecordFile();
                if (primaryRecord == null) {
                    primaryRecord = downloadMissingRecord(block, dayWork.bucketRecords(), bucket);
                    blocksFixed.incrementAndGet();
                }
                writeBlock(writer, primaryRecord, block);
                ps.progress().incrementAndGet();
                reportProgress(block.recordFileTime(), progressPrefix, ps, lastReportedMinute);
            });
        } catch (Exception e) {
            throw new IllegalStateException("Failed to process day archive: " + srcDayPath, e);
        }

        PrettyPrint.clearProgress();
        System.out.println(
                Ansi.AUTO.string("@|yellow \u26a0 Old archive should be manually deleted: " + srcDayPath + "|@"));
        return blocksFixed.get();
    }

    /** Download a missing primary record file from GCP. */
    private InMemoryFile downloadMissingRecord(
            UnparsedRecordBlock block, Map<Instant, Set<String>> bucketRecords, MainNetBucket bucket) {
        final Instant blockTime = block.recordFileTime();
        final String blockTimeFileStr = convertInstantToStringWithPadding(blockTime);
        Set<String> nodesWithRecord = bucketRecords.get(blockTime);
        if (nodesWithRecord == null || nodesWithRecord.isEmpty()) {
            PrettyPrint.clearProgress();
            System.out.println(Ansi.AUTO.string(
                    "@|red Error: No record files found in bucket for block time: " + blockTime + "|@"));
            throw new IllegalStateException("No record files found in bucket for block time: " + blockTime);
        }
        String nodeId = nodesWithRecord.iterator().next();
        String recordBucketPath = "recordstreams/record" + nodeId + "/" + blockTimeFileStr + ".rcd";
        byte[] recordData;
        try {
            recordData = bucket.download(recordBucketPath);
        } catch (Exception e) {
            recordData = bucket.download(recordBucketPath + ".gz");
        }
        PrettyPrint.clearProgress();
        System.out.println(Ansi.AUTO.string(
                "@|green Fixed missing record for block " + blockTime + " (downloaded from node " + nodeId + ")|@"));
        return new InMemoryFile(Path.of(blockTimeFileStr + "/" + blockTimeFileStr + ".rcd"), recordData);
    }

    /** Write all files for a block to the output archive. */
    private static void writeBlock(
            ConcurrentTarZstdWriter writer, InMemoryFile primaryRecord, UnparsedRecordBlock block) {
        writer.putEntry(primaryRecord);
        block.signatureFiles().forEach(writer::putEntry);
        block.otherRecordFiles().forEach(writer::putEntry);
        block.primarySidecarFiles().forEach(writer::putEntry);
        block.otherSidecarFiles().forEach(writer::putEntry);
    }

    /** Report progress with speed and ETA once per consensus-minute. */
    private static void reportProgress(
            Instant blockTime, String progressPrefix, ProgressState ps, AtomicLong lastReportedMinute) {
        long blockMinute = blockTime.getEpochSecond() / 60L;
        if (blockMinute == lastReportedMinute.get()) return;

        final long currentNanos = System.nanoTime();
        String speedString = computeSpeed(blockTime, currentNanos, ps);
        final String progressString = String.format("%s %s%s", progressPrefix, blockTime, speedString);
        final long elapsedMillis = (currentNanos - ps.startNanos()) / 1_000_000L;
        final long processed = ps.progress().get();
        final long total = ps.totalProgress().get();
        double percent = ((double) processed / (double) total) * 100.0;
        long remainingMillis = PrettyPrint.computeRemainingMilliseconds(processed, total, elapsedMillis);
        PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
        lastReportedMinute.set(blockMinute);
    }

    /** Compute speed string based on wall clock vs consensus time ratio. */
    private static String computeSpeed(Instant blockTime, long currentNanos, ProgressState ps) {
        if (ps.lastSpeedCalcBlockTime().get() == null) {
            ps.lastSpeedCalcBlockTime().set(blockTime);
            ps.lastSpeedCalcRealTimeNanos().set(currentNanos);
        }
        long realTimeSinceLastCalc =
                currentNanos - ps.lastSpeedCalcRealTimeNanos().get();
        if (realTimeSinceLastCalc >= 10_000_000_000L) {
            ps.lastSpeedCalcBlockTime().set(blockTime);
            ps.lastSpeedCalcRealTimeNanos().set(currentNanos);
        }
        if (realTimeSinceLastCalc >= 1_000_000_000L) {
            long dataMs =
                    blockTime.toEpochMilli() - ps.lastSpeedCalcBlockTime().get().toEpochMilli();
            long realMs = realTimeSinceLastCalc / 1_000_000L;
            return String.format(" speed %.1fx", (double) dataMs / (double) realMs);
        }
        return "";
    }
}
