// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.utils.PrettyPrint.printProgress;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.hiero.block.tools.utils.ConcurrentTarZstdWriter;
import org.hiero.block.tools.utils.gcp.MainNetBucket;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Command to fix missing signatures in downloaded days
 */
@SuppressWarnings("FieldCanBeLocal")
@Command(
        name = "fix-missing-signatures",
        description = "Command to fix missing signatures in downloaded days",
        mixinStandardHelpOptions = true)
public class FixMissingSignatures implements Runnable {
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
                "@|green ✓ Found source date range: " + days.getFirst() + " to " + days.getLast() + "|@\n"));
        // Set up GCP bucket access
        final MainNetBucket bucket = new MainNetBucket(
                false, Path.of("metadata/gcp-cache"), minNodeAccountId, maxNodeAccountId, userProject);

        // Producer-consumer queue with bounded capacity for backpressure
        final BlockingQueue<DayWork> dayListingsQueue = new LinkedBlockingQueue<>(10);
        // start a background thread to fetch day bucket signatures
        Thread fetcherThread = new Thread(() -> {
            try {
                for (LocalDate day : days) {
                    dayListingsQueue.put(new DayWork(day, bucket.listSignatureFilesForDay(day.toString())));
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
        fetcherThread.start();
        // process each day
        int dayCount = 0;
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
            fixDaySignatures(numOfDays, dayCount, dayWork.day, bucket, dayWork.bucketSignatures);
            dayCount++;
        }
    }

    private record DayWork(LocalDate day, Map<String, Set<String>> bucketSignatures) {}

    private void fixDaySignatures(
            int numOfDays,
            int dayIndex,
            LocalDate day,
            MainNetBucket bucket,
            final Map<String, Set<String>> bucketSignatures) {
        final int progressOffset = dayIndex * ESTIMATE_BLOCKS_PER_DAY;
        final int totalEstimatedBlocks = numOfDays * ESTIMATE_BLOCKS_PER_DAY;
        final String progressPrefix = String.format("Day %d of %d (%s): ", dayIndex + 1, numOfDays, day);
        printProgress(progressOffset, totalEstimatedBlocks, progressPrefix);
        // Compute path to day file
        final String dayFileName = day.toString() + ".tar.zstd";
        final Path srcDayPath = compressedDaysDir.toPath().resolve(dayFileName);
        final Path dstDayPath = fixedCompressedDaysDir.toPath().resolve(dayFileName);

        try (Stream<UnparsedRecordBlock> stream = TarZstdDayReaderUsingExec.streamTarZstd(srcDayPath);
                ConcurrentTarZstdWriter writer = new ConcurrentTarZstdWriter(dstDayPath)) {
            // we have a stream of files and need to collect all files for a block
            AtomicInteger blockCounter = new AtomicInteger(0);
            stream.forEach((UnparsedRecordBlock block) -> {
                String blockTimeStr = block.recordFileTime().toString().replace(':', '_');
                // get expected signatures from bucket
                Set<String> expectedSignatures = bucketSignatures.get(blockTimeStr);
                if (expectedSignatures == null) {
                    throw new RuntimeException("No signatures found in bucket for block time: " + blockTimeStr);
                }
                // remove all signatures present in block
                for (InMemoryFile sigFile : block.signatureFiles()) {
                    final String sigFileName = sigFile.path().getFileName().toString(); // eg. node_0.0.10.rcd_sig
                    final String sigNodeId =
                            sigFileName.substring(sigFileName.indexOf("_") + 1, sigFileName.indexOf(".rc"));
                    expectedSignatures.remove(sigNodeId);
                }
                // what ever remains in expectedSignatures is missing from downloaded block, so download from bucket
                // gs://hedera-mainnet-streams/recordstreams/record0.0.3/2019-10-09T21_32_20.601587003Z.rcd_sig
                List<InMemoryFile> newSigFiles = expectedSignatures.stream()
                        .parallel()
                        .map((String sigNodeId) -> {
                            String sigBucketPath = "recordstreams/record" + sigNodeId + "/" + blockTimeStr + ".rcd_sig";
                            // example path 2024-06-18T00_00_00.001886911Z/node_0.0.10.rcd_sig
                            return new InMemoryFile(
                                    Path.of(blockTimeStr + "/node_" + sigNodeId + ".rcd_sig"),
                                    bucket.download(sigBucketPath));
                        })
                        .toList();
                // add the new signature files to the block
                block.signatureFiles().addAll(newSigFiles);
                // write the updated block to the new day file
                writer.putEntry(block.primaryRecordFile());
                block.signatureFiles().forEach(writer::putEntry);
                block.otherRecordFiles().forEach(writer::putEntry);
                block.primarySidecarFiles().forEach(writer::putEntry);
                block.otherSidecarFiles().forEach(writer::putEntry);
                // print progress
                printProgress(
                        progressOffset + blockCounter.incrementAndGet(),
                        totalEstimatedBlocks,
                        progressPrefix + " " + block.recordFileTime() + " processed");
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
