// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static org.hiero.block.tools.records.RecordFileDates.blockTimeLongToInstant;
import static org.hiero.block.tools.records.RecordFileDates.extractRecordFileTime;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.hiero.block.tools.records.RecordFileDates;
import org.hiero.block.tools.utils.gcp.MainNetBucket;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Add block times for blocks newer than mirror node data from GCP
 */
@SuppressWarnings("FieldCanBeLocal")
@Command(name = "addNewerBlockTimes", description = "Add block times for blocks newer than mirror node data from GCP")
public class AddNewerBlockTimes implements Runnable {
    @Option(
            names = {"-c", "--cache-enabled"},
            description = "Use local cache for downloaded content")
    private boolean cacheEnabled = true;

    @Option(
            names = {"--min-node-account-id"},
            description = "the account id of the first node in the network")
    private int minNodeAccountId = 3;

    @Option(
            names = {"--max-node-account-id"},
            description = "the account id of the last node in the network")
    private int maxNodeAccountId = 34;

    @Option(
            names = {"-d", "--data-dir"},
            description = "the data directory for output and temporary files")
    private Path dataDir = Path.of("data");

    /** The path to the block times file. */
    @Option(
            names = {"--block-times"},
            description = "Path to the block times \".bin\" file.")
    private Path blockTimesFile = Path.of("data/block_times.bin");

    /**
     * Add block times for blocks newer than mirror node data from GCP. This is done by listing the record files and
     * sorting and counting them.
     */
    @Override
    public void run() {
        try {
            System.out.println(
                    Ansi.AUTO.string("@|bold,green AddNewerBlockTimes - reading existing block times file data|@"));
            System.out.println(Ansi.AUTO.string("@|yellow blockTimesFile =|@ " + blockTimesFile));
            final long binFileSize = Files.size(blockTimesFile);
            // get last block number
            final int lastBlockNumberInFile = (int) (binFileSize / Long.BYTES) - 1;
            System.out.println(Ansi.AUTO.string("@|yellow lastBlockNumberInFile =|@ " + lastBlockNumberInFile));
            // read last long in block_times.bin
            final long lastBlockTime;
            try (RandomAccessFile raf = new RandomAccessFile(blockTimesFile.toFile(), "r")) {
                raf.seek(binFileSize - Long.BYTES);
                lastBlockTime = raf.readLong();
            }
            System.out.println(Ansi.AUTO.string("@|yellow lastBlockTime = |@" + lastBlockTime + " nanos @|yellow =|@ "
                    + RecordFileDates.blockTimeLongToInstant(lastBlockTime)));
            // Open connection to get data from mainnet GCP bucket
            final MainNetBucket mainNetBucket =
                    new MainNetBucket(cacheEnabled, dataDir.resolve("gcp-cache"), minNodeAccountId, maxNodeAccountId);

            // find the day containing the last block time
            final Instant lastBlockTimeInstant = blockTimeLongToInstant(lastBlockTime);
            final Instant lastBlockDay = lastBlockTimeInstant.truncatedTo(ChronoUnit.DAYS);
            System.out.println(Ansi.AUTO.string("@|yellow lastBlockDay = |@" + lastBlockDay));
            // create date formatter for output
            DateTimeFormatter dateFormatter =
                    DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));
            // loop over days from lastBlockDay to today
            Instant day = lastBlockDay;
            long blockNumber = lastBlockNumberInFile;
            try (RandomAccessFile raf = new RandomAccessFile(blockTimesFile.toFile(), "rw")) {
                raf.seek(raf.length());
                while (day.isBefore(Instant.now().truncatedTo(ChronoUnit.DAYS))) {
                    System.out.println(
                            Ansi.AUTO.string("@|bold,green,underline Processing day |@" + dateFormatter.format(day)));
                    // get listing of all files in bucket for current day
                    final List<String> allFilesInDay =
                            mainNetBucket.listDayFileNames(RecordFileDates.blockTimeInstantToLong(day));
                    for (String recordFileName : allFilesInDay) {
                        final Instant fileTime = extractRecordFileTime(recordFileName);
                        if (fileTime.isAfter(lastBlockTimeInstant)) {
                            blockNumber++;
                            if (blockNumber < (lastBlockNumberInFile + 5) || blockNumber % 1_000 == 0) {
                                System.out.println(Ansi.AUTO.string(
                                        "@|yellow blockNumber = |@" + blockNumber + " @|yellow fileTime = |@"
                                                + fileTime + " @|yellow recordFileName = |@"
                                                + recordFileName));
                            }
                            final long currentBlockTime = RecordFileDates.instantToBlockTimeLong(fileTime);
                            // append block time to block_times.bin
                            raf.writeLong(currentBlockTime);
                        }
                    }
                    // flush the file
                    raf.getChannel().force(false);
                    // double check last block number once each day
                    String blockFileNameFromMirrorNode = FetchBlockQuery.getRecordFileNameForBlock(blockNumber);
                    String lastRecordFileName = allFilesInDay.getLast();
                    System.out.println(Ansi.AUTO.string("@|cyan,bold Checking|@ @|yellow lastBlockNumberOfDay = |@"
                            + blockNumber + " @|yellow blockFileNameFromMirrorNode = |@"
                            + blockFileNameFromMirrorNode + " @|yellow lastRecordFileName = |@"
                            + lastRecordFileName));
                    if (!blockFileNameFromMirrorNode.equals(lastRecordFileName)) {
                        throw new RuntimeException("Last block of day number mismatch");
                    }
                    // next day
                    day = day.plus(1, ChronoUnit.DAYS);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
