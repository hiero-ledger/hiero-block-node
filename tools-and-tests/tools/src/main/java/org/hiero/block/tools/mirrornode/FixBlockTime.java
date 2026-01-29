// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static org.hiero.block.tools.mirrornode.MirrorNodeUtils.MAINNET_MIRROR_NODE_API_URL;
import static org.hiero.block.tools.records.RecordFileDates.extractRecordFileTime;
import static org.hiero.block.tools.records.RecordFileDates.instantToBlockTimeLong;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.hiero.block.tools.metadata.MetadataFiles;
import org.hiero.block.tools.records.RecordFileDates;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Fix incorrect block time entries in block_times.bin by querying the mirror node
 * for the correct timestamp.
 */
@Command(
        name = "fixBlockTime",
        description = "Fix incorrect block time entries in block_times.bin by querying the mirror node",
        mixinStandardHelpOptions = true)
public class FixBlockTime implements Runnable {

    private static final DateTimeFormatter DATE_TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"));

    /** The path to the block times file. */
    @Option(
            names = {"--block-times"},
            description = "Path to the block times \".bin\" file.")
    private Path blockTimesFile = MetadataFiles.BLOCK_TIMES_FILE;

    @Option(
            names = {"-n", "--dry-run"},
            description = "Show what would be fixed without actually modifying the file")
    private boolean dryRun = false;

    @Parameters(index = "0", description = "Starting block number to fix")
    private long fromBlock;

    @Parameters(index = "1", description = "Ending block number to fix (inclusive)", defaultValue = "-1")
    private long toBlock = -1;

    @Override
    public void run() {
        try {
            // If toBlock not specified, just fix the single block
            if (toBlock == -1) {
                toBlock = fromBlock;
            }

            if (toBlock < fromBlock) {
                System.out.println(Ansi.AUTO.string(
                        "@|red Error: toBlock (" + toBlock + ") must be >= fromBlock (" + fromBlock + ")|@"));
                return;
            }

            System.out.println(Ansi.AUTO.string("@|bold,green FixBlockTime - fixing block times from mirror node|@"));
            System.out.println(Ansi.AUTO.string("@|yellow blockTimesFile =|@ " + blockTimesFile));
            System.out.println(Ansi.AUTO.string("@|yellow fromBlock =|@ " + fromBlock));
            System.out.println(Ansi.AUTO.string("@|yellow toBlock =|@ " + toBlock));
            if (dryRun) {
                System.out.println(Ansi.AUTO.string("@|cyan DRY RUN - no changes will be made|@"));
            }
            System.out.println();

            // Check file exists
            if (!Files.exists(blockTimesFile)) {
                System.out.println(
                        Ansi.AUTO.string("@|red Error: block_times.bin does not exist at " + blockTimesFile + "|@"));
                return;
            }

            // Check file size to see max block
            long fileSize = Files.size(blockTimesFile);
            long maxBlockInFile = (fileSize / Long.BYTES) - 1;
            System.out.println(Ansi.AUTO.string("@|yellow Max block in file =|@ " + maxBlockInFile));

            if (toBlock > maxBlockInFile) {
                System.out.println(Ansi.AUTO.string(
                        "@|red Error: toBlock (" + toBlock + ") exceeds max block in file (" + maxBlockInFile + ")|@"));
                return;
            }

            // Open file for reading current values and writing fixes
            try (RandomAccessFile raf = new RandomAccessFile(blockTimesFile.toFile(), dryRun ? "r" : "rw");
                    BlockTimeReader reader = new BlockTimeReader(blockTimesFile)) {

                int fixedCount = 0;
                int unchangedCount = 0;

                for (long blockNumber = fromBlock; blockNumber <= toBlock; blockNumber++) {
                    // Get current value from file
                    Instant currentTime = reader.getBlockInstant(blockNumber);
                    String currentTimeStr = DATE_TIME_FORMAT.format(currentTime);

                    // Query mirror node for correct value
                    String url = MAINNET_MIRROR_NODE_API_URL + "blocks/" + blockNumber;
                    JsonObject blockData = MirrorNodeUtils.readUrl(url);

                    if (blockData == null || !blockData.has("name")) {
                        System.out.println(Ansi.AUTO.string(
                                "@|red Block " + blockNumber + ": Could not fetch data from mirror node|@"));
                        continue;
                    }

                    String recordFileName = blockData.get("name").getAsString();
                    Instant correctTime = extractRecordFileTime(recordFileName);
                    String correctTimeStr = DATE_TIME_FORMAT.format(correctTime);

                    // Compare times
                    if (currentTime.equals(correctTime)) {
                        unchangedCount++;
                        if (fromBlock == toBlock || (blockNumber - fromBlock) % 100 == 0) {
                            System.out.println(Ansi.AUTO.string(
                                    "@|green Block " + blockNumber + ": OK - " + correctTimeStr + " UTC|@"));
                        }
                    } else {
                        fixedCount++;
                        System.out.println(Ansi.AUTO.string("@|yellow Block " + blockNumber + ":|@"));
                        System.out.println(Ansi.AUTO.string("  @|red Current:|@  " + currentTimeStr + " UTC (epoch: "
                                + currentTime.getEpochSecond() + ")"));
                        System.out.println(Ansi.AUTO.string("  @|green Correct:|@  " + correctTimeStr + " UTC (epoch: "
                                + correctTime.getEpochSecond() + ")"));
                        System.out.println(Ansi.AUTO.string("  @|cyan Record file:|@ " + recordFileName));

                        if (!dryRun) {
                            // Write the correct value
                            long blockTimeLong = instantToBlockTimeLong(correctTime);
                            long position = blockNumber * Long.BYTES;
                            raf.seek(position);
                            raf.writeLong(blockTimeLong);

                            // Verify the write
                            raf.seek(position);
                            long writtenValue = raf.readLong();
                            Instant verifyTime = RecordFileDates.blockTimeLongToInstant(writtenValue);
                            if (verifyTime.equals(correctTime)) {
                                System.out.println(Ansi.AUTO.string("  @|bold,green ✓ Fixed successfully|@"));
                            } else {
                                System.out.println(Ansi.AUTO.string("  @|bold,red ✗ Verification failed!|@"));
                            }
                        } else {
                            System.out.println(Ansi.AUTO.string("  @|cyan (dry run - would fix)|@"));
                        }
                        System.out.println();
                    }
                }

                // Summary
                System.out.println();
                System.out.println(Ansi.AUTO.string("@|bold,cyan ═══════════════════════════════════════════════|@"));
                System.out.println(Ansi.AUTO.string("@|bold,cyan Summary|@"));
                System.out.println(Ansi.AUTO.string("@|bold,cyan ═══════════════════════════════════════════════|@"));
                System.out.println(Ansi.AUTO.string("@|yellow Blocks checked:|@ " + (toBlock - fromBlock + 1)));
                System.out.println(Ansi.AUTO.string("@|yellow Blocks unchanged:|@ " + unchangedCount));
                System.out.println(
                        Ansi.AUTO.string("@|yellow Blocks " + (dryRun ? "to fix" : "fixed") + ":|@ " + fixedCount));
            }

        } catch (IOException e) {
            System.out.println(Ansi.AUTO.string("@|red Error: " + e.getMessage() + "|@"));
            e.printStackTrace();
        }
    }
}
