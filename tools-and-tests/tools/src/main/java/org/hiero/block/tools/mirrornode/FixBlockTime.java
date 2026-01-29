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

            if (!validateInputs()) {
                return;
            }

            printHeader();

            long maxBlockInFile = getMaxBlockInFile();
            if (maxBlockInFile < 0 || toBlock > maxBlockInFile) {
                System.out.println(Ansi.AUTO.string(
                        "@|red Error: toBlock (" + toBlock + ") exceeds max block in file (" + maxBlockInFile + ")|@"));
                return;
            }

            int[] counts = processBlocks();
            printSummary(counts[0], counts[1]);

        } catch (IOException e) {
            System.out.println(Ansi.AUTO.string("@|red Error: " + e.getMessage() + "|@"));
            e.printStackTrace();
        }
    }

    private boolean validateInputs() {
        if (toBlock < fromBlock) {
            System.out.println(Ansi.AUTO.string(
                    "@|red Error: toBlock (" + toBlock + ") must be >= fromBlock (" + fromBlock + ")|@"));
            return false;
        }
        if (!Files.exists(blockTimesFile)) {
            System.out.println(
                    Ansi.AUTO.string("@|red Error: block_times.bin does not exist at " + blockTimesFile + "|@"));
            return false;
        }
        return true;
    }

    private void printHeader() {
        System.out.println(Ansi.AUTO.string("@|bold,green FixBlockTime - fixing block times from mirror node|@"));
        System.out.println(Ansi.AUTO.string("@|yellow blockTimesFile =|@ " + blockTimesFile));
        System.out.println(Ansi.AUTO.string("@|yellow fromBlock =|@ " + fromBlock));
        System.out.println(Ansi.AUTO.string("@|yellow toBlock =|@ " + toBlock));
        if (dryRun) {
            System.out.println(Ansi.AUTO.string("@|cyan DRY RUN - no changes will be made|@"));
        }
        System.out.println();
    }

    private long getMaxBlockInFile() throws IOException {
        long fileSize = Files.size(blockTimesFile);
        long maxBlockInFile = (fileSize / Long.BYTES) - 1;
        System.out.println(Ansi.AUTO.string("@|yellow Max block in file =|@ " + maxBlockInFile));
        return maxBlockInFile;
    }

    private int[] processBlocks() throws IOException {
        int fixedCount = 0;
        int unchangedCount = 0;

        try (RandomAccessFile raf = new RandomAccessFile(blockTimesFile.toFile(), dryRun ? "r" : "rw");
                BlockTimeReader reader = new BlockTimeReader(blockTimesFile)) {

            for (long blockNumber = fromBlock; blockNumber <= toBlock; blockNumber++) {
                int result = processBlock(blockNumber, raf, reader);
                if (result > 0) {
                    fixedCount++;
                } else if (result == 0) {
                    unchangedCount++;
                }
                // result < 0 means error, don't count
            }
        }
        return new int[] {fixedCount, unchangedCount};
    }

    private int processBlock(long blockNumber, RandomAccessFile raf, BlockTimeReader reader) throws IOException {
        Instant currentTime = reader.getBlockInstant(blockNumber);

        String url = MAINNET_MIRROR_NODE_API_URL + "blocks/" + blockNumber;
        JsonObject blockData = MirrorNodeUtils.readUrl(url);

        if (blockData == null || !blockData.has("name")) {
            System.out.println(
                    Ansi.AUTO.string("@|red Block " + blockNumber + ": Could not fetch data from mirror node|@"));
            return -1;
        }

        String recordFileName = blockData.get("name").getAsString();
        Instant correctTime = extractRecordFileTime(recordFileName);

        if (currentTime.equals(correctTime)) {
            if (fromBlock == toBlock || (blockNumber - fromBlock) % 100 == 0) {
                String correctTimeStr = DATE_TIME_FORMAT.format(correctTime);
                System.out.println(
                        Ansi.AUTO.string("@|green Block " + blockNumber + ": OK - " + correctTimeStr + " UTC|@"));
            }
            return 0;
        }

        printMismatch(blockNumber, currentTime, correctTime, recordFileName);
        if (!dryRun) {
            writeAndVerify(blockNumber, correctTime, raf);
        } else {
            System.out.println(Ansi.AUTO.string("  @|cyan (dry run - would fix)|@"));
        }
        System.out.println();
        return 1;
    }

    private void printMismatch(long blockNumber, Instant currentTime, Instant correctTime, String recordFileName) {
        String currentTimeStr = DATE_TIME_FORMAT.format(currentTime);
        String correctTimeStr = DATE_TIME_FORMAT.format(correctTime);
        System.out.println(Ansi.AUTO.string("@|yellow Block " + blockNumber + ":|@"));
        System.out.println(Ansi.AUTO.string(
                "  @|red Current:|@  " + currentTimeStr + " UTC (epoch: " + currentTime.getEpochSecond() + ")"));
        System.out.println(Ansi.AUTO.string(
                "  @|green Correct:|@  " + correctTimeStr + " UTC (epoch: " + correctTime.getEpochSecond() + ")"));
        System.out.println(Ansi.AUTO.string("  @|cyan Record file:|@ " + recordFileName));
    }

    private void writeAndVerify(long blockNumber, Instant correctTime, RandomAccessFile raf) throws IOException {
        long blockTimeLong = instantToBlockTimeLong(correctTime);
        long position = blockNumber * Long.BYTES;
        raf.seek(position);
        raf.writeLong(blockTimeLong);

        raf.seek(position);
        long writtenValue = raf.readLong();
        Instant verifyTime = RecordFileDates.blockTimeLongToInstant(writtenValue);
        if (verifyTime.equals(correctTime)) {
            System.out.println(Ansi.AUTO.string("  @|bold,green ✓ Fixed successfully|@"));
        } else {
            System.out.println(Ansi.AUTO.string("  @|bold,red ✗ Verification failed!|@"));
        }
    }

    private void printSummary(int fixedCount, int unchangedCount) {
        System.out.println();
        System.out.println(Ansi.AUTO.string("@|bold,cyan ═══════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan Summary|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan ═══════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|yellow Blocks checked:|@ " + (toBlock - fromBlock + 1)));
        System.out.println(Ansi.AUTO.string("@|yellow Blocks unchanged:|@ " + unchangedCount));
        System.out.println(Ansi.AUTO.string("@|yellow Blocks " + (dryRun ? "to fix" : "fixed") + ":|@ " + fixedCount));
    }

    /**
     * Programmatically fix block times for a range of blocks using batch queries.
     * This is called by download-live2 before batch downloads to ensure data correctness.
     *
     * <p>Uses batch queries to the mirror node API to fetch multiple blocks at once,
     * making it ~100x faster than querying individual blocks.
     *
     * @param blockTimesFile the path to block_times.bin
     * @param fromBlock starting block number
     * @param toBlock ending block number (inclusive)
     * @return number of blocks fixed
     */
    public static int fixBlockTimeRange(Path blockTimesFile, long fromBlock, long toBlock) {
        try {
            if (!Files.exists(blockTimesFile)) {
                System.err.println("[fixBlockTime] Error: block_times.bin does not exist");
                return 0;
            }

            long fileSize = Files.size(blockTimesFile);
            long maxBlockInFile = (fileSize / Long.BYTES) - 1;

            if (toBlock > maxBlockInFile) {
                // Extend file if needed - just fix up to what we have
                toBlock = maxBlockInFile;
            }

            if (fromBlock > toBlock) {
                return 0;
            }

            long totalBlocks = toBlock - fromBlock + 1;
            System.out.println("[fixBlockTime] Fixing block times for " + totalBlocks + " blocks (" + fromBlock + " to "
                    + toBlock + ") using batch queries...");

            try (RandomAccessFile raf = new RandomAccessFile(blockTimesFile.toFile(), "rw");
                    BlockTimeReader reader = new BlockTimeReader(blockTimesFile)) {

                int fixedCount = 0;
                int checkedCount = 0;
                long currentFrom = fromBlock;

                // Process in batches of 100 (mirror node limit)
                while (currentFrom <= toBlock) {
                    long currentTo = Math.min(currentFrom + 99, toBlock);

                    // Build batch query URL
                    String url = MAINNET_MIRROR_NODE_API_URL + "blocks"
                            + "?block.number=gte:" + currentFrom
                            + "&block.number=lte:" + currentTo
                            + "&limit=100&order=asc";

                    com.google.gson.JsonObject response = MirrorNodeUtils.readUrl(url);

                    if (response != null && response.has("blocks")) {
                        com.google.gson.JsonArray blocks = response.getAsJsonArray("blocks");

                        for (int i = 0; i < blocks.size(); i++) {
                            com.google.gson.JsonObject blockData = blocks.get(i).getAsJsonObject();

                            if (!blockData.has("number") || !blockData.has("name")) {
                                continue;
                            }

                            long blockNumber = blockData.get("number").getAsLong();
                            String recordFileName = blockData.get("name").getAsString();

                            try {
                                Instant currentTime = reader.getBlockInstant(blockNumber);
                                Instant correctTime = extractRecordFileTime(recordFileName);

                                if (!currentTime.equals(correctTime)) {
                                    // Write the correct value
                                    long blockTimeLong = instantToBlockTimeLong(correctTime);
                                    long position = blockNumber * Long.BYTES;
                                    raf.seek(position);
                                    raf.writeLong(blockTimeLong);
                                    fixedCount++;
                                }
                                checkedCount++;
                            } catch (Exception e) {
                                // Skip this block on error
                            }
                        }
                    }

                    currentFrom = currentTo + 1;

                    // Progress update every 10 batches (1000 blocks)
                    if (checkedCount % 1000 == 0 && checkedCount > 0) {
                        double percent = (100.0 * checkedCount) / totalBlocks;
                        System.out.printf(
                                "[fixBlockTime] Progress: %d/%d blocks (%.1f%%), %d fixed%n",
                                checkedCount, totalBlocks, percent, fixedCount);
                    }
                }

                System.out.println(
                        "[fixBlockTime] Completed: checked " + checkedCount + " blocks, fixed " + fixedCount);
                return fixedCount;
            }
        } catch (IOException e) {
            System.err.println("[fixBlockTime] Error: " + e.getMessage());
            return 0;
        }
    }
}
