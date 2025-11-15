// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.mirrornode;

import static org.hiero.block.tools.commands.mirrornode.MirrorNodeUtils.MAINNET_MIRROR_NODE_API_URL;
import static org.hiero.block.tools.records.RecordFileDates.extractRecordFileTime;
import static org.hiero.block.tools.records.RecordFileDates.instantToBlockTimeLong;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Update block data files (block_times.bin and day_blocks.json) with newer blocks from mirror node.
 */
@SuppressWarnings("DuplicatedCode")
@Command(
        name = "update",
        description = "Update block data files with newer blocks from mirror node",
        mixinStandardHelpOptions = true)
public class UpdateBlockData implements Runnable {

    /** The path to the block times file. */
    @Option(
            names = {"--block-times"},
            description = "Path to the block times \".bin\" file.")
    private Path blockTimesFile = Path.of("data/block_times.bin");

    /** The path to the day blocks file. */
    @Option(
            names = {"--day-blocks"},
            description = "Path to the day blocks \".json\" file.")
    private Path dayBlocksFile = DayBlockInfo.DEFAULT_DAY_BLOCKS_PATH;

    /** The batch size for fetching blocks from the mirror node. */
    private static final int BATCH_SIZE = 100;

    /**
     * Update block data files with newer blocks from the mirror node.
     */
    @Override
    public void run() {
        updateMirrorNodeData(blockTimesFile, dayBlocksFile);
    }

    /**
     * Update block data files (block_times.bin and day_blocks.json) with newer blocks from the mirror node.
     *
     * @param blockTimesFile the path to the block times file
     * @param dayBlocksFile  the path to the day blocks file
     * @return the latest block number
     */
    public static long updateMirrorNodeData(Path blockTimesFile, Path dayBlocksFile) {
        try {
            System.out.println(Ansi.AUTO.string("@|bold,green UpdateBlockData - reading existing block data files|@"));

            // Read the highest block number from block_times.bin
            long highestBlockInTimesFile = readHighestBlockFromTimesFile(blockTimesFile);
            System.out.println(
                    Ansi.AUTO.string("@|yellow Highest block in block_times.bin:|@ " + highestBlockInTimesFile));

            // Read the highest block number from day_blocks.json
            long highestBlockInDayBlocks = readHighestBlockFromDayBlocks(dayBlocksFile);
            System.out.println(
                    Ansi.AUTO.string("@|yellow Highest block in day_blocks.json:|@ " + highestBlockInDayBlocks));

            // Pick the minimum of the two
            long startBlockNumber = Math.min(highestBlockInTimesFile, highestBlockInDayBlocks) + 1;
            System.out.println(Ansi.AUTO.string("@|yellow Starting from block number:|@ " + startBlockNumber));

            // Query the latest block number from mirror node
            long latestBlockNumber = getLatestBlockNumber();
            System.out.println(
                    Ansi.AUTO.string("@|yellow Latest block number from mirror node:|@ " + latestBlockNumber));

            if (startBlockNumber > latestBlockNumber) {
                System.out.println(
                        Ansi.AUTO.string("@|bold,green Block data is already up to date. No updates needed.|@"));
                return latestBlockNumber;
            }

            // Load existing day blocks data
            Map<LocalDate, DayBlockInfo> dayBlocksMap = loadDayBlocksMap(dayBlocksFile);

            // Fetch and update blocks in batches
            long currentBlock = startBlockNumber;
            try (RandomAccessFile blockTimesRaf = new RandomAccessFile(blockTimesFile.toFile(), "rw")) {
                // Seek to the position to append new block times
                blockTimesRaf.seek(blockTimesRaf.length());

                while (currentBlock <= latestBlockNumber) {
                    long batchEndBlock = Math.min(currentBlock + BATCH_SIZE - 1, latestBlockNumber);
                    System.out.println(Ansi.AUTO.string(
                            "@|cyan Fetching blocks |@" + currentBlock + " @|cyan to|@ " + batchEndBlock));

                    // Fetch batch from mirror node
                    JsonArray blocks = fetchBlockBatch(currentBlock, BATCH_SIZE);

                    // Process each block in the batch
                    for (int i = 0; i < blocks.size(); i++) {
                        JsonObject block = blocks.get(i).getAsJsonObject();
                        long blockNumber = block.get("number").getAsLong();
                        String recordFileName = block.get("name").getAsString();
                        String blockHash = block.get("hash").getAsString();
                        // Strip "0x" prefix from hash if present
                        if (blockHash.startsWith("0x")) {
                            blockHash = blockHash.substring(2);
                        }

                        // Extract block time and convert to block time long
                        Instant blockInstant = extractRecordFileTime(recordFileName);
                        long blockTimeLong = instantToBlockTimeLong(blockInstant);

                        // Append to block_times.bin
                        blockTimesRaf.writeLong(blockTimeLong);

                        // Update day_blocks.json data
                        LocalDate blockDate =
                                blockInstant.atZone(ZoneOffset.UTC).toLocalDate();
                        updateDayBlockInfo(dayBlocksMap, blockDate, blockNumber, blockHash, blockInstant);

                        // Print progress every 1000 blocks or for the first few
                        if (blockNumber < (startBlockNumber + 5) || blockNumber % 1_000 == 0) {
                            System.out.println(Ansi.AUTO.string("@|yellow   Block|@ " + blockNumber
                                    + " @|yellow time:|@ " + blockInstant + " @|yellow file:|@ "
                                    + recordFileName));
                        }
                    }

                    // Flush the file periodically
                    blockTimesRaf.getChannel().force(false);

                    currentBlock = batchEndBlock + 1;
                }
            }

            // Write updated day_blocks.json
            writeDayBlocksJson(dayBlocksMap, dayBlocksFile);

            System.out.println(Ansi.AUTO.string("@|bold,green Update complete! Updated blocks from |@"
                    + startBlockNumber + " @|bold,green to|@ " + latestBlockNumber));
            // return the largest block number
            return latestBlockNumber;
        } catch (IOException e) {
            throw new RuntimeException("Error updating block data", e);
        }
    }

    /**
     * Read the highest block number from block_times.bin.
     *
     * @return the highest block number in the file, or -1 if the file is empty or doesn't exist
     */
    public static long readHighestBlockFromTimesFile(Path blockTimesFile) throws IOException {
        if (!Files.exists(blockTimesFile)) {
            System.out.println(Ansi.AUTO.string("@|yellow block_times.bin does not exist, starting from block 0|@"));
            return -1;
        }

        long binFileSize = Files.size(blockTimesFile);
        if (binFileSize == 0) {
            return -1;
        }

        // The highest block number is (file size / 8) - 1 since each long is 8 bytes
        return (binFileSize / Long.BYTES) - 1;
    }

    /**
     * Read the highest block number from day_blocks.json.
     *
     * @return the highest block number in the file, or -1 if the file is empty or doesn't exist
     */
    private static long readHighestBlockFromDayBlocks(Path dayBlocksFile) throws IOException {
        if (!Files.exists(dayBlocksFile)) {
            System.out.println(Ansi.AUTO.string("@|yellow day_blocks.json does not exist, starting from block 0|@"));
            return -1;
        }

        Map<LocalDate, DayBlockInfo> dayBlocksMap = DayBlockInfo.loadDayBlockInfoMap(dayBlocksFile);
        if (dayBlocksMap.isEmpty()) {
            return -1;
        }

        // Find the maximum lastBlockNumber across all days
        return dayBlocksMap.values().stream()
                .mapToLong(day -> day.lastBlockNumber)
                .max()
                .orElse(-1);
    }

    /**
     * Get the latest block number from the mirror node.
     *
     * @return the latest block number
     */
    private static long getLatestBlockNumber() {
        String url = MAINNET_MIRROR_NODE_API_URL + "blocks?limit=1&order=desc";
        JsonObject response = MirrorNodeUtils.readUrl(url);
        JsonArray blocks = response.getAsJsonArray("blocks");
        if (blocks.isEmpty()) {
            throw new RuntimeException("No blocks returned from mirror node");
        }
        return blocks.get(0).getAsJsonObject().get("number").getAsLong();
    }

    /**
     * Fetch a batch of blocks from the mirror node.
     *
     * @param startBlock the starting block number (inclusive)
     * @param limit      the maximum number of blocks to fetch
     * @return the JSON array of blocks
     */
    private static JsonArray fetchBlockBatch(long startBlock, int limit) {
        String url = MAINNET_MIRROR_NODE_API_URL + "blocks?block.number=gte%3A" + startBlock + "&limit=" + limit
                + "&order=asc";
        JsonObject response = MirrorNodeUtils.readUrl(url);
        return response.getAsJsonArray("blocks");
    }

    /**
     * Load the day blocks map from the JSON file.
     *
     * @return the map of LocalDate to DayBlockInfo
     */
    private static Map<LocalDate, DayBlockInfo> loadDayBlocksMap(Path dayBlocksFile) throws IOException {
        if (!Files.exists(dayBlocksFile)) {
            return new HashMap<>();
        }
        return DayBlockInfo.loadDayBlockInfoMap(dayBlocksFile);
    }

    /**
     * Update the day block info for a given date with a new block.
     *
     * @param dayBlocksMap the map of day blocks
     * @param date         the date of the block
     * @param blockNumber  the block number
     * @param blockHash    the block hash
     * @param blockInstant the block instant
     */
    private static void updateDayBlockInfo(
            Map<LocalDate, DayBlockInfo> dayBlocksMap,
            LocalDate date,
            long blockNumber,
            String blockHash,
            Instant blockInstant) {
        DayBlockInfo dayInfo = dayBlocksMap.get(date);
        if (dayInfo == null) {
            // Create new day info
            dayInfo = new DayBlockInfo(
                    date.getYear(),
                    date.getMonthValue(),
                    date.getDayOfMonth(),
                    blockNumber,
                    blockHash,
                    blockNumber,
                    blockHash);
            dayInfo.firstBlockInstant = blockInstant;
            dayInfo.lastBlockInstant = blockInstant;
            dayBlocksMap.put(date, dayInfo);
        } else {
            // Update existing day info
            if (dayInfo.firstBlockInstant == null || blockInstant.isBefore(dayInfo.firstBlockInstant)) {
                dayInfo.firstBlockNumber = blockNumber;
                dayInfo.firstBlockHash = blockHash;
                dayInfo.firstBlockInstant = blockInstant;
            }
            if (dayInfo.lastBlockInstant == null || blockInstant.isAfter(dayInfo.lastBlockInstant)) {
                dayInfo.lastBlockNumber = blockNumber;
                dayInfo.lastBlockHash = blockHash;
                dayInfo.lastBlockInstant = blockInstant;
            }
        }
    }

    /**
     * Write the day blocks map to the JSON file.
     *
     * @param dayBlocksMap the map of day blocks
     */
    private static void writeDayBlocksJson(Map<LocalDate, DayBlockInfo> dayBlocksMap, Path dayBlocksFile)
            throws IOException {
        // Convert map to sorted list
        List<DayBlockInfo> dayList = dayBlocksMap.values().stream()
                .sorted(Comparator.comparingInt((DayBlockInfo d) -> d.year)
                        .thenComparingInt(d -> d.month)
                        .thenComparingInt(d -> d.day))
                .toList();

        // Ensure parent directory exists
        Path parent = dayBlocksFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        // Write JSON
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(dayList);
        Files.writeString(dayBlocksFile, json, StandardCharsets.UTF_8);
        System.out.println(
                Ansi.AUTO.string("@|cyan Wrote|@ " + dayList.size() + " @|cyan day entries to|@ " + dayBlocksFile));
    }
}
