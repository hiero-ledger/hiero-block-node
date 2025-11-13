// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.mirrornode;

import com.google.gson.GsonBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

/**
 * Information about the first and last block for a given day.
 */
public class DayBlockInfo {
    public static final Path DEFAULT_DAY_BLOCKS_PATH = Path.of("data/day_blocks.json");
    /** Representation of a calendar day and the first/last block info for that day (UTC). */
    public final int year;

    public final int month;
    public final int day;
    public long firstBlockNumber;
    public String firstBlockHash;
    public long lastBlockNumber;
    public String lastBlockHash;
    /**
     * Transient instants used to determine first/last blocks when CSV rows are out of order. These are not serialized
     * to JSON (transient) and are used only during aggregation.
     */
    public transient Instant firstBlockInstant;

    public transient Instant lastBlockInstant;

    /**
     * Create a Day entry.
     *
     * @param year             the UTC year
     * @param month            the UTC month (1-12)
     * @param day              the UTC day of month
     * @param firstBlockNumber the block number of the first block on this day
     * @param firstBlockHash   the running hash of the first block as hex string
     * @param lastBlockNumber  the block number of the last block on this day
     * @param lastBlockHash    the running hash of the last block as hex string
     */
    public DayBlockInfo(
            int year,
            int month,
            int day,
            long firstBlockNumber,
            String firstBlockHash,
            long lastBlockNumber,
            String lastBlockHash) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.firstBlockNumber = firstBlockNumber;
        this.firstBlockHash = firstBlockHash;
        this.lastBlockNumber = lastBlockNumber;
        this.lastBlockHash = lastBlockHash;
    }

    /**
     * Load the day block info map from a default JSON file path.
     *
     * @return a map of LocalDate to DayBlockInfo
     */
    public static Map<LocalDate, DayBlockInfo> loadDayBlockInfoMap() {
        return loadDayBlockInfoMap(DEFAULT_DAY_BLOCKS_PATH);
    }

    /**
     * Load the day block info map from a JSON file.
     *
     * @param dayBlocksJsonFilePath the path to the day blocks JSON file
     * @return a map of LocalDate to DayBlockInfo
     */
    public static Map<LocalDate, DayBlockInfo> loadDayBlockInfoMap(Path dayBlocksJsonFilePath) {
        try (var reader = Files.newBufferedReader(dayBlocksJsonFilePath)) {
            final DayBlockInfo[] dayBlockInfoArray = new GsonBuilder().create().fromJson(reader, DayBlockInfo[].class);
            Map<LocalDate, DayBlockInfo> dayBlockInfoMap = new java.util.HashMap<>();
            for (DayBlockInfo dayBlockInfo : dayBlockInfoArray) {
                LocalDate date = LocalDate.of(dayBlockInfo.year, dayBlockInfo.month, dayBlockInfo.day);
                dayBlockInfoMap.put(date, dayBlockInfo);
            }
            return dayBlockInfoMap;
        } catch (Exception e) {
            throw new RuntimeException("Error loading day block info from " + dayBlocksJsonFilePath, e);
        }
    }
}
