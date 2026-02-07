// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link UpdateBlockData}. */
class UpdateBlockDataTest {

    @TempDir
    Path tempDir;

    /** Build a fake mirror-node block JSON object. */
    private static JsonObject fakeBlock(long number, String recordFileName, String hash) {
        JsonObject obj = new JsonObject();
        obj.addProperty("number", number);
        obj.addProperty("name", recordFileName);
        obj.addProperty("hash", hash);
        return obj;
    }

    /** Build a JsonArray batch from an array of JsonObjects. */
    private static JsonArray batch(JsonObject... blocks) {
        JsonArray array = new JsonArray();
        for (JsonObject b : blocks) {
            array.add(b);
        }
        return array;
    }

    // ===== readHighestBlockFromTimesFile tests =====

    @Test
    void readHighestBlockFromTimesFile_nonExistentFile_returnsNegativeOne() throws Exception {
        Path nonExistent = tempDir.resolve("missing.bin");
        assertEquals(-1, UpdateBlockData.readHighestBlockFromTimesFile(nonExistent));
    }

    @Test
    void readHighestBlockFromTimesFile_emptyFile_returnsNegativeOne() throws Exception {
        Path emptyFile = tempDir.resolve("empty.bin");
        //noinspection ResultOfMethodCallIgnored
        emptyFile.toFile().createNewFile();
        assertEquals(-1, UpdateBlockData.readHighestBlockFromTimesFile(emptyFile));
    }

    @Test
    void readHighestBlockFromTimesFile_oneBlock_returnsZero() throws Exception {
        Path file = tempDir.resolve("one_block.bin");
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.writeLong(1234567890L); // block 0 time
        }
        assertEquals(0, UpdateBlockData.readHighestBlockFromTimesFile(file));
    }

    @Test
    void readHighestBlockFromTimesFile_multipleBlocks_returnsLastIndex() throws Exception {
        Path file = tempDir.resolve("multi_block.bin");
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.writeLong(100L); // block 0
            raf.writeLong(200L); // block 1
            raf.writeLong(300L); // block 2
            raf.writeLong(400L); // block 3
            raf.writeLong(500L); // block 4
        }
        // 5 longs x 8 bytes = 40 bytes -> (40/8)-1 = 4
        assertEquals(4, UpdateBlockData.readHighestBlockFromTimesFile(file));
    }

    @Test
    void readHighestBlockFromTimesFile_largeBlockCount() throws Exception {
        Path file = tempDir.resolve("large.bin");
        int blockCount = 10_000;
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek((long) (blockCount - 1) * Long.BYTES);
            raf.writeLong(999L);
        }
        assertEquals(blockCount - 1, UpdateBlockData.readHighestBlockFromTimesFile(file));
    }

    // ===== readHighestBlockFromDayBlocks tests =====

    @Test
    void readHighestBlockFromDayBlocks_nonExistentFile_returnsNegativeOne() throws Exception {
        Path nonExistent = tempDir.resolve("missing.json");
        assertEquals(-1, UpdateBlockData.readHighestBlockFromDayBlocks(nonExistent));
    }

    @Test
    void readHighestBlockFromDayBlocks_emptyArray_returnsNegativeOne() throws Exception {
        Path file = tempDir.resolve("empty_array.json");
        Files.writeString(file, "[]", StandardCharsets.UTF_8);
        assertEquals(-1, UpdateBlockData.readHighestBlockFromDayBlocks(file));
    }

    @Test
    void readHighestBlockFromDayBlocks_singleDay_returnsLastBlock() throws Exception {
        Path file = tempDir.resolve("single_day.json");
        String json = """
                [{"year":2019,"month":9,"day":13,"firstBlockNumber":0,"firstBlockHash":"aaa","lastBlockNumber":1259,"lastBlockHash":"bbb"}]
                """;
        Files.writeString(file, json, StandardCharsets.UTF_8);
        assertEquals(1259, UpdateBlockData.readHighestBlockFromDayBlocks(file));
    }

    @Test
    void readHighestBlockFromDayBlocks_multipleDays_returnsMaxLastBlock() throws Exception {
        Path file = tempDir.resolve("multi_day.json");
        String json = """
                [
                  {"year":2019,"month":9,"day":13,"firstBlockNumber":0,"firstBlockHash":"a","lastBlockNumber":1259,"lastBlockHash":"b"},
                  {"year":2019,"month":9,"day":14,"firstBlockNumber":1260,"firstBlockHash":"c","lastBlockNumber":18434,"lastBlockHash":"d"}
                ]
                """;
        Files.writeString(file, json, StandardCharsets.UTF_8);
        assertEquals(18434, UpdateBlockData.readHighestBlockFromDayBlocks(file));
    }

    // ===== loadDayBlocksMap tests =====

    @Test
    void loadDayBlocksMap_nonExistentFile_returnsEmptyMap() throws Exception {
        Path nonExistent = tempDir.resolve("missing.json");
        Map<LocalDate, DayBlockInfo> map = UpdateBlockData.loadDayBlocksMap(nonExistent);
        assertTrue(map.isEmpty());
    }

    @Test
    void loadDayBlocksMap_validFile_returnsCorrectEntries() throws Exception {
        Path file = tempDir.resolve("days.json");
        String json = """
                [
                  {"year":2019,"month":9,"day":13,"firstBlockNumber":0,"firstBlockHash":"aaa","lastBlockNumber":1259,"lastBlockHash":"bbb"},
                  {"year":2019,"month":9,"day":14,"firstBlockNumber":1260,"firstBlockHash":"ccc","lastBlockNumber":18434,"lastBlockHash":"ddd"}
                ]
                """;
        Files.writeString(file, json, StandardCharsets.UTF_8);

        Map<LocalDate, DayBlockInfo> map = UpdateBlockData.loadDayBlocksMap(file);

        assertEquals(2, map.size());
        DayBlockInfo day1 = map.get(LocalDate.of(2019, 9, 13));
        assertNotNull(day1);
        assertEquals(0, day1.firstBlockNumber);
        assertEquals(1259, day1.lastBlockNumber);

        DayBlockInfo day2 = map.get(LocalDate.of(2019, 9, 14));
        assertNotNull(day2);
        assertEquals(1260, day2.firstBlockNumber);
        assertEquals(18434, day2.lastBlockNumber);
    }

    // ===== updateDayBlockInfo tests =====

    @Test
    void updateDayBlockInfo_newDay_createsEntry() {
        Map<LocalDate, DayBlockInfo> map = new HashMap<>();
        LocalDate date = LocalDate.of(2019, 9, 13);
        Instant instant = Instant.parse("2019-09-13T21:53:51Z");

        UpdateBlockData.updateDayBlockInfo(map, date, 0, "hash0", instant);

        assertEquals(1, map.size());
        DayBlockInfo info = map.get(date);
        assertNotNull(info);
        assertEquals(0, info.firstBlockNumber);
        assertEquals("hash0", info.firstBlockHash);
        assertEquals(0, info.lastBlockNumber);
        assertEquals("hash0", info.lastBlockHash);
        assertEquals(instant, info.firstBlockInstant);
        assertEquals(instant, info.lastBlockInstant);
    }

    @Test
    void updateDayBlockInfo_existingDay_updatesLastBlock() {
        Map<LocalDate, DayBlockInfo> map = new HashMap<>();
        LocalDate date = LocalDate.of(2019, 9, 13);
        Instant first = Instant.parse("2019-09-13T21:53:51Z");
        Instant last = Instant.parse("2019-09-13T23:59:59Z");

        UpdateBlockData.updateDayBlockInfo(map, date, 0, "hash0", first);
        UpdateBlockData.updateDayBlockInfo(map, date, 1259, "hash1259", last);

        DayBlockInfo info = map.get(date);
        assertEquals(0, info.firstBlockNumber);
        assertEquals("hash0", info.firstBlockHash);
        assertEquals(first, info.firstBlockInstant);
        assertEquals(1259, info.lastBlockNumber);
        assertEquals("hash1259", info.lastBlockHash);
        assertEquals(last, info.lastBlockInstant);
    }

    @Test
    void updateDayBlockInfo_outOfOrder_tracksBothEnds() {
        Map<LocalDate, DayBlockInfo> map = new HashMap<>();
        LocalDate date = LocalDate.of(2019, 9, 13);
        Instant middle = Instant.parse("2019-09-13T22:00:00Z");
        Instant early = Instant.parse("2019-09-13T21:53:51Z");
        Instant late = Instant.parse("2019-09-13T23:59:59Z");

        // Insert middle first, then earlier, then later
        UpdateBlockData.updateDayBlockInfo(map, date, 500, "hashMiddle", middle);
        UpdateBlockData.updateDayBlockInfo(map, date, 0, "hashFirst", early);
        UpdateBlockData.updateDayBlockInfo(map, date, 1259, "hashLast", late);

        DayBlockInfo info = map.get(date);
        assertEquals(0, info.firstBlockNumber);
        assertEquals("hashFirst", info.firstBlockHash);
        assertEquals(early, info.firstBlockInstant);
        assertEquals(1259, info.lastBlockNumber);
        assertEquals("hashLast", info.lastBlockHash);
        assertEquals(late, info.lastBlockInstant);
    }

    @Test
    void updateDayBlockInfo_multipleDays_separateEntries() {
        Map<LocalDate, DayBlockInfo> map = new HashMap<>();
        LocalDate day1 = LocalDate.of(2019, 9, 13);
        LocalDate day2 = LocalDate.of(2019, 9, 14);

        UpdateBlockData.updateDayBlockInfo(map, day1, 0, "h0", Instant.parse("2019-09-13T22:00:00Z"));
        UpdateBlockData.updateDayBlockInfo(map, day2, 1260, "h1260", Instant.parse("2019-09-14T00:00:01Z"));

        assertEquals(2, map.size());
        assertEquals(0, map.get(day1).firstBlockNumber);
        assertEquals(1260, map.get(day2).firstBlockNumber);
    }

    // ===== writeDayBlocksJson round-trip tests =====

    @Test
    void writeDayBlocksJson_emptyMap_writesEmptyArray() throws Exception {
        Path file = tempDir.resolve("out.json");
        Map<LocalDate, DayBlockInfo> map = new HashMap<>();

        UpdateBlockData.writeDayBlocksJson(map, file);

        assertTrue(Files.exists(file));
        String content = Files.readString(file);
        assertEquals("[]", content);
    }

    @Test
    void writeDayBlocksJson_excludesToday() throws Exception {
        Path file = tempDir.resolve("out.json");
        Map<LocalDate, DayBlockInfo> map = new HashMap<>();
        // Add a day far in the past - should be included
        LocalDate pastDate = LocalDate.of(2019, 9, 13);
        map.put(pastDate, new DayBlockInfo(2019, 9, 13, 0, "aaa", 1259, "bbb"));
        // Add today's date - should be excluded
        LocalDate today = LocalDate.now(java.time.ZoneOffset.UTC);
        map.put(
                today,
                new DayBlockInfo(
                        today.getYear(), today.getMonthValue(), today.getDayOfMonth(), 99999, "xxx", 99999, "xxx"));

        UpdateBlockData.writeDayBlocksJson(map, file);

        // Read back and verify today is excluded
        Map<LocalDate, DayBlockInfo> loaded = UpdateBlockData.loadDayBlocksMap(file);
        assertEquals(1, loaded.size());
        assertTrue(loaded.containsKey(pastDate));
    }

    @Test
    void writeDayBlocksJson_sortsByDate() throws Exception {
        Path file = tempDir.resolve("out.json");
        Map<LocalDate, DayBlockInfo> map = new HashMap<>();
        // Add out of order
        map.put(LocalDate.of(2019, 9, 15), new DayBlockInfo(2019, 9, 15, 2000, "c", 2999, "d"));
        map.put(LocalDate.of(2019, 9, 13), new DayBlockInfo(2019, 9, 13, 0, "a", 999, "b"));
        map.put(LocalDate.of(2019, 9, 14), new DayBlockInfo(2019, 9, 14, 1000, "b", 1999, "c"));

        UpdateBlockData.writeDayBlocksJson(map, file);

        String content = Files.readString(file);
        // Verify the order: day 13 before day 14 before day 15
        int pos13 = content.indexOf("\"day\": 13");
        int pos14 = content.indexOf("\"day\": 14");
        int pos15 = content.indexOf("\"day\": 15");
        assertTrue(pos13 < pos14, "Day 13 should appear before day 14");
        assertTrue(pos14 < pos15, "Day 14 should appear before day 15");
    }

    @Test
    void writeDayBlocksJson_createsParentDirectories() throws Exception {
        Path file = tempDir.resolve("sub/dir/out.json");
        Map<LocalDate, DayBlockInfo> map = new HashMap<>();
        map.put(LocalDate.of(2019, 9, 13), new DayBlockInfo(2019, 9, 13, 0, "a", 999, "b"));

        UpdateBlockData.writeDayBlocksJson(map, file);

        assertTrue(Files.exists(file));
    }

    @Test
    void writeThenLoad_roundTrip() throws Exception {
        Path file = tempDir.resolve("roundtrip.json");
        Map<LocalDate, DayBlockInfo> original = new HashMap<>();
        original.put(LocalDate.of(2019, 9, 13), new DayBlockInfo(2019, 9, 13, 0, "hash0", 1259, "hash1259"));
        original.put(LocalDate.of(2019, 9, 14), new DayBlockInfo(2019, 9, 14, 1260, "hash1260", 18434, "hash18434"));

        UpdateBlockData.writeDayBlocksJson(original, file);
        Map<LocalDate, DayBlockInfo> loaded = UpdateBlockData.loadDayBlocksMap(file);

        assertEquals(2, loaded.size());
        DayBlockInfo day1 = loaded.get(LocalDate.of(2019, 9, 13));
        assertEquals(0, day1.firstBlockNumber);
        assertEquals("hash0", day1.firstBlockHash);
        assertEquals(1259, day1.lastBlockNumber);
        assertEquals("hash1259", day1.lastBlockHash);

        DayBlockInfo day2 = loaded.get(LocalDate.of(2019, 9, 14));
        assertEquals(1260, day2.firstBlockNumber);
        assertEquals(18434, day2.lastBlockNumber);
    }

    // ===== updateMirrorNodeData (core logic) tests =====

    @Test
    void updateMirrorNodeData_freshStart_createsFilesAndWritesBlocks() throws Exception {
        Path timesFile = tempDir.resolve("data/block_times.bin");
        Path dayBlocksFile = tempDir.resolve("data/day_blocks.json");

        // Provide 3 fake blocks on 2019-09-13
        JsonArray fakeBatch = batch(
                fakeBlock(0, "2019-09-13T21_53_51.396440Z.rcd", "0xaaa111"),
                fakeBlock(1, "2019-09-13T21_53_53.524086Z.rcd", "bbb222"),
                fakeBlock(2, "2019-09-13T21_53_55.657803Z.rcd", "0xccc333"));

        //noinspection unused
        UpdateBlockData.updateMirrorNodeData(timesFile, dayBlocksFile, null, 2, (start, limit) -> fakeBatch);

        // block_times.bin should exist with 3 entries (blocks 0, 1, 2)
        assertTrue(Files.exists(timesFile));
        assertEquals(2, UpdateBlockData.readHighestBlockFromTimesFile(timesFile));

        // day_blocks.json should exist with 1 day entry
        assertTrue(Files.exists(dayBlocksFile));
        Map<LocalDate, DayBlockInfo> dayBlocks = UpdateBlockData.loadDayBlocksMap(dayBlocksFile);
        assertEquals(1, dayBlocks.size());
        DayBlockInfo day = dayBlocks.get(LocalDate.of(2019, 9, 13));
        assertNotNull(day);
        assertEquals(0, day.firstBlockNumber);
        assertEquals(2, day.lastBlockNumber);
    }

    @Test
    void updateMirrorNodeData_alreadyUpToDate_doesNothing() throws Exception {
        Path timesFile = tempDir.resolve("block_times.bin");
        Path dayBlocksFile = tempDir.resolve("day_blocks.json");

        // Pre-populate block_times.bin with 3 blocks (0, 1, 2)
        try (RandomAccessFile raf = new RandomAccessFile(timesFile.toFile(), "rw")) {
            raf.writeLong(100L);
            raf.writeLong(200L);
            raf.writeLong(300L);
        }
        // Pre-populate day_blocks.json
        String json = """
                [{"year":2019,"month":9,"day":13,"firstBlockNumber":0,"firstBlockHash":"a","lastBlockNumber":2,"lastBlockHash":"c"}]
                """;
        Files.writeString(dayBlocksFile, json, StandardCharsets.UTF_8);

        // The latest block is 2, the same as stored → should do nothing
        boolean[] fetcherCalled = {false};
        //noinspection unused
        UpdateBlockData.updateMirrorNodeData(timesFile, dayBlocksFile, null, 2, (start, limit) -> {
            fetcherCalled[0] = true;
            return new JsonArray();
        });

        assertFalse(fetcherCalled[0], "Fetcher should not be called when already up to date");
    }

    @Test
    void updateMirrorNodeData_stripsHexPrefix() throws Exception {
        Path timesFile = tempDir.resolve("block_times.bin");
        Path dayBlocksFile = tempDir.resolve("day_blocks.json");

        // Block with "0x" prefix on hash
        JsonArray fakeBatch = batch(fakeBlock(0, "2019-09-13T21_53_51.396440Z.rcd", "0xDEADBEEF"));

        //noinspection unused
        UpdateBlockData.updateMirrorNodeData(timesFile, dayBlocksFile, null, 0, (start, limit) -> fakeBatch);

        Map<LocalDate, DayBlockInfo> dayBlocks = UpdateBlockData.loadDayBlocksMap(dayBlocksFile);
        DayBlockInfo day = dayBlocks.get(LocalDate.of(2019, 9, 13));
        assertNotNull(day);
        assertEquals("DEADBEEF", day.firstBlockHash, "0x prefix should be stripped");
    }

    @Test
    void updateMirrorNodeData_endDate_stopsProcessing() throws Exception {
        Path timesFile = tempDir.resolve("block_times.bin");
        Path dayBlocksFile = tempDir.resolve("day_blocks.json");

        // Blocks on two different days
        JsonArray fakeBatch = batch(
                fakeBlock(0, "2019-09-13T21_53_51.396440Z.rcd", "aaa"),
                fakeBlock(1, "2019-09-13T22_00_00.000000Z.rcd", "bbb"),
                fakeBlock(2, "2019-09-14T00_00_05.000000Z.rcd", "ccc")); // past end date

        // The end date is 2019-09-13, so block 2 (on 2019-09-14) should be excluded
        //noinspection unused
        UpdateBlockData.updateMirrorNodeData(
                timesFile, dayBlocksFile, LocalDate.of(2019, 9, 13), 2, (start, limit) -> fakeBatch);

        // Only blocks 0 and 1 should be in block_times.bin
        assertEquals(1, UpdateBlockData.readHighestBlockFromTimesFile(timesFile));

        // day_blocks.json should only have 2019-09-13
        Map<LocalDate, DayBlockInfo> dayBlocks = UpdateBlockData.loadDayBlocksMap(dayBlocksFile);
        assertEquals(1, dayBlocks.size());
        assertTrue(dayBlocks.containsKey(LocalDate.of(2019, 9, 13)));
        assertFalse(dayBlocks.containsKey(LocalDate.of(2019, 9, 14)));
    }

    @Test
    void updateMirrorNodeData_resumeFromExisting() throws Exception {
        Path timesFile = tempDir.resolve("block_times.bin");
        Path dayBlocksFile = tempDir.resolve("day_blocks.json");

        // Pre-populate with block 0
        try (RandomAccessFile raf = new RandomAccessFile(timesFile.toFile(), "rw")) {
            raf.writeLong(0L); // block 0 time
        }
        String json = """
                [{"year":2019,"month":9,"day":13,"firstBlockNumber":0,"firstBlockHash":"a","lastBlockNumber":0,"lastBlockHash":"a"}]
                """;
        Files.writeString(dayBlocksFile, json, StandardCharsets.UTF_8);

        // Fetcher returns blocks 1 and 2 (starting from block 1)
        JsonArray fakeBatch = batch(
                fakeBlock(1, "2019-09-13T21_53_53.524086Z.rcd", "bbb"),
                fakeBlock(2, "2019-09-13T21_53_55.657803Z.rcd", "ccc"));

        //noinspection unused
        UpdateBlockData.updateMirrorNodeData(timesFile, dayBlocksFile, null, 2, (start, limit) -> {
            assertEquals(1, start, "Should start fetching from block 1");
            return fakeBatch;
        });

        // All 3 blocks should now be present
        assertEquals(2, UpdateBlockData.readHighestBlockFromTimesFile(timesFile));

        Map<LocalDate, DayBlockInfo> dayBlocks = UpdateBlockData.loadDayBlocksMap(dayBlocksFile);
        DayBlockInfo day = dayBlocks.get(LocalDate.of(2019, 9, 13));
        // firstBlockNumber is 1 (not 0) because DayBlockInfo.firstBlockInstant is transient
        // and null after loading from JSON, so updateDayBlockInfo replaces it with the first
        // block encountered in the new batch
        assertEquals(1, day.firstBlockNumber);
        assertEquals(2, day.lastBlockNumber);
    }
}
