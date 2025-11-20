// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.zip.GZIPInputStream;
import org.hiero.block.tools.metadata.MetadataFiles;
import org.hiero.block.tools.records.RecordFileDates;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Read the record_file.csv.gz file from the mirror node and extract the block info for each day into a json file.
 * <p>
 * Json file contains an array of objects with the following fields:
 * <ul>
 *     <li>year: the UTC year</li>
 *     <li>month: the UTC month (1-12)</li>
 *     <li>day: the UTC day of the month</li>
 *     <li>firstBlockNumber: the block number of the first block on this day</li>
 *     <li>firstBlockHash: the running hash of the first block as hex string</li>
 *     <li>lastBlockNumber: the block number of the last block on this day</li>
 *     <li>lastBlockHash: the running hash of the last block as hex string</li>
 * </ul>
 * The array is sorted chronologically by year, month, day.
 * </p>
 */
@SuppressWarnings({"DuplicatedCode", "CallToPrintStackTrace"})
@Command(name = "extractDayBlock", description = "Extract block info for each day into json file")
public class ExtractDayBlocks implements Runnable {

    /** The path to download the record table CSVs from the mirror node to, gzipped. */
    @Option(
            names = {"--record-dir"},
            description = "Path to download the record table CSVs from mirror node to, gzipped.")
    private Path recordsCsvDir = Path.of("data/mirror_node_record_files");

    /** The path to the day blocks file. */
    @Option(
            names = {"--day-blocks"},
            description = "Path to the day blocks \".json\" file.")
    private Path blockTimesFile = MetadataFiles.DAY_BLOCKS_FILE;

    /**
     * Read the record file table CSV file and extract the block times into a file.
     */
    @Override
    public void run() {
        // Map to collect day -> first/last block info.
        final Map<LocalDate, DayBlockInfo> days = new HashMap<>();

        // find the record files subdirectory
        final Path recordsCsvRootPath = recordsCsvDir.toAbsolutePath();
        final String latestVersion;
        try (var listStream = Files.list(recordsCsvRootPath)) {
            latestVersion = listStream
                    .map(path -> path.getFileName().toString())
                    .filter(pathStr -> MirrorNodeUtils.SYMANTIC_VERSION_PATTERN
                            .matcher(pathStr)
                            .matches())
                    .max(Comparator.comparingLong(MirrorNodeUtils::parseSymantecVersion))
                    .orElseThrow();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final Path recordsCsvDirPath = recordsCsvRootPath.resolve(latestVersion).resolve("record_file");

        // read all record files in the directory
        try (var listStream = Files.list(recordsCsvDirPath)) {
            final List<Path> recordsCsvFiles = listStream
                    .filter(path -> path.getFileName().toString().startsWith("record_file"))
                    .filter(path -> path.getFileName().toString().endsWith(".csv.gz"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList();
            System.out.println("Found " + recordsCsvFiles.size() + " record CSV files in " + recordsCsvDirPath);

            for (Path recordsCsvFile : recordsCsvFiles) {
                try (var reader = new BufferedReader(
                        new InputStreamReader(new GZIPInputStream(new FileInputStream(recordsCsvFile.toFile()))))) {
                    final String headerLine = reader.readLine();
                    if (headerLine == null) continue;
                    final String[] headerLineParts = headerLine.split(",");
                    int recordStreamFileNameIndex = -1;
                    int blockNumberIndex = -1;
                    int hashIndex = -1;
                    for (int i = 0; i < headerLineParts.length; i++) {
                        final String h = headerLineParts[i];
                        switch (h) {
                            case "name" -> recordStreamFileNameIndex = i;
                            case "index" -> blockNumberIndex = i;
                            case "hash" -> hashIndex = i;
                        }
                    }
                    if (recordStreamFileNameIndex == -1 || blockNumberIndex == -1 || hashIndex == -1) {
                        throw new RuntimeException(
                                "Required CSV columns not found in " + recordsCsvFile + ": name,index,hash");
                    }

                    // Capture indices into final locals so they can be used inside the lambda (must be effectively
                    // final)
                    final int recordIndexFinal = recordStreamFileNameIndex;
                    final int blockIndexFinal = blockNumberIndex;
                    final int hashIndexFinal = hashIndex;

                    // process lines sequentially to preserve chronological order
                    reader.lines().forEach(line -> {
                        try {
                            final String[] parts = line.split(",");
                            if (parts.length <= Math.max(Math.max(recordIndexFinal, blockIndexFinal), hashIndexFinal)) {
                                // malformed line
                                return;
                            }
                            final String recordStreamFileName = parts[recordIndexFinal];
                            final long blockNumber = Long.parseLong(parts[blockIndexFinal]);
                            final String hashHex = parts[hashIndexFinal];

                            final Instant recordInstant = RecordFileDates.extractRecordFileTime(recordStreamFileName);
                            final LocalDate date =
                                    recordInstant.atZone(ZoneOffset.UTC).toLocalDate();

                            // Keep the hash as hex string for JSON output. Empty if missing.
                            final String hashHexNormalized = (hashHex == null) ? "" : hashHex;

                            final DayBlockInfo current = days.get(date);
                            if (current == null) {
                                // Create a new mutable Day for this date and initialize both instants to this record's
                                // instant
                                final DayBlockInfo newDay = new DayBlockInfo(
                                        date.getYear(),
                                        date.getMonthValue(),
                                        date.getDayOfMonth(),
                                        blockNumber,
                                        hashHexNormalized,
                                        blockNumber,
                                        hashHexNormalized);
                                newDay.firstBlockInstant = recordInstant;
                                newDay.lastBlockInstant = recordInstant;
                                days.put(date, newDay);
                            } else {
                                // CSV order may not be chronological; use the record instant to choose first/last
                                if (current.firstBlockInstant == null
                                        || recordInstant.isBefore(current.firstBlockInstant)) {
                                    current.firstBlockNumber = blockNumber;
                                    current.firstBlockHash = hashHexNormalized;
                                    current.firstBlockInstant = recordInstant;
                                }
                                if (current.lastBlockInstant == null
                                        || recordInstant.isAfter(current.lastBlockInstant)) {
                                    current.lastBlockNumber = blockNumber;
                                    current.lastBlockHash = hashHexNormalized;
                                    current.lastBlockInstant = recordInstant;
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Convert collected data to list of Day objects and sort chronologically
        final List<DayBlockInfo> dayList = days.values().stream()
                .sorted(Comparator.comparingInt((DayBlockInfo d) -> d.year)
                        .thenComparingInt(d -> d.month)
                        .thenComparingInt(d -> d.day))
                .toList();

        // Ensure parent directory exists
        final Path parent = blockTimesFile.getParent();
        try {
            if (parent != null) Files.createDirectories(parent);
            final Gson gson = new GsonBuilder().setPrettyPrinting().create();
            final String json = gson.toJson(dayList);
            Files.writeString(blockTimesFile, json, StandardCharsets.UTF_8);
            System.out.println("Wrote " + dayList.size() + " day entries to " + blockTimesFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
