// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.metadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Command to display summary information about metadata files.
 *
 * <p>Shows status and summary for:
 * <ul>
 *   <li>block_times.bin - block number to timestamp mapping</li>
 *   <li>day_blocks.json - daily block info with first/last blocks</li>
 *   <li>listingsByDay - directory of daily bucket listings</li>
 * </ul>
 */
@Command(name = "ls", description = "Display summary information about metadata files")
public class MetadataLsCommand implements Runnable {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Option(
            names = {"-d", "--dir"},
            description = "Base directory for metadata files (default: ${DEFAULT-VALUE})")
    private Path metadataDir = MetadataFiles.METADATA_DIR;

    /** Default constructor. */
    public MetadataLsCommand() {}

    @Override
    public void run() {
        printHeader();

        Path blockTimesFile = metadataDir.resolve("block_times.bin");
        Path dayBlocksFile = metadataDir.resolve("day_blocks.json");
        Path listingsDir = metadataDir.resolve("listingsByDay");

        // Block Times Summary
        printBlockTimesSummary(blockTimesFile);

        // Day Blocks Summary
        printDayBlocksSummary(dayBlocksFile);

        // Listings By Day Summary
        printListingsByDaySummary(listingsDir);
    }

    private void printHeader() {
        System.out.println();
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   METADATA FILES SUMMARY|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println();
    }

    private void printSectionHeader(String title) {
        System.out.println(Ansi.AUTO.string("@|bold,blue ▶ " + title + "|@"));
        System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────────|@"));
    }

    private void printBlockTimesSummary(Path blockTimesFile) {
        printSectionHeader("block_times.bin");

        if (!Files.exists(blockTimesFile)) {
            System.out.println(Ansi.AUTO.string("  @|red ✗ File not found: " + blockTimesFile + "|@"));
            System.out.println();
            return;
        }

        try (BlockTimeReader reader = new BlockTimeReader(blockTimesFile)) {
            long maxBlock = reader.getMaxBlockNumber();
            var firstTime = reader.getBlockLocalDateTime(0);
            var lastTime = reader.getBlockLocalDateTime(maxBlock);

            System.out.println(Ansi.AUTO.string("  @|green ✓ File exists|@"));
            System.out.println();
            System.out.println(Ansi.AUTO.string("  @|white Block Range:|@"));
            System.out.println(
                    Ansi.AUTO.string(String.format("    First Block: @|yellow %,d|@ at @|cyan %s|@", 0L, firstTime)));
            System.out.println(Ansi.AUTO.string(
                    String.format("    Last Block:  @|yellow %,d|@ at @|cyan %s|@", maxBlock, lastTime)));
            System.out.println(Ansi.AUTO.string(String.format("    Total Blocks: @|bold %,d|@", maxBlock + 1)));
        } catch (IOException e) {
            System.out.println(Ansi.AUTO.string("  @|red ✗ Error reading file: " + e.getMessage() + "|@"));
        }
        System.out.println();
    }

    private void printDayBlocksSummary(Path dayBlocksFile) {
        printSectionHeader("day_blocks.json");

        if (!Files.exists(dayBlocksFile)) {
            System.out.println(Ansi.AUTO.string("  @|red ✗ File not found: " + dayBlocksFile + "|@"));
            System.out.println();
            return;
        }

        try {
            Map<LocalDate, DayBlockInfo> dayBlockInfoMap = DayBlockInfo.loadDayBlockInfoMap(dayBlocksFile);

            if (dayBlockInfoMap.isEmpty()) {
                System.out.println(Ansi.AUTO.string("  @|yellow ⚠ File is empty|@"));
                System.out.println();
                return;
            }

            System.out.println(Ansi.AUTO.string("  @|green ✓ File exists|@"));
            System.out.println();

            // Find first and last days
            List<LocalDate> sortedDates =
                    dayBlockInfoMap.keySet().stream().sorted().toList();
            LocalDate firstDate = sortedDates.getFirst();
            LocalDate lastDate = sortedDates.getLast();
            DayBlockInfo firstDay = dayBlockInfoMap.get(firstDate);
            DayBlockInfo lastDay = dayBlockInfoMap.get(lastDate);

            System.out.println(Ansi.AUTO.string("  @|white Date Range:|@"));
            System.out.println(Ansi.AUTO.string(String.format(
                    "    First Day: @|cyan %s|@ (blocks @|yellow %,d|@ - @|yellow %,d|@)",
                    firstDate.format(DATE_FORMAT), firstDay.firstBlockNumber, firstDay.lastBlockNumber)));
            System.out.println(Ansi.AUTO.string(String.format(
                    "    Last Day:  @|cyan %s|@ (blocks @|yellow %,d|@ - @|yellow %,d|@)",
                    lastDate.format(DATE_FORMAT), lastDay.firstBlockNumber, lastDay.lastBlockNumber)));
            System.out.println(Ansi.AUTO.string(String.format("    Total Days: @|bold %,d|@", sortedDates.size())));
            System.out.println();

            // Validate continuity
            System.out.println(Ansi.AUTO.string("  @|white Validation:|@"));
            List<String> errors = validateDayBlocksContinuity(sortedDates, dayBlockInfoMap);

            if (errors.isEmpty()) {
                System.out.println(Ansi.AUTO.string("    @|green ✓ All blocks are continuous (no gaps)|@"));
            } else {
                System.out.println(Ansi.AUTO.string("    @|red ✗ Found " + errors.size() + " gap(s):|@"));
                for (String error : errors) {
                    System.out.println(Ansi.AUTO.string("      @|red • " + error + "|@"));
                }
            }
        } catch (Exception e) {
            System.out.println(Ansi.AUTO.string("  @|red ✗ Error reading file: " + e.getMessage() + "|@"));
        }
        System.out.println();
    }

    private List<String> validateDayBlocksContinuity(
            List<LocalDate> sortedDates, Map<LocalDate, DayBlockInfo> dayBlockInfoMap) {
        List<String> errors = new ArrayList<>();

        for (int i = 0; i < sortedDates.size() - 1; i++) {
            LocalDate currentDate = sortedDates.get(i);
            LocalDate nextDate = sortedDates.get(i + 1);
            DayBlockInfo current = dayBlockInfoMap.get(currentDate);
            DayBlockInfo next = dayBlockInfoMap.get(nextDate);

            // Check if next day's firstBlockNumber = current day's lastBlockNumber + 1
            long expectedNextFirst = current.lastBlockNumber + 1;
            if (next.firstBlockNumber != expectedNextFirst) {
                errors.add(String.format(
                        "%s->%s: expected block %,d but got %,d (gap of %,d)",
                        currentDate.format(DATE_FORMAT),
                        nextDate.format(DATE_FORMAT),
                        expectedNextFirst,
                        next.firstBlockNumber,
                        next.firstBlockNumber - expectedNextFirst));
            }
        }

        return errors;
    }

    private void printListingsByDaySummary(Path listingsDir) {
        printSectionHeader("listingsByDay");

        if (!Files.exists(listingsDir)) {
            System.out.println(Ansi.AUTO.string("  @|red ✗ Directory not found: " + listingsDir + "|@"));
            System.out.println();
            return;
        }

        if (!Files.isDirectory(listingsDir)) {
            System.out.println(Ansi.AUTO.string("  @|red ✗ Not a directory: " + listingsDir + "|@"));
            System.out.println();
            return;
        }

        try {
            // Find all day listing files (format: YYYY/MM/DD.bin)
            List<LocalDate> foundDates = new ArrayList<>();

            try (Stream<Path> yearDirs = Files.list(listingsDir)) {
                yearDirs.filter(Files::isDirectory)
                        .filter(p -> p.getFileName().toString().matches("\\d{4}"))
                        .forEach(yearDir -> {
                            try (Stream<Path> monthDirs = Files.list(yearDir)) {
                                monthDirs
                                        .filter(Files::isDirectory)
                                        .filter(p -> p.getFileName().toString().matches("\\d{2}"))
                                        .forEach(monthDir -> {
                                            try (Stream<Path> dayFiles = Files.list(monthDir)) {
                                                dayFiles.filter(Files::isRegularFile)
                                                        .filter(p -> p.getFileName()
                                                                .toString()
                                                                .matches("\\d{2}\\.bin"))
                                                        .forEach(dayFile -> {
                                                            int year = Integer.parseInt(yearDir.getFileName()
                                                                    .toString());
                                                            int month = Integer.parseInt(monthDir.getFileName()
                                                                    .toString());
                                                            int day = Integer.parseInt(dayFile.getFileName()
                                                                    .toString()
                                                                    .replace(".bin", ""));
                                                            foundDates.add(LocalDate.of(year, month, day));
                                                        });
                                            } catch (IOException e) {
                                                // Ignore errors listing files
                                            }
                                        });
                            } catch (IOException e) {
                                // Ignore errors listing directories
                            }
                        });
            }

            if (foundDates.isEmpty()) {
                System.out.println(Ansi.AUTO.string("  @|yellow ⚠ No listing files found|@"));
                System.out.println();
                return;
            }

            System.out.println(Ansi.AUTO.string("  @|green ✓ Directory exists|@"));
            System.out.println();

            // Sort dates
            foundDates.sort(LocalDate::compareTo);
            LocalDate firstDate = foundDates.getFirst();
            LocalDate lastDate = foundDates.getLast();

            System.out.println(Ansi.AUTO.string("  @|white Date Range:|@"));
            System.out.println(
                    Ansi.AUTO.string(String.format("    First Day: @|cyan %s|@", firstDate.format(DATE_FORMAT))));
            System.out.println(
                    Ansi.AUTO.string(String.format("    Last Day:  @|cyan %s|@", lastDate.format(DATE_FORMAT))));
            System.out.println(Ansi.AUTO.string(String.format("    Total Days: @|bold %,d|@", foundDates.size())));
            System.out.println();

            // Check for missing days
            System.out.println(Ansi.AUTO.string("  @|white Completeness:|@"));
            List<LocalDate> missingDates = findMissingDates(firstDate, lastDate, foundDates);

            if (missingDates.isEmpty()) {
                System.out.println(Ansi.AUTO.string("    @|green ✓ Complete (no missing days)|@"));
            } else {
                System.out.println(Ansi.AUTO.string("    @|red ✗ Found " + missingDates.size() + " missing day(s):|@"));
                // Show the first few missing dates
                int showCount = Math.min(5, missingDates.size());
                for (int i = 0; i < showCount; i++) {
                    System.out.println(Ansi.AUTO.string(
                            "      @|red • " + missingDates.get(i).format(DATE_FORMAT) + "|@"));
                }
                if (missingDates.size() > showCount) {
                    System.out.println(
                            Ansi.AUTO.string("      @|red • ... and " + (missingDates.size() - showCount) + " more|@"));
                }
            }
        } catch (IOException e) {
            System.out.println(Ansi.AUTO.string("  @|red ✗ Error reading directory: " + e.getMessage() + "|@"));
        }
        System.out.println();
    }

    private List<LocalDate> findMissingDates(LocalDate start, LocalDate end, List<LocalDate> foundDates) {
        List<LocalDate> missingDates = new ArrayList<>();
        LocalDate current = start;

        int foundIndex = 0;
        while (!current.isAfter(end)) {
            if (foundIndex < foundDates.size() && foundDates.get(foundIndex).equals(current)) {
                foundIndex++;
            } else {
                missingDates.add(current);
            }
            current = current.plusDays(1);
        }

        return missingDates;
    }
}
