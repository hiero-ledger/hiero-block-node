// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hiero.block.tools.config.NetworkConfig;
import org.hiero.block.tools.metadata.MetadataFiles;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Generate or update {@code day_blocks.json} by querying the Hedera Mirror Node REST API directly.
 *
 * <p>For each day in the requested range the command issues two API calls:
 * <ol>
 *   <li>One with {@code order=asc&limit=1} to find the first block of the day.</li>
 *   <li>One with {@code order=desc&limit=1} to find the last block of the day.</li>
 * </ol>
 *
 * <p>By default the command merges the fetched data into the existing {@code day_blocks.json}
 * (if it exists), overwriting only entries that fall within the requested date range.  Use
 * {@code --no-merge} to replace the entire file.
 */
@SuppressWarnings("FieldCanBeLocal")
@Command(
        name = "extractDayBlocksFromApi",
        description = "Generate or update day_blocks.json from the Hedera Mirror Node REST API",
        mixinStandardHelpOptions = true)
public class ExtractDayBlocksFromApi implements Runnable {

    /** Minimum milliseconds to wait between consecutive API requests to avoid rate-limiting. */
    private static final long REQUEST_DELAY_MS = 150;

    @Option(
            names = {"--start-date"},
            description = "First day to fetch (inclusive), format YYYY-MM-DD. Default: network genesis date.")
    private String startDate = NetworkConfig.current().genesisDate().toString();

    @Option(
            names = {"--end-date"},
            description = "Last day to fetch (inclusive), format YYYY-MM-DD. Default: yesterday (UTC) so today's"
                    + " incomplete day is excluded.")
    private String endDate = null; // resolved at runtime

    @Option(
            names = {"--day-blocks"},
            description = "Path to the output day_blocks.json file.")
    private Path dayBlocksFile = MetadataFiles.DAY_BLOCKS_FILE;

    @Option(
            names = {"--no-merge"},
            description = "Replace the entire output file instead of merging with existing data.")
    private boolean noMerge = false;

    @Option(
            names = {"--mirror-node-url"},
            description = "Base URL for the mirror node API (must end with '/').")
    private String mirrorNodeUrl = NetworkConfig.current().mirrorNodeApiUrl();

    @Override
    public void run() {
        final LocalDate start = LocalDate.parse(startDate);
        final LocalDate end = endDate != null
                ? LocalDate.parse(endDate)
                : LocalDate.now(ZoneOffset.UTC).minusDays(1); // yesterday UTC

        if (start.isAfter(end)) {
            System.err.println("--start-date (" + start + ") must not be after --end-date (" + end + ")");
            System.exit(1);
        }

        // Load existing data unless --no-merge was requested
        final Map<LocalDate, DayBlockInfo> existing = new HashMap<>();
        if (!noMerge && Files.exists(dayBlocksFile)) {
            try {
                existing.putAll(DayBlockInfo.loadDayBlockInfoMap(dayBlocksFile));
                System.out.println("Loaded " + existing.size() + " existing entries from " + dayBlocksFile);
            } catch (Exception e) {
                System.err.println("Warning: could not load existing " + dayBlocksFile + ": " + e.getMessage()
                        + " – starting fresh.");
            }
        }

        // Fetch data from the API for each day in [start, end]
        long totalDays = start.datesUntil(end.plusDays(1)).count();
        long processed = 0;
        long failed = 0;

        for (LocalDate day = start; !day.isAfter(end); day = day.plusDays(1)) {
            try {
                final DayBlockInfo info = fetchDayInfo(day);
                if (info != null) {
                    existing.put(day, info);
                } else {
                    System.err.println("  No blocks found for " + day + " – skipping.");
                    failed++;
                }
                processed++;
                // Progress report every 50 days
                if (processed % 50 == 0) {
                    System.out.printf("  Fetched %d / %d days...%n", processed, totalDays);
                }
                // Polite delay to avoid hitting API rate limits
                //noinspection BusyWait
                Thread.sleep(REQUEST_DELAY_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                System.err.println("Interrupted – writing partial results.");
                break;
            } catch (Exception e) {
                System.err.println("  Error fetching " + day + ": " + e.getMessage());
                failed++;
            }
        }

        // Write the merged result
        final List<DayBlockInfo> sorted = new ArrayList<>(existing.values());
        sorted.sort(Comparator.comparingInt((DayBlockInfo d) -> d.year)
                .thenComparingInt(d -> d.month)
                .thenComparingInt(d -> d.day));

        try {
            final Path parent = dayBlocksFile.getParent();
            if (parent != null) Files.createDirectories(parent);
            final String json = new GsonBuilder().setPrettyPrinting().create().toJson(sorted);
            Files.writeString(dayBlocksFile, json, StandardCharsets.UTF_8);
            System.out.printf(
                    "Wrote %d entries to %s (%d days fetched, %d failed).%n",
                    sorted.size(), dayBlocksFile, processed, failed);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write " + dayBlocksFile, e);
        }
    }

    /**
     * Fetch the first and last block for a calendar day via the mirror node REST API.
     *
     * @param day the UTC calendar day
     * @return a populated {@link DayBlockInfo}, or {@code null} if the day has no blocks
     */
    private DayBlockInfo fetchDayInfo(LocalDate day) {
        final long dayStartEpoch = day.atStartOfDay(ZoneOffset.UTC).toEpochSecond();
        final long dayEndEpoch = day.plusDays(1).atStartOfDay(ZoneOffset.UTC).toEpochSecond();
        final String tsGte = "gte:" + dayStartEpoch + ".000000000";
        final String tsLt = "lt:" + dayEndEpoch + ".000000000";

        final List<BlockInfo> firstBlocks =
                FetchBlockQuery.getLatestBlocks(1, MirrorNodeBlockQueryOrder.ASC, List.of(tsGte, tsLt), mirrorNodeUrl);
        if (firstBlocks.isEmpty()) return null;

        final List<BlockInfo> lastBlocks =
                FetchBlockQuery.getLatestBlocks(1, MirrorNodeBlockQueryOrder.DESC, List.of(tsGte, tsLt), mirrorNodeUrl);
        if (lastBlocks.isEmpty()) return null;

        final BlockInfo first = firstBlocks.getFirst();
        final BlockInfo last = lastBlocks.getFirst();

        return new DayBlockInfo(
                day.getYear(),
                day.getMonthValue(),
                day.getDayOfMonth(),
                first.number,
                first.hash,
                last.number,
                last.hash);
    }
}
