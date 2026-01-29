// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.listing.DayListingFileWriter;
import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.metadata.MetadataFiles;
import org.hiero.block.tools.records.ChainFile;
import org.hiero.block.tools.records.RecordFileDates;
import org.hiero.block.tools.utils.PrettyPrint;
import org.hiero.block.tools.utils.gcp.MainNetBucket;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command to update day listing files by fetching file metadata from Google Cloud Storage.
 *
 * <p>This command scans the local listing directory to find the last complete day,
 * then downloads metadata for all subsequent days up to (but not including) today.
 * Today is excluded because the day is still in progress and would be incomplete.
 *
 * <p>The command lists files from the "hedera-mainnet-streams" GCS bucket, specifically
 * from the recordstreams directory which contains data from multiple consensus nodes
 * (e.g., record0.0.3 through record0.0.37). Each node directory contains record files
 * and a sidecar subdirectory.
 *
 * <p>Example usage:
 * <pre>
 * java -jar tools.jar days updateDayListings -l /path/to/listings
 * </pre>
 */
@SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal", "CallToPrintStackTrace", "unused"})
@Command(name = "updateDayListings", description = "Update day listing files by downloading metadata from GCS bucket")
public class UpdateDayListingsCommand implements Runnable {

    /** Date formatter for display (YYYY-MM-DD format). */
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Pattern to match day listing files like "13.bin". */
    static final Pattern DAY_FILE_PATTERN = Pattern.compile("(\\d{2})\\.bin");

    @Option(
            names = {"-l", "--listing-dir"},
            description = "Directory where listing files are stored (default: listingsByDay)")
    private Path listingDir = MetadataFiles.LISTINGS_DIR;

    @Option(
            names = {"-c", "--cache-dir"},
            description = "Directory for GCS cache (default: data/gcp-cache)")
    private File cacheDir = new File("data/gcp-cache");

    @Option(
            names = {"--cache"},
            description = "Enable GCS caching (default: false)")
    private boolean cacheEnabled = false;

    @Option(
            names = {"--min-node"},
            description = "Minimum node account ID (default: 3)")
    private int minNodeAccountId = 3;

    @Option(
            names = {"--max-node"},
            description = "Maximum node account ID (default: 37)")
    private int maxNodeAccountId = 37;

    @Option(
            names = {"-p", "--user-project"},
            description = "GCP project to bill for requester-pays bucket access (default: from GCP_PROJECT_ID env var)")
    private String userProject = DownloadConstants.GCP_PROJECT_ID;

    /**
     * Main entry point for the command. Discovers the last complete day on disk,
     * then updates listings for all subsequent days up to yesterday.
     */
    @Override
    public void run() {
        updateDayListings(listingDir, cacheDir.toPath(), cacheEnabled, minNodeAccountId, maxNodeAccountId, userProject);
    }

    /** The first day of Hedera mainnet. */
    private static final LocalDate HEDERA_START_DATE = LocalDate.of(2019, 9, 13);

    /**
     * Update day listing files by fetching file metadata from Google Cloud Storage.
     *
     * <p>This method scans the local listing directory to find missing days (including gaps),
     * then downloads metadata for all missing days up to (but not including) today.
     * Today is excluded because the day is still in progress and would be incomplete.
     *
     * @param listingDir the directory where listing files are stored
     * @param cacheDir the directory for GCS cache
     * @param cacheEnabled whether to enable GCS caching
     * @param minNodeAccountId minimum node account ID
     * @param maxNodeAccountId maximum node account ID
     * @param userProject GCP project for requester-pays billing
     */
    public static void updateDayListings(
            Path listingDir,
            Path cacheDir,
            boolean cacheEnabled,
            int minNodeAccountId,
            int maxNodeAccountId,
            String userProject) {
        try {

            // Ensure the listing directory exists
            if (!Files.exists(listingDir)) {
                Files.createDirectories(listingDir);
                System.out.println("Created listing directory: " + listingDir);
            }

            // Ensure cache directory exists if caching is enabled
            if (cacheEnabled) {
                Files.createDirectories(cacheDir);
            }

            final LocalDate today = LocalDate.now(ZoneOffset.UTC);
            final LocalDate yesterday = today.minusDays(1);

            // Find all missing days (including gaps) from Hedera start to yesterday
            final List<LocalDate> missingDays = findMissingDaysStatic(listingDir, HEDERA_START_DATE, yesterday);

            // Check if there are any days to process
            if (missingDays.isEmpty()) {
                System.out.println("All listings are up to date (through " + yesterday + ")");
                return;
            }

            System.out.println("Found " + missingDays.size() + " missing day(s) to process");
            System.out.println("First missing: " + missingDays.getFirst() + ", Last missing: " + missingDays.getLast());
            System.out.println("Using nodes " + minNodeAccountId + " to " + maxNodeAccountId);

            // Create MainNetBucket for GCS access
            final MainNetBucket mainNetBucket =
                    new MainNetBucket(cacheEnabled, cacheDir, minNodeAccountId, maxNodeAccountId, userProject);

            // Process each missing day
            final long totalDays = missingDays.size();
            long processedDays = 0;
            final long startTimeNanos = System.nanoTime();

            for (LocalDate missingDay : missingDays) {
                processDayStatic(listingDir, mainNetBucket, missingDay, processedDays, totalDays, startTimeNanos);
                processedDays++;
            }

            // Clear progress and print summary
            PrettyPrint.clearProgress();
            final long elapsedMillis = (System.nanoTime() - startTimeNanos) / 1_000_000;
            System.out.println(
                    "Completed updating " + processedDays + " day(s) in " + formatElapsedTimeStatic(elapsedMillis));

        } catch (Exception e) {
            PrettyPrint.clearProgress();
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Finds all missing days between startDate and endDate (inclusive).
     *
     * @param listingDir the directory where listing files are stored
     * @param startDate the start date to check from
     * @param endDate the end date to check to (inclusive)
     * @return a list of missing dates in chronological order
     */
    private static List<LocalDate> findMissingDaysStatic(
            final Path listingDir, final LocalDate startDate, final LocalDate endDate) {
        final List<LocalDate> missingDays = new java.util.ArrayList<>();
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            if (!dayListingExistsStatic(listingDir, current)) {
                missingDays.add(current);
            }
            current = current.plusDays(1);
        }
        return missingDays;
    }

    /**
     * Checks if a listing file exists for the given day.
     *
     * @param listingDir the directory where listing files are stored
     * @param day the day to check
     * @return true if the listing file exists, false otherwise
     */
    private static boolean dayListingExistsStatic(final Path listingDir, final LocalDate day) {
        final Path dayFile = listingDir
                .resolve(String.format("%04d", day.getYear()))
                .resolve(String.format("%02d", day.getMonthValue()))
                .resolve(String.format("%02d.bin", day.getDayOfMonth()));
        return Files.exists(dayFile);
    }

    /**
     * Static version of findLastDayOnDisk for use by the static API.
     */
    private static LocalDate findLastDayOnDiskStatic(final Path listingPath) throws IOException {
        if (!Files.exists(listingPath)) {
            return null;
        }

        LocalDate latestDate = null;

        // Scan year directories
        try (Stream<Path> yearStream = Files.list(listingPath)) {
            final List<Path> yearDirs =
                    yearStream.filter(Files::isDirectory).sorted().toList();

            for (Path yearDir : yearDirs) {
                final String yearName = yearDir.getFileName().toString();
                if (!yearName.matches("\\d{4}")) {
                    continue;
                }
                final int year = Integer.parseInt(yearName);

                // Scan month directories
                try (Stream<Path> monthStream = Files.list(yearDir)) {
                    final List<Path> monthDirs =
                            monthStream.filter(Files::isDirectory).sorted().toList();

                    for (Path monthDir : monthDirs) {
                        final String monthName = monthDir.getFileName().toString();
                        if (!monthName.matches("\\d{2}")) {
                            continue;
                        }
                        final int month = Integer.parseInt(monthName);

                        // Scan day files
                        try (Stream<Path> dayStream = Files.list(monthDir)) {
                            final List<Path> dayFiles = dayStream
                                    .filter(path -> DAY_FILE_PATTERN
                                            .matcher(path.getFileName().toString())
                                            .matches())
                                    .sorted()
                                    .toList();

                            for (Path dayFile : dayFiles) {
                                final Matcher matcher = DAY_FILE_PATTERN.matcher(
                                        dayFile.getFileName().toString());
                                if (matcher.matches()) {
                                    final int day = Integer.parseInt(matcher.group(1));
                                    try {
                                        final LocalDate date = LocalDate.of(year, month, day);
                                        if (latestDate == null || date.isAfter(latestDate)) {
                                            latestDate = date;
                                        }
                                    } catch (java.time.DateTimeException e) {
                                        // Invalid date (e.g., day 99), skip this file
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return latestDate;
    }

    /**
     * Static version of processDay for use by the static API.
     */
    private static void processDayStatic(
            final Path listingPath,
            final MainNetBucket mainNetBucket,
            final LocalDate day,
            final long processedDays,
            final long totalDays,
            final long startTimeNanos)
            throws Exception {

        final int year = day.getYear();
        final int month = day.getMonthValue();
        final int dayOfMonth = day.getDayOfMonth();

        // Convert LocalDate to block time (nanoseconds since epoch)
        final Instant dayStart = day.atStartOfDay(ZoneOffset.UTC).toInstant();
        final long blockTime = RecordFileDates.instantToBlockTimeLong(dayStart);

        // Update progress before listing
        updateProgressStatic(day, processedDays, totalDays, startTimeNanos, 0, 100);

        // List all files for this day using MainNetBucket
        final List<ChainFile> chainFiles = mainNetBucket.listDay(blockTime);

        // Update progress after listing
        updateProgressStatic(day, processedDays, totalDays, startTimeNanos, 50, 100);

        // Create writer and write all files
        try (DayListingFileWriter writer = new DayListingFileWriter(listingPath, year, month, dayOfMonth)) {
            for (ChainFile chainFile : chainFiles) {
                final ListingRecordFile recordFile = convertToListingRecordFileStatic(chainFile);
                writer.writeRecordFile(recordFile);
            }
        }

        // Update progress after writing
        updateProgressStatic(day, processedDays + 1, totalDays, startTimeNanos, 100, 100);
    }

    /**
     * Static version of convertToListingRecordFile for use by the static API.
     */
    private static ListingRecordFile convertToListingRecordFileStatic(final ChainFile chainFile) {
        // Remove "recordstreams/" prefix from path
        final String path = chainFile.path();
        final String relativePath;
        if (path.startsWith("recordstreams/")) {
            relativePath = path.substring("recordstreams/".length());
        } else {
            relativePath = path;
        }

        // Convert MD5 from Base64 to hex
        final String md5Base64 = chainFile.md5();
        final byte[] md5Bytes = Base64.getDecoder().decode(md5Base64);
        final String md5Hex = HexFormat.of().formatHex(md5Bytes);

        // Extract timestamp from path
        final java.time.LocalDateTime timestamp =
                org.hiero.block.tools.records.RecordFileUtils.extractRecordFileTimeFromPath(path);

        return new ListingRecordFile(relativePath, timestamp, chainFile.size(), md5Hex);
    }

    /**
     * Static version of updateProgress for use by the static API.
     */
    @SuppressWarnings("SameParameterValue")
    private static void updateProgressStatic(
            final LocalDate currentDay,
            final long processedDays,
            final long totalDays,
            final long startTimeNanos,
            final int dayProgress,
            final int dayTotal) {

        // Calculate overall progress
        final double dayFraction = (double) dayProgress / dayTotal;
        final double overallProgress = (processedDays + dayFraction) / totalDays;
        final double percent = overallProgress * 100.0;

        // Calculate ETA
        final long elapsedNanos = System.nanoTime() - startTimeNanos;
        final long elapsedMillis = elapsedNanos / 1_000_000;
        final long remainingMillis =
                PrettyPrint.computeRemainingMilliseconds((long) (overallProgress * 1000), 1000, elapsedMillis);

        // Format progress string
        final String progressString =
                String.format("Day %d/%d: %s", processedDays + 1, totalDays, currentDay.format(DATE_FORMATTER));

        PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
    }

    /**
     * Updates listings for a single specific day (including today).
     * Unlike updateDayListings which excludes today, this method can fetch listings
     * for any day including the current day.
     *
     * @param listingDir the directory where listing files are stored
     * @param cacheDir the directory for GCS cache
     * @param cacheEnabled whether to enable GCS caching
     * @param minNodeAccountId minimum node account ID
     * @param maxNodeAccountId maximum node account ID
     * @param userProject GCP project for requester-pays billing
     * @param targetDay the specific day to fetch listings for
     */
    public static void updateListingsForSingleDay(
            Path listingDir,
            Path cacheDir,
            boolean cacheEnabled,
            int minNodeAccountId,
            int maxNodeAccountId,
            String userProject,
            LocalDate targetDay) {
        try {
            // Ensure the listing directory exists
            if (!Files.exists(listingDir)) {
                Files.createDirectories(listingDir);
            }

            // Ensure cache directory exists if caching is enabled
            if (cacheEnabled) {
                Files.createDirectories(cacheDir);
            }

            // Create MainNetBucket for GCS access
            final MainNetBucket mainNetBucket =
                    new MainNetBucket(cacheEnabled, cacheDir, minNodeAccountId, maxNodeAccountId, userProject);

            // Process the single day
            final long startTimeNanos = System.nanoTime();
            processDayStatic(listingDir, mainNetBucket, targetDay, 0, 1, startTimeNanos);

            PrettyPrint.clearProgress();
            System.out.println("Updated listings for " + targetDay);

        } catch (Exception e) {
            PrettyPrint.clearProgress();
            System.err.println("Error updating listings for " + targetDay + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Static version of formatElapsedTime for use by the static API.
     */
    private static String formatElapsedTimeStatic(final long millis) {
        if (millis <= 0) {
            return "0s";
        }
        long seconds = millis / 1000;
        final long days = seconds / 86400;
        seconds %= 86400;
        final long hours = seconds / 3600;
        seconds %= 3600;
        final long minutes = seconds / 60;
        seconds %= 60;

        final StringBuilder sb = new StringBuilder();
        if (days > 0) {
            sb.append(days).append("d");
        }
        if (hours > 0) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            sb.append(hours).append("h");
        }
        if (minutes > 0) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            sb.append(minutes).append("m");
        }
        if (seconds > 0 || sb.isEmpty()) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            sb.append(seconds).append("s");
        }
        return sb.toString();
    }

    /**
     * Finds the most recent day that has a listing file on disk.
     *
     * <p>Scans the directory structure looking for the pattern:
     * listingDir/YYYY/MM/DD.bin
     *
     * @param listingPath the base directory for listings
     * @return the most recent date found, or null if no listings exist
     * @throws IOException if an I/O error occurs during directory scanning
     */
    private LocalDate findLastDayOnDisk(final Path listingPath) throws IOException {
        if (!Files.exists(listingPath)) {
            return null;
        }

        LocalDate latestDate = null;

        // Scan year directories
        try (Stream<Path> yearStream = Files.list(listingPath)) {
            final List<Path> yearDirs =
                    yearStream.filter(Files::isDirectory).sorted().toList();

            for (Path yearDir : yearDirs) {
                final String yearName = yearDir.getFileName().toString();
                if (!yearName.matches("\\d{4}")) {
                    continue;
                }
                final int year = Integer.parseInt(yearName);

                // Scan month directories
                try (Stream<Path> monthStream = Files.list(yearDir)) {
                    final List<Path> monthDirs =
                            monthStream.filter(Files::isDirectory).sorted().toList();

                    for (Path monthDir : monthDirs) {
                        final String monthName = monthDir.getFileName().toString();
                        if (!monthName.matches("\\d{2}")) {
                            continue;
                        }
                        final int month = Integer.parseInt(monthName);

                        // Scan day files
                        try (Stream<Path> dayStream = Files.list(monthDir)) {
                            final List<Path> dayFiles = dayStream
                                    .filter(path -> DAY_FILE_PATTERN
                                            .matcher(path.getFileName().toString())
                                            .matches())
                                    .sorted()
                                    .toList();

                            for (Path dayFile : dayFiles) {
                                final Matcher matcher = DAY_FILE_PATTERN.matcher(
                                        dayFile.getFileName().toString());
                                if (matcher.matches()) {
                                    final int day = Integer.parseInt(matcher.group(1));
                                    try {
                                        final LocalDate date = LocalDate.of(year, month, day);
                                        if (latestDate == null || date.isAfter(latestDate)) {
                                            latestDate = date;
                                        }
                                    } catch (java.time.DateTimeException e) {
                                        // Invalid date (e.g., day 99), skip this file
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return latestDate;
    }

    /**
     * Processes a single day by listing all files from GCS and writing to a listing file.
     *
     * @param listingPath the base directory for listings
     * @param mainNetBucket the MainNetBucket for GCS access
     * @param day the day to process
     * @param processedDays number of days already processed
     * @param totalDays total number of days to process
     * @param startTimeNanos start time for ETA calculation
     * @throws Exception if an error occurs during processing
     */
    private void processDay(
            final Path listingPath,
            final MainNetBucket mainNetBucket,
            final LocalDate day,
            final long processedDays,
            final long totalDays,
            final long startTimeNanos)
            throws Exception {

        final int year = day.getYear();
        final int month = day.getMonthValue();
        final int dayOfMonth = day.getDayOfMonth();

        // Convert LocalDate to block time (nanoseconds since epoch)
        final Instant dayStart = day.atStartOfDay(ZoneOffset.UTC).toInstant();
        final long blockTime = RecordFileDates.instantToBlockTimeLong(dayStart);

        // Update progress before listing
        updateProgress(day, processedDays, totalDays, startTimeNanos, 0, 100);

        // List all files for this day using MainNetBucket
        final List<ChainFile> chainFiles = mainNetBucket.listDay(blockTime);

        // Update progress after listing
        updateProgress(day, processedDays, totalDays, startTimeNanos, 50, 100);

        // Create writer and write all files
        try (DayListingFileWriter writer = new DayListingFileWriter(listingPath, year, month, dayOfMonth)) {
            for (ChainFile chainFile : chainFiles) {
                final ListingRecordFile recordFile = convertToListingRecordFile(chainFile);
                writer.writeRecordFile(recordFile);
            }
        }

        // Update progress after writing
        updateProgress(day, processedDays + 1, totalDays, startTimeNanos, 100, 100);
    }

    /**
     * Converts a ChainFile to a ListingRecordFile.
     *
     * @param chainFile the ChainFile to convert
     * @return the corresponding ListingRecordFile
     */
    private ListingRecordFile convertToListingRecordFile(final ChainFile chainFile) {
        // Remove "recordstreams/" prefix from path
        final String path = chainFile.path();
        final String relativePath;
        if (path.startsWith("recordstreams/")) {
            relativePath = path.substring("recordstreams/".length());
        } else {
            relativePath = path;
        }

        // Convert MD5 from Base64 to hex
        final String md5Base64 = chainFile.md5();
        final byte[] md5Bytes = Base64.getDecoder().decode(md5Base64);
        final String md5Hex = HexFormat.of().formatHex(md5Bytes);

        // Extract timestamp from path
        final java.time.LocalDateTime timestamp =
                org.hiero.block.tools.records.RecordFileUtils.extractRecordFileTimeFromPath(path);

        return new ListingRecordFile(relativePath, timestamp, chainFile.size(), md5Hex);
    }

    /**
     * Updates the progress display with current status and ETA.
     *
     * @param currentDay the day currently being processed
     * @param processedDays number of complete days processed
     * @param totalDays total number of days to process
     * @param startTimeNanos start time for ETA calculation
     * @param dayProgress progress within the current day (0-100)
     * @param dayTotal total for current-day progress
     */
    @SuppressWarnings("SameParameterValue")
    private void updateProgress(
            final LocalDate currentDay,
            final long processedDays,
            final long totalDays,
            final long startTimeNanos,
            final int dayProgress,
            final int dayTotal) {

        // Calculate overall progress
        final double dayFraction = (double) dayProgress / dayTotal;
        final double overallProgress = (processedDays + dayFraction) / totalDays;
        final double percent = overallProgress * 100.0;

        // Calculate ETA
        final long elapsedNanos = System.nanoTime() - startTimeNanos;
        final long elapsedMillis = elapsedNanos / 1_000_000;
        final long remainingMillis =
                PrettyPrint.computeRemainingMilliseconds((long) (overallProgress * 1000), 1000, elapsedMillis);

        // Format progress string
        final String progressString =
                String.format("Day %d/%d: %s", processedDays + 1, totalDays, currentDay.format(DATE_FORMATTER));

        PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
    }

    /**
     * Finds the most recent day on disk. Exposed for testing.
     *
     * @param listingPath the base directory for listings
     * @return the most recent date found, or null if none exist
     * @throws IOException if an I/O error occurs
     */
    public LocalDate findLastDayOnDiskForTesting(final Path listingPath) throws IOException {
        return findLastDayOnDisk(listingPath);
    }

    /**
     * Formats elapsed time in milliseconds to a human-readable string.
     *
     * @param millis the elapsed time in milliseconds
     * @return formatted string like "2d 3h 15m 30s"
     */
    private String formatElapsedTime(final long millis) {
        if (millis <= 0) {
            return "0s";
        }
        long seconds = millis / 1000;
        final long days = seconds / 86400;
        seconds %= 86400;
        final long hours = seconds / 3600;
        seconds %= 3600;
        final long minutes = seconds / 60;
        seconds %= 60;

        final StringBuilder sb = new StringBuilder();
        if (days > 0) {
            sb.append(days).append("d");
        }
        if (hours > 0) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            sb.append(hours).append("h");
        }
        if (minutes > 0) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            sb.append(minutes).append("m");
        }
        if (seconds > 0 || sb.isEmpty()) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            sb.append(seconds).append("s");
        }
        return sb.toString();
    }
}
