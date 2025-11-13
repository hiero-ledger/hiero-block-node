// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import static org.hiero.block.tools.commands.days.listing.JsonFileScanner.scanJsonFile;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.tools.commands.days.listing.DayListingFileWriter;
import org.hiero.block.tools.commands.days.listing.ListingRecordFile;
import org.hiero.block.tools.records.RecordFileUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command to read giant JSON listing file and split into smaller binary files, one per day. It parses the JSON and
 * creates RecordFile objects, then writes them to DayListingFileWriter which creates the binary files.
 * <p>To create the JSON files.json run rclone with command line. Warning this takes about 2 weeks to run!
 * <pre>nohup rclone lsjson -R --hash --no-mimetype --no-modtime --gcs-user-project <PROJECT>
 *     "gcp:hedera-mainnet-streams/recordstreams" > files.json &</pre>
 * </p>
 */
@SuppressWarnings({"CallToPrintStackTrace", "RedundantJavaTimeOperations", "UnusedAssignment"})
@Command(name = "split-files-listing", description = "Split the files.json listing into day files")
public class SplitJsonToDayFiles implements Runnable {
    private static final int yearStart = 2019;
    private static DayListingFileWriter[][][] dayWriters;

    @Option(
            names = {"-l", "--listing-dir"},
            description = "Directory where listing files are stored")
    private Path listingDir = Path.of("listingsByDay");

    @Parameters(index = "0", description = "The files.json file to read")
    private final Path jsonFile = Path.of("files.json");

    private static DayListingFileWriter getDayWriter(Path listingDir, int year, int month, int day) throws IOException {
        int yearIdx = year - yearStart;
        DayListingFileWriter dfw = dayWriters[yearIdx][month - 1][day - 1];
        if (dfw == null) {
            dfw = new DayListingFileWriter(listingDir, year, month, day);
            dayWriters[yearIdx][month - 1][day - 1] = dfw;
        }
        return dfw;
    }

    @Override
    public void run() {
        try {
            // create all years
            final int currentYear =
                    Instant.now().atZone(java.time.ZoneOffset.UTC).getYear();
            dayWriters = new DayListingFileWriter[(currentYear - yearStart) + 1][12][31];
            for (int year = yearStart; year <= currentYear; year++) {
                final DayListingFileWriter[][] yearArray = new DayListingFileWriter[12][31];
                for (int m = 0; m < 12; m++) {
                    yearArray[m] = new DayListingFileWriter[31];
                }
                dayWriters[year - yearStart] = yearArray;
            }
            // json file to read
            System.out.println("jsonFile = " + jsonFile);
            AtomicLong fileCallBackCount = new AtomicLong(0);
            AtomicLong fileWrittenCount = new AtomicLong(0);
            // Progress bar setup
            final long totalProcessedLines = scanJsonFile(jsonFile, (path, name, size, md5Hex) -> {
                fileCallBackCount.incrementAndGet();
                // unused, all handled in scanJsonFile below
                // Parse timestamp from file name
                LocalDateTime timestamp = RecordFileUtils.extractRecordFileTime(name);
                // Create RecordFile
                ListingRecordFile recordFile = new ListingRecordFile(path, timestamp, size, md5Hex);
                // write to month file
                try {
                    getDayWriter(
                                    listingDir,
                                    timestamp.get(ChronoField.YEAR),
                                    timestamp.get(ChronoField.MONTH_OF_YEAR),
                                    timestamp.get(ChronoField.DAY_OF_MONTH))
                            .writeRecordFile(recordFile);
                    fileWrittenCount.incrementAndGet();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            // print and close all month writers
            for (int yIdx = 0; yIdx < dayWriters.length; yIdx++) {
                DayListingFileWriter[][] yearWriters = dayWriters[yIdx];
                for (int m = 0; m < yearWriters.length; m++) {
                    DayListingFileWriter[] monthWriters = yearWriters[m];
                    if (monthWriters != null) {
                        for (int d = 0; d < monthWriters.length; d++) {
                            DayListingFileWriter dfw = monthWriters[d];
                            if (dfw != null) {
                                System.out.println(dfw);
                                try {
                                    dfw.close();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else {
                                System.out.println(
                                        "null day writer, year=" + (yIdx + yearStart) + ", month=" + m + ", day=" + d);
                            }
                        }
                    }
                }
            }
            // no read back count longs from header of each file and sum them to verify
            long verifyTotal = 0;
            try (final var stream = Files.walk(listingDir)) {
                verifyTotal = stream.parallel()
                        .filter(Files::isRegularFile)
                        .filter(f -> f.getFileName().toString().endsWith(".bin"))
                        .mapToLong(f -> {
                            try (DataInputStream din = new DataInputStream(Files.newInputStream(f))) {
                                return din.readLong();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .sum();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println(
                    "=========================================================================================");
            System.out.println("Verification total files: " + verifyTotal + " vs processed total JSON lines = "
                    + totalProcessedLines);
            System.out.println("File callback count: " + fileCallBackCount.get() + " File written count: "
                    + fileWrittenCount.get());
            System.out.println(
                    "=========================================================================================");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
