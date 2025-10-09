// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import static org.hiero.block.tools.commands.days.listing.DayListingFileReader.loadRecordsFileForDay;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import org.hiero.block.tools.commands.days.listing.ListingRecordFile;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@SuppressWarnings({"FieldCanBeLocal", "CallToPrintStackTrace", "FieldMayBeFinal"})
@Command(name = "print-listing", description = "Print listing for one day")
public class PrintListing implements Runnable {
    @Option(
            names = {"-l", "--listing-dir"},
            description = "Directory where listing files are stored")
    private File listingDir = new File("listingsByDay");

    @Parameters(index = "0", description = "Year to download")
    private int year = 2019;

    @Parameters(index = "1", description = "Month to download")
    private int month = 9;

    @Parameters(index = "2", description = "Day to download")
    private int day = 13;

    @Override
    public void run() {
        try {
            System.out.println("Loading listing for " + year + "-" + month + "-" + day + " from "
                    + ListingRecordFile.getFileForDay(listingDir.toPath(), year, month, day));
            List<ListingRecordFile> files = loadRecordsFileForDay(listingDir.toPath(), year, month, day);
            System.out.println("Loaded " + files.size() + " files for " + year + "-" + month + "-" + day);
            System.out.println(
                    "==========================================================================================");
            System.out.println("First 100 record files");
            System.out.println(
                    "==========================================================================================");
            files.stream()
                    .filter(f -> f.type() == ListingRecordFile.Type.RECORD)
                    .sorted(Comparator.comparingLong(ListingRecordFile::timestampEpocMillis))
                    .limit(50)
                    .forEach(System.out::println);
            System.out.println(
                    "==========================================================================================");

            System.out.println("files containing T00_00_00");
            System.out.println(
                    "==========================================================================================");
            files.stream()
                    .filter(f -> f.path().contains("T00_00_00"))
                    .sorted(Comparator.comparingLong(ListingRecordFile::timestampEpocMillis))
                    .forEach(System.out::println);
            System.out.println(
                    "==========================================================================================");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
