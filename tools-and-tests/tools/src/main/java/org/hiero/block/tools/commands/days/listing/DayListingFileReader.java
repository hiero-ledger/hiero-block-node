package org.hiero.block.tools.commands.days.listing;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Utility class to read the listing file for a specific day and return the list of RecordFile objects.
 */
public class DayListingFileReader {
    public static List<ListingRecordFile> loadRecordsFileForDay(final Path listingDir,
            final int year, final int month, final int day) throws IOException {
        final Path listingPath = ListingRecordFile.getFileForDay(listingDir, year, month, day);
        return loadRecordsFile(listingPath);
    }

    public static List<ListingRecordFile> loadRecordsFile(Path listingPath) throws IOException {
        final List<ListingRecordFile> recordFiles = new ArrayList<>();
        // Implementation to read the listing file for the given day and return a list of RecordFile objects
        try(var din = new DataInputStream(new BufferedInputStream(Files.newInputStream(listingPath), 1024*1024))) {
            final long numberOfFiles = din.readLong();
            for (long i = 0; i < numberOfFiles; i++) {
                recordFiles.add(ListingRecordFile.read(din));
            }
            // double check there are no remaining bytes
            if (din.available() > 0 || din.readAllBytes().length > 0) {
                throw new IOException("Unexpected extra bytes in listing file: " + listingPath);
            }
        }
        return recordFiles;
    }

    public static void main(String[] args) {
        // support args: year month day <OPTIONAL sizeEstimateOnly>
        if (!(args.length == 3)) {
            System.err.println("Usage: DayListingFileReader <year> <month> <day>");
            System.exit(1);
        }
        int year = Integer.parseInt(args[0]);
        int month = Integer.parseInt(args[1]);
        int day = Integer.parseInt(args[2]);
        try {
            List<ListingRecordFile> files = loadRecordsFileForDay(year, month, day);
            System.out.println("Loaded " + files.size() + " files for " + year + "-" + month + "-" + day);
            System.out.println("==========================================================================================");
            System.out.println("First 100 record files");
            System.out.println("==========================================================================================");
            files.stream()
                    .filter(f -> f.type() == ListingRecordFile.Type.RECORD)
                    .sorted(Comparator.comparingLong(ListingRecordFile::timestampEpocMillis))
                    .limit(50)
                    .forEach(System.out::println);
            System.out.println("==========================================================================================");

            System.out.println("files containing T00_00_00");
            System.out.println("==========================================================================================");
            files.stream()
                    .filter(f -> f.path().contains("T00_00_00"))
                    .sorted(Comparator.comparingLong(ListingRecordFile::timestampEpocMillis))
                    .forEach(System.out::println);
            System.out.println("==========================================================================================");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
