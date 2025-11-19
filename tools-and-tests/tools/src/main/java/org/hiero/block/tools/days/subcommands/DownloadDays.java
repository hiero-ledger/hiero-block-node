// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.days.download.DownloadDayImpl.downloadDay;

import java.io.File;
import java.time.LocalDate;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal", "CallToPrintStackTrace"})
@Command(name = "download-days", description = "Download all record files for a specific day")
public class DownloadDays implements Runnable {

    @Option(
            names = {"-l", "--listing-dir"},
            description = "Directory where listing files are stored")
    private File listingDir = new File("listingsByDay");

    @Option(
            names = {"-d", "--downloaded-days-dir"},
            description = "Directory where downloaded days are stored")
    private File downloadedDaysDir = new File("compressedDays");

    @Option(
            names = {"-t", "--threads"},
            description = "How many days to download in parallel")
    private int threads = Runtime.getRuntime().availableProcessors();

    @Parameters(index = "0", description = "From year to download")
    private int fromYear = 2019;

    @Parameters(index = "1", description = "From month to download")
    private int fromMonth = 9;

    @Parameters(index = "2", description = "From day to download")
    private int fromDay = 13;

    @Parameters(index = "3", description = "To year to download")
    private int toYear = LocalDate.now().getYear();

    @Parameters(index = "4", description = "To month to download")
    private int toMonth = LocalDate.now().getMonthValue();

    @Parameters(index = "5", description = "To day to download")
    private int toDay = LocalDate.now().getDayOfMonth();

    @Override
    public void run() {
        final var days = LocalDate.of(fromYear, fromMonth, fromDay)
                .datesUntil(LocalDate.of(toYear, toMonth, toDay).plusDays(1))
                .toList();
        final long totalDays = days.size();
        final long overallStartMillis = System.currentTimeMillis();
        byte[] previousRecordHash = null;
        for (int i = 0; i < days.size(); i++) {
            final LocalDate localDate = days.get(i);
            try {
                previousRecordHash = downloadDay(
                        listingDir.toPath(),
                        downloadedDaysDir.toPath(),
                        localDate.getYear(),
                        localDate.getMonthValue(),
                        localDate.getDayOfMonth(),
                        totalDays,
                        i, // progressStart as day index (0-based)
                        threads,
                        previousRecordHash,
                        overallStartMillis);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }
}
