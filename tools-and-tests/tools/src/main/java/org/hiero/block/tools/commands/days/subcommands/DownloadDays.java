// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import static org.hiero.block.tools.commands.days.download.DownloadConstants.RECORD_FILES_PER_DAY;
import static org.hiero.block.tools.commands.days.download.DownloadDay.downloadDay;

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

    @Parameters(index = "0", description = "To year to download")
    private int toYear = LocalDate.now().getYear();

    @Parameters(index = "1", description = "To month to download")
    private int toMonth = LocalDate.now().getMonthValue();

    @Parameters(index = "2", description = "To day to download")
    private int toDay = LocalDate.now().getDayOfMonth();

    @Override
    public void run() {
        final var days = LocalDate.of(fromYear, fromMonth, fromDay)
                .datesUntil(LocalDate.of(toYear, toMonth, toDay).plusDays(1))
                .toList();
        final long totalProgress = days.size() * RECORD_FILES_PER_DAY;
        long progress = 0;
        byte[] previousRecordHash = null;
        for (final LocalDate localDate : days) {
            try {
                previousRecordHash = downloadDay(
                        listingDir.toPath(),
                        downloadedDaysDir.toPath(),
                        localDate.getYear(),
                        localDate.getMonthValue(),
                        localDate.getDayOfMonth(),
                        totalProgress,
                        progress,
                        threads,
                        previousRecordHash);
                progress += RECORD_FILES_PER_DAY;
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }
}
