package org.hiero.block.tools.commands.days.subcommands;

import static org.hiero.block.tools.commands.days.download.DownloadConstants.RECORD_FILES_PER_DAY;
import static org.hiero.block.tools.commands.days.download.DownloadDay.downloadDay;

import java.io.File;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal"})
@Command(
    name = "download-day",
    description = "Download all record files for a specific day")
public class DownloadDay implements Runnable {

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
    private int threads = Runtime.getRuntime().availableProcessors()/2;

    @Parameters(index = "0", description = "Year to download")
    private int year = 2019;
    @Parameters(index = "1", description = "Month to download")
    private int month = 9;
    @Parameters(index = "2", description = "Day to download")
    private int day = 13;

    @Override
    public void run() {
        try {
            downloadDay(listingDir.toPath(), downloadedDaysDir.toPath(), year, month, day,
                RECORD_FILES_PER_DAY, 0, threads);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
