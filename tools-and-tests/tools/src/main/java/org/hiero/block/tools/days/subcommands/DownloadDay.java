// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.days.download.DownloadDayImpl.downloadDay;

import java.io.File;
import java.nio.file.Path;
import org.hiero.block.tools.metadata.MetadataFiles;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal", "CallToPrintStackTrace"})
@Command(name = "download-day", description = "Download all record files for a specific day")
public class DownloadDay implements Runnable {

    @Option(
            names = {"-l", "--listing-dir"},
            description = "Directory where listing files are stored")
    private Path listingDir = MetadataFiles.LISTINGS_DIR;

    @Option(
            names = {"-d", "--downloaded-days-dir"},
            description = "Directory where downloaded days are stored")
    private File downloadedDaysDir = new File("compressedDays");

    @Option(
            names = {"-t", "--threads"},
            description = "How many days to download in parallel")
    private int threads = Runtime.getRuntime().availableProcessors();

    @Parameters(index = "0", description = "Year to download")
    private int year = 2019;

    @Parameters(index = "1", description = "Month to download")
    private int month = 9;

    @Parameters(index = "2", description = "Day to download")
    private int day = 13;

    @Override
    public void run() {
        try {
            long now = System.currentTimeMillis();
            downloadDay(
                    listingDir,
                    downloadedDaysDir.toPath(),
                    year,
                    month,
                    day,
                    1L, // totalDays
                    0, // dayIndex
                    threads,
                    null,
                    now);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
