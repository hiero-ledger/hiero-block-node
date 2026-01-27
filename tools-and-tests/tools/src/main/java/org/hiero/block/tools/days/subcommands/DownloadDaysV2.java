// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.days.download.DownloadConstants.GCP_PROJECT_ID;
import static org.hiero.block.tools.days.download.DownloadDayImplV2.downloadDay;
import static org.hiero.block.tools.mirrornode.DayBlockInfo.loadDayBlockInfoMap;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Map;
import org.hiero.block.tools.metadata.MetadataFiles;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.utils.gcp.ConcurrentDownloadManager;
import org.hiero.block.tools.utils.gcp.ConcurrentDownloadManagerVirtualThreads;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal", "CallToPrintStackTrace"})
@Command(name = "download-days-v2", description = "Download all record files for a specific day. V2")
public class DownloadDaysV2 implements Runnable {

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
    private int threads = 1000;

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
        try (BlockTimeReader blockTimeReader = new BlockTimeReader();
                Storage storage = StorageOptions.grpc()
                        .setAttemptDirectPath(false)
                        .setProjectId(GCP_PROJECT_ID)
                        .build()
                        .getService();
                ConcurrentDownloadManager downloadManager = ConcurrentDownloadManagerVirtualThreads.newBuilder(storage)
                        .setInitialConcurrency(64)
                        .setMaxConcurrency(threads)
                        .build()) {
            // Load day block info map
            final Map<LocalDate, DayBlockInfo> daysInfo = loadDayBlockInfoMap();
            final var days = LocalDate.of(fromYear, fromMonth, fromDay)
                    .datesUntil(LocalDate.of(toYear, toMonth, toDay).plusDays(1))
                    .toList();
            final long totalDays = days.size();
            final long overallStartMillis = System.currentTimeMillis();
            byte[] previousRecordHash = null;
            for (int i = 0; i < days.size(); i++) {
                final LocalDate localDate = days.get(i);
                DayBlockInfo dayBlockInfo = daysInfo.get(localDate);
                try {
                    previousRecordHash = downloadDay(
                            downloadManager,
                            dayBlockInfo,
                            blockTimeReader,
                            listingDir,
                            downloadedDaysDir.toPath(),
                            localDate.getYear(),
                            localDate.getMonthValue(),
                            localDate.getDayOfMonth(),
                            previousRecordHash,
                            totalDays,
                            i, // progressStart as day index (0-based)
                            overallStartMillis);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
