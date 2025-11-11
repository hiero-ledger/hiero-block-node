package org.hiero.block.tools.commands.days.subcommands;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;

/**
 * CLI skeleton for the "download-live" command.
 * This version only parses and validates arguments, then prints
 * a structured "not implemented yet" message with the parsed config.
 *
 * Business logic (polling mirror, downloading, validation, rollover, compression)
 * will be added in subsequent stories.
 */
@Command(
        name = "download-live",
        description = "Continuously follow mirror node for new block files; dedupe, validate, and organize into daily folders.",
        mixinStandardHelpOptions = true,
        version = "download-live 0.1"
)
public class DownloadLive implements Runnable {

    @Option(names = "--out",
            required = true,
            paramLabel = "DIR",
            description = "Output directory for daily folders (e.g., /data/records)")
    private Path out;

    @Option(names = "--mirror-endpoint",
            required = true,
            paramLabel = "URL",
            description = "Mirror node API base URL (e.g., https://mainnet.mirrornode.io/api)")
    private String mirrorEndpoint;

    @Option(names = "--source-bucket",
            required = true,
            paramLabel = "S3URI",
            description = "Object storage location for record streams (e.g., s3://bucket/recordstreams/)")
    private String sourceBucket;

    @Option(names = "--poll-interval",
            defaultValue = "60s",
            paramLabel = "DURATION",
            description = "Polling interval for mirror API (e.g., 60s, 2m). Parsed later by implementation.")
    private String pollInterval;

    @Option(names = "--batch-size",
            defaultValue = "50",
            paramLabel = "N",
            description = "Max number of block descriptors to request per poll (mirror max is typically 50).")
    private int batchSize;

    @Option(names = "--day-rollover-tz",
            defaultValue = "UTC",
            paramLabel = "TZ",
            description = "Timezone ID used to determine end-of-day rollover (e.g., UTC, America/Los_Angeles).")
    private String dayRolloverTz;

    @Option(names = "--state",
            defaultValue = "./state/live.db",
            paramLabel = "FILE",
            description = "Path to local state DB used for dedupe and resume.")
    private Path statePath;

    @Option(names = "--max-concurrency",
            defaultValue = "8",
            paramLabel = "N",
            description = "Max parallel downloads.")
    private int maxConcurrency;

    @Override
    public void run() {
        // Skeleton behavior: print parsed configuration and exit.
        System.out.println("[download-live] Starting (skeleton mode)");
        System.out.println("Configuration:");
        System.out.println("  out=" + out);
        System.out.println("  mirrorEndpoint=" + mirrorEndpoint);
        System.out.println("  sourceBucket=" + sourceBucket);
        System.out.println("  pollInterval=" + pollInterval);
        System.out.println("  batchSize=" + batchSize);
        System.out.println("  dayRolloverTz=" + dayRolloverTz);
        System.out.println("  state=" + statePath);
        System.out.println("  maxConcurrency=" + maxConcurrency);
        System.out.println("Status: Not implemented yet");
    }

    /**
     * Supported compression formats for daily archives.
     * Chosen names match common archive naming in the project.
     */
    public enum CompressFormat {
        tar_zstd("tar.zstd"),
        tar_gz("tar.gz");

        private final String label;

        CompressFormat(String label) {
            this.label = label;
        }

        @Override
        public String toString() {
            return label;
        }
    }
}
