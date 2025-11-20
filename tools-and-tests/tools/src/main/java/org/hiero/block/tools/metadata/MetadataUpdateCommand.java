// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.metadata;

import java.io.File;
import java.nio.file.Path;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.subcommands.UpdateDayListingsCommand;
import org.hiero.block.tools.mirrornode.UpdateBlockData;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Command to update all metadata files.
 *
 * <p>This command updates:
 * <ul>
 *   <li>block_times.bin and day_blocks.json from mirror node</li>
 *   <li>listingsByDay directory from GCS bucket</li>
 * </ul>
 */
@SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal"})
@Command(
        name = "update",
        description = "Update all metadata files (block_times.bin, day_blocks.json, listingsByDay)",
        mixinStandardHelpOptions = true)
public class MetadataUpdateCommand implements Runnable {

    // Mirror node data options
    @Option(
            names = {"--block-times"},
            description = "Path to the block times \".bin\" file (default: ${DEFAULT-VALUE})")
    private Path blockTimesFile = MetadataFiles.BLOCK_TIMES_FILE;

    @Option(
            names = {"--day-blocks"},
            description = "Path to the day blocks \".json\" file (default: ${DEFAULT-VALUE})")
    private Path dayBlocksFile = MetadataFiles.DAY_BLOCKS_FILE;

    // Day listings options
    @Option(
            names = {"-l", "--listing-dir"},
            description = "Directory where listing files are stored (default: ${DEFAULT-VALUE})")
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

    @Option(
            names = {"--skip-mirror-node"},
            description = "Skip updating from mirror node (block_times.bin, day_blocks.json)")
    private boolean skipMirrorNode = false;

    @Option(
            names = {"--skip-listings"},
            description = "Skip updating day listings from GCS")
    private boolean skipListings = false;

    /** Default constructor. */
    public MetadataUpdateCommand() {}

    @Override
    public void run() {
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   METADATA UPDATE|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println();

        // Step 1: Update mirror node data (block_times.bin and day_blocks.json)
        if (!skipMirrorNode) {
            System.out.println(Ansi.AUTO.string("@|bold,blue ▶ Step 1: Updating mirror node data|@"));
            System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────────|@"));
            UpdateBlockData.updateMirrorNodeData(blockTimesFile, dayBlocksFile);
            System.out.println();
        } else {
            System.out.println(Ansi.AUTO.string("@|yellow ⚠ Skipping mirror node data update|@"));
            System.out.println();
        }

        // Step 2: Update day listings from GCS
        if (!skipListings) {
            System.out.println(Ansi.AUTO.string("@|bold,blue ▶ Step 2: Updating day listings from GCS|@"));
            System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────────|@"));
            UpdateDayListingsCommand.updateDayListings(
                    listingDir, cacheDir.toPath(), cacheEnabled, minNodeAccountId, maxNodeAccountId, userProject);
            System.out.println();
        } else {
            System.out.println(Ansi.AUTO.string("@|yellow ⚠ Skipping day listings update|@"));
            System.out.println();
        }

        System.out.println(
                Ansi.AUTO.string("@|bold,green ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,green   METADATA UPDATE COMPLETE|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,green ════════════════════════════════════════════════════════════|@"));
    }
}
