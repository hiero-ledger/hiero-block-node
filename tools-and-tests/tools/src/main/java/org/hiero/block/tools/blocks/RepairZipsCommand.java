// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.hiero.block.tools.blocks.repair.MissingBlockFiller;
import org.hiero.block.tools.blocks.repair.ZipRepairEngine;
import org.hiero.block.tools.config.NetworkConfig;
import org.hiero.block.tools.metadata.MetadataFiles;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * CLI subcommand that repairs corrupt zip archives in a wrapped-block directory and, optionally,
 * fills any blocks that are still missing after repair.
 *
 * <h2>Phase 1 — CEN repair (always runs)</h2>
 *
 * <p>Scans all zip archives in the target directory, detects those with a corrupt or missing
 * central directory (CEN), and repairs them by rebuilding the CEN from the intact local file
 * entries. See {@link ZipRepairEngine} for implementation details.</p>
 *
 * <h2>Phase 2 — Fill missing blocks (runs when {@code -i} is provided)</h2>
 *
 * <p>After CEN repair, some zips may still be incomplete: blocks never flushed to disk before a
 * crash cannot be recovered from the zip itself. When the source record-file day archives are
 * available (supply their directory with {@code -i}), this phase re-wraps each missing block from
 * its original source file and appends it to the repaired zip. See {@link MissingBlockFiller} for
 * implementation details.</p>
 */
@Command(
        name = "repair-zips",
        description =
                "Repair corrupt zip CENs in a wrapped-block directory, then optionally fill any blocks still missing"
                        + " from source day archives (-i)",
        mixinStandardHelpOptions = true)
public class RepairZipsCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Directory containing wrapped block zip files to scan and repair")
    private Path directory;

    @Option(
            names = {"--scan-threads"},
            description = "Number of threads for the parallel CEN scan phase (default: 4). "
                    + "Increase for SSD-backed drives, keep low (1-2) for single-spindle HDDs.")
    private int scanThreads = 4;

    @Option(
            names = {"--repair-threads"},
            description = "Number of threads for the parallel CEN repair phase (default: 4). "
                    + "Each thread holds its own read/write buffer in memory.")
    private int repairThreads = 4;

    @Option(
            names = {"--buffer-size"},
            description = "Per-thread buffer size in MiB for the in-memory repair path. "
                    + "Files smaller than this value are repaired entirely in RAM; larger files use "
                    + "zero-copy streaming. Default: auto-computed from available heap and thread count "
                    + "(~75%% of maxHeap / (2 × repairThreads)). "
                    + "Example: --buffer-size 2048 for a 2 GiB per-thread buffer.")
    private int bufferSizeMiB = 0;

    @Option(
            names = {"-i", "--input-dir"},
            description = "Directory containing source record-file .tar.zstd day archives. "
                    + "When provided, a second phase fills any blocks still missing after CEN repair. "
                    + "Omit to run Phase 1 (CEN repair) only.")
    private Path compressedDaysDir = null;

    @Option(
            names = {"-b", "--blocktimes-file"},
            description = "Block-times binary file for mapping block numbers to timestamps (default: block_times.bin)")
    private Path blockTimesFile = MetadataFiles.BLOCK_TIMES_FILE;

    @Option(
            names = {"-d", "--day-blocks"},
            description = "Day-blocks JSON file mapping calendar dates to first/last block numbers"
                    + " (default: day_blocks.json)")
    private Path dayBlocksFile = MetadataFiles.DAY_BLOCKS_FILE;

    @Option(
            names = {"--backup"},
            description = "Directory to copy corrupt zip files into before repairing them. "
                    + "The relative path from the input directory is preserved, so "
                    + "\"wrapped-blocks/000/008/72/20000s.zip\" becomes "
                    + "\"backup-dir/000/008/72/20000s.zip\". "
                    + "When omitted, corrupt originals are simply replaced in-place.")
    private Path backupDir = null;

    @Option(
            names = {"--dry-run"},
            description = "Phase 2 only: scan and report missing blocks without re-wrapping or modifying any files")
    private boolean dryRun = false;

    @Override
    public Integer call() {
        if (!Files.isDirectory(directory)) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ Not a directory: " + directory));
            return 1;
        }

        final int repairExit =
                new ZipRepairEngine(scanThreads, repairThreads, bufferSizeMiB, backupDir).runScanAndRepair(directory);

        if (compressedDaysDir == null) {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|yellow Tip:|@ Re-run with -i <source-days-dir> to also fill"
                    + " any blocks still missing after repair."));
            return repairExit;
        }

        final int fillExit = new MissingBlockFiller(
                        directory,
                        compressedDaysDir,
                        blockTimesFile,
                        dayBlocksFile,
                        NetworkConfig.current().networkName(),
                        dryRun)
                .fill();
        return repairExit == 0 && fillExit == 0 ? 0 : 1;
    }
}
