// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import java.io.File;
import org.hiero.block.tools.blocks.model.BlockInfo;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command line command that prints info for block files.
 *
 * <p>Supports:
 * <ul>
 *   <li>Standalone block files: {@code <BLOCK_NUMBER>.blk} (uncompressed)</li>
 *   <li>Compressed standalone: {@code .blk.gz} or {@code .blk.zstd}</li>
 *   <li>Zip archives containing blocks (each internally as {@code .blk} or {@code .blk.zstd})</li>
 * </ul>
 *
 * <p>Input can be files or directories. Directories are recursively scanned for blocks.
 * Results are sorted by block number.
 */
@SuppressWarnings({"unused", "FieldMayBeFinal", "FieldCanBeLocal"})
@Command(name = "ls", description = "Prints info for block files (supports .blk, .blk.gz, .blk.zstd, and zip archives)")
public class LsBlockFiles implements Runnable {

    @Parameters(index = "0..*", description = "Block files, directories, or zip archives to process")
    private File[] files;

    @Option(
            names = {"-ms", "--min-size"},
            description = "Filter to only files bigger than this minimum file size in megabytes")
    private double minSizeMb = Double.MAX_VALUE;

    @Option(
            names = {"-c", "--csv"},
            description = "Enable CSV output mode (default: ${DEFAULT-VALUE})")
    private boolean csvMode = false;

    @Option(
            names = {"-o", "--output-file"},
            description = "Output to file rather than stdout")
    private File outputFile;

    /** Empty Default constructor to remove the Javadoc warning. */
    public LsBlockFiles() {}

    /** Main method to run the command. */
    @Override
    public void run() {
        if (files == null || files.length == 0) {
            System.err.println("No files to display info for");
        } else {
            // BlockInfo.blockInfo handles all format detection and sorting by block number
            BlockInfo.blockInfo(files, csvMode, outputFile, minSizeMb);
        }
    }
}
