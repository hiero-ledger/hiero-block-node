// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import org.hiero.block.tools.records.RecordFileInfo;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command line command that prints info for block files or record files
 */
@SuppressWarnings({ "unused", "DuplicatedCode",
    "FieldMayBeFinal", "CallToPrintStackTrace", "FieldCanBeLocal"})
@Command(name = "info", description = "Prints info for record or block files")
public class Info implements Runnable {

    @Parameters(index = "0..*")
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

    /**
     * Empty Default constructor to remove Javadoc warning
     */
    public Info() {}

    /**
     * Main method to run the command
     */
    @Override
    public void run() {
        if (files == null || files.length == 0) {
            System.err.println("No files to display info for");
        } else {
            // check if all files are record files or block files
            if (Arrays.stream(files).anyMatch(f -> f.getName().endsWith("rcd.gz") || f.getName().endsWith("rcd"))) {
                for (File f : files) {
                    try  (InputStream in = f.getName().endsWith(".gz") ?
                                new GZIPInputStream(Files.newInputStream(f.toPath()))  :
                                Files.newInputStream(f.toPath())){
                        final byte[] recordFileContents = in.readAllBytes();
                        final RecordFileInfo info = RecordFileInfo.parse(recordFileContents);
                        System.out.println("==== RECORD FILE " + f.getName() +" ============================\n" +
                            info.prettyToString());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else if (Arrays.stream(files).anyMatch(f -> f.getName().endsWith("blk.gz") || f.getName().endsWith("blk"))) {
                BlockInfo.blockInfo(files, csvMode,outputFile,minSizeMb);
            } else {
                System.err.println("Only rcd, rcd.gz, blk and blk.gz files are supported");
            }
        }
    }

}
