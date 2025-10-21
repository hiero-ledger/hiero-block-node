// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Subcommand to list record file info from .rcd or .rcd.gz files. For example:
 * <pre>
 * File Name                                                              Ver   HAPI      PrevHash   BlockHash  Size
 * ------------------------------------------------------------------------------------------------------------------------
 * recordstreams_record0.0.20_2022-02-01T17_30_00.008743294Z (1).rcd      5     0.11.0    f032213b   b9279d9b   58.9 KB
 * recordstreams_record0.0.20_2022-02-01T17_30_00.008743294Z.rcd          5     0.11.0    f032213b   b9279d9b   58.9 KB
 * recordstreams_record0.0.4_2022-02-01T17_30_02.050973000Z.rcd           5     0.11.0    b9279d9b   d4cd9a9b   56.6 KB
 * recordstreams_record0.0.4_2022-02-01T17_30_04.006373067Z.rcd           5     0.11.0    d4cd9a9b   9d7591cc   71.7 KB
 * recordstreams_record0.0.10_2022-02-01T17_30_06.019622000Z.rcd          5     0.11.0    9d7591cc   b1ce579a   55.1 KB
 * recordstreams_record0.0.4_2022-02-01T17_30_06.019622000Z.rcd           5     0.11.0    9d7591cc   b1ce579a   55.1 KB
 * recordstreams_record0.0.4_2022-02-01T17_30_08.025169846Z.rcd           5     0.11.0    b1ce579a   f2d31b2d   74.9 KB
 * recordstreams_record0.0.10_2022-02-01T17_30_08.025169846Z.rcd          5     0.11.0    b1ce579a   f2d31b2d   74.9 KB
 * recordstreams_record0.0.4_2022-02-01T17_30_10.014479000Z.rcd           5     0.11.0    f2d31b2d   22ecbee1   76.8 KB
 * recordstreams_record0.0.10_2022-02-01T17_30_10.014479000Z.rcd          5     0.11.0    f2d31b2d   22ecbee1   76.8 KB
 * recordstreams_record0.0.3_2022-02-01T17_30_10.014479000Z.rcd           5     0.11.0    f2d31b2d   22ecbee1   76.8 KB
 * recordstreams_record0.0.10_2022-02-01T17_30_10.079423000Z.rcd          5     0.11.0    678f41c1   f0948a44   134.9 KB
 * recordstreams_record0.0.10_2022-02-01T17_30_12.013554473Z.rcd          5     0.11.0    f0948a44   f0bd3f8e   46.6 KB
 * recordstreams_record0.0.4_2022-02-01T17_30_12.057031724Z.rcd           5     0.11.0    22ecbee1   9b7ba56d   72.6 KB
 * recordstreams_record0.0.10_2022-02-01T17_30_14.044794612Z.rcd          5     0.11.0    9b7ba56d   0483da03   59.0 KB
 * recordstreams_record0.0.4_2022-02-01T17_30_14.044794612Z.rcd           5     0.11.0    9b7ba56d   0483da03   59.0 KB
 * </pre>
 */
@SuppressWarnings("MismatchedReadAndWriteOfArray")
@Command(name = "ls", description = "List record file info contained in the provided .rcd, .rcd.gz files or directories")
public class LsRecordFiles implements Runnable {
    @Parameters(index = "0..*", description = "Files or directories to process")
    private final File[] recordFilesOrDirectories = new File[0];

    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        // If no inputs are provided, print usage help for this subcommand
        if (recordFilesOrDirectories.length == 0) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }
        System.out.printf("%-70s %-5s %-9s %-10s %-10s %s%n",
            "File Name",
            "Ver",
            "HAPI",
            "PrevHash",
            "BlockHash",
            "Size"
        );
        System.out.println("---------------------------------------------------------------------------------"+
            "---------------------------------------");
        Arrays.stream(recordFilesOrDirectories)
            .flatMap(
                f -> {
                    if (f.isDirectory()) {
                        try (Stream<Path> paths = Files.walk(f.toPath())) {
                            return paths.filter(Files::isRegularFile).filter(p -> {
                                    String name = p.getFileName().toString().toLowerCase();
                                    return name.endsWith(".rcd") || name.endsWith(".rcd.gz");
                                }).toList().stream();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        return Stream.of(f.toPath());
                    }
                }
            )
            .sorted(Comparator.comparing(RecordFileUtils::extractRecordFileTimeStrFromPath))
            .forEach(recordFilePath -> {
            try {
                final byte[] fileData = recordFilePath.getFileName().toString().endsWith(".gz ") ?
                    new GZIPInputStream(Files.newInputStream(recordFilePath)).readAllBytes()
                    : Files.readAllBytes(recordFilePath);
                final RecordFileInfo info = RecordFileInfo.parse(fileData);
                System.out.printf("%-70s %-5d %-9s %-10s %-10s %s%n",
                    recordFilePath.getFileName().toString(),
                    info.recordFormatVersion(),
                    info.hapiProtoVersion().major() +"." +info.hapiProtoVersion().minor() +"." +
                        info.hapiProtoVersion().patch(),
                    info.previousBlockHash().toString().substring(0,8),
                    info.blockHash().toString().substring(0,8),
                    PrettyPrint.prettyPrintFileSize(fileData.length)
                );
            } catch (IOException e) {
                System.err.println("Error reading record file: " + recordFilePath + " Error: " + e.getMessage());
            }
        });
    }
}
