// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.tools.commands.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.commands.days.model.TarZstdDayUtils;
import org.hiero.block.tools.records.RecordFileBlock;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(name = "ls", description = "List record file sets contained in the provided .tar.zstd files or directories")
public class Ls implements Runnable {
    @Parameters(index = "0..*", description = "Files or directories to process")
    private final File[] compressedDayOrDaysDirs = new File[0];


    @Option(
        names = {"-l", "--extended"},
        description = "Display extra information from parsing each record file block")
    private boolean extended = false;

    @Option(
        names = {"-p", "--time-prefix"},
        description = "String time prefix to filter displayed record file blocks")
    private String timePrefix = null;

    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        System.out.println("extended = " + extended);
        // If no inputs are provided, print usage help for this subcommand
        if (compressedDayOrDaysDirs.length == 0) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }
        final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(compressedDayOrDaysDirs);
        for (Path dayFile : dayPaths) {
            try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(dayFile)) {
                stream
                    .filter((RecordFileBlock set) -> timePrefix == null || set.recordFileTime().toString().startsWith(timePrefix))
                    .forEach((RecordFileBlock set) -> System.out.println(extended ? set.toStringExtended() : set.toString()));
            }
        }
    }
}
