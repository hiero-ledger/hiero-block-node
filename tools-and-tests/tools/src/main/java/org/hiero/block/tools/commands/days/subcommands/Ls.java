// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.tools.commands.days.model.TarZstdDayReader;
import org.hiero.block.tools.commands.days.model.TarZstdDayUtils;
import org.hiero.block.tools.records.InMemoryBlock;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(name = "ls", description = "List record file sets contained in the provided .tar.zstd files or directories")
public class Ls implements Runnable {
    @Parameters(index = "0..*", description = "Files or directories to process")
    private final File[] compressedDayOrDaysDirs = new File[0];

    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        // If no inputs are provided, print usage help for this subcommand
        if (compressedDayOrDaysDirs.length == 0) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }
        final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(compressedDayOrDaysDirs);
        for (Path dayFile : dayPaths) {
            try (var stream = TarZstdDayReader.streamTarZstd(dayFile)) {
                stream.forEach((InMemoryBlock set) -> System.out.println(set.toString()));
            }
        }
    }
}
