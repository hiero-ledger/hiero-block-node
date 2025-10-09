package org.hiero.block.tools.commands.days.subcommands;

import java.io.File;
import org.hiero.block.tools.commands.days.model.TarZstdUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(
    name = "ls",
    description = "List record file sets contained in the provided .tar.zstd files or directories")
public class Ls implements Runnable {
    @Parameters(index = "0..*", description = "Files or directories to process")
    private final File[] compressedDayOrDaysDirs = new File[0];

    @Override
    public void run() {
        TarZstdUtils.processPaths(compressedDayOrDaysDirs, set -> System.out.println(set.toString()));
    }
}
