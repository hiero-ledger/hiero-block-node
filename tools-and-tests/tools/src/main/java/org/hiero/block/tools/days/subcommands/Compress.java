// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import org.hiero.block.tools.days.model.TarZstdDayWriter;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Compress directories like "2019-09-13" into "2019-09-13.tar.zstd" making sure all files inside have relative paths
 * and are stored in date ascending sorted order.
 */
@Command(name = "compress", description = "Compress one or more day directories into .tar.zst files")
public class Compress implements Runnable {
    private static final Pattern DAY_DIR_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

    @Option(
            names = {"-o", "--output-dir"},
            description = "Directory where to write compressed files into")
    private File outputDir;

    @Option(
            names = {"-c", "--compression-level"},
            description = "the zstd compression level to use, 1 is fastest, 22 is slowest",
            defaultValue = "6")
    private int compressionLevel;

    @Parameters(index = "0..*", description = "day directory or directories of day directories to process")
    private final List<File> compressedDayOrDaysDirs = List.of();

    @Override
    public void run() {
        // If no positional args were provided print help and exit
        if (compressedDayOrDaysDirs.isEmpty()) {
            CommandLine.usage(this, System.out);
            return;
        }

        // check outputDir is set and is a directory
        if (outputDir == null) {
            throw new IllegalArgumentException("Output directory must be specified");
        }
        if (outputDir.exists() && !outputDir.isDirectory()) {
            throw new IllegalArgumentException("Output directory is not a directory: " + outputDir);
        }
        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                throw new IllegalArgumentException(
                        "Output directory does not exist and could not be created: " + outputDir);
            }
        }
        // do compression
        for (File dir : compressedDayOrDaysDirs) {
            if (dir.isDirectory()) {
                if (DAY_DIR_PATTERN.matcher(dir.getName()).matches()) {
                    Path outFile = outputDir.toPath().resolve(dir.getName() + ".tar.zstd");
                    if (Files.exists(outFile)) {
                        System.out.println("Skipping existing: " + outFile);
                    } else {
                        TarZstdDayWriter.compressDay(dir.toPath(), outputDir.toPath(), compressionLevel);
                    }
                } else {
                    // assume a directory of date dirs
                    try (var stream = Files.list(dir.toPath())) {
                        stream.filter(Files::isDirectory)
                                .filter(path -> DAY_DIR_PATTERN
                                        .matcher(path.getFileName().toString())
                                        .matches())
                                .forEach(path -> {
                                    Path outFile = outputDir
                                            .toPath()
                                            .resolve(path.getFileName().toString() + ".tar.zstd");
                                    if (Files.exists(outFile)) {
                                        System.out.println("Skipping existing: " + outFile);
                                    } else {
                                        TarZstdDayWriter.compressDay(path, outputDir.toPath(), compressionLevel);
                                    }
                                });
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}
