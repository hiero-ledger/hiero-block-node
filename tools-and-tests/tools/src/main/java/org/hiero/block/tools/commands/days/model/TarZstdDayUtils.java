// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.model;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Utility class for working with .tar.zstd day files.
 */
public class TarZstdDayUtils {
    /** Regex pattern for matching a single-day file name like "2021-02-04.tar.zstd" */
    public static final Pattern DAY_FILE_PATTERN = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})\\.tar\\.zstd");

    /**
     * Given a list of compressed day directories or .tar.zstd files, produce a clean sorted list of tar.zstd day file
     * paths.
     *
     * @param compressedDayOrDaysDirs files or directories containing .tar.zstd files to process
     * @return a sorted list of paths to the .tar.zstd files to process
     */
    public static List<Path> sortedDayPaths(File[] compressedDayOrDaysDirs) {
        if (compressedDayOrDaysDirs == null || compressedDayOrDaysDirs.length == 0) {
            throw new IllegalArgumentException("No input paths provided");
        }
        // scan all files or directories building uber list of all .tar.zstd files
        final List<Path> allDayFiles = new ArrayList<>();
        for (File f : compressedDayOrDaysDirs) {
            if (f.isDirectory()) {
                try (Stream<Path> fileStream = Files.walk(f.toPath())) {
                    fileStream
                            .filter(Files::isRegularFile)
                            .filter(p -> DAY_FILE_PATTERN
                                    .matcher(p.getFileName().toString())
                                    .matches())
                            .forEach(allDayFiles::add);
                } catch (IOException ioe) {
                    throw new RuntimeException("IO error processing path: " + f + " -> " + ioe.getMessage(), ioe);
                }
            } else if (DAY_FILE_PATTERN.matcher(f.getName()).matches()) {
                allDayFiles.add(f.toPath());
            }
        }
        // sort the input paths for a consistent processing order
        allDayFiles.sort(Comparator.comparing(p -> p.getFileName().toString()));
        // return the sorted list
        return allDayFiles;
    }

    /**
     * Parse a LocalDate from a .tar.zstd day file name.
     *
     * @param fileName the .tar.zstd day file name, like "2021-02-04.tar.zstd"
     * @return the LocalDate represented by the file name
     * @throws IllegalArgumentException if the file name does not match the expected pattern
     */
    public static LocalDate parseDayFromFileName(String fileName) {
        try {
            Matcher matcher = DAY_FILE_PATTERN.matcher(fileName);
            if (!matcher.matches()) {
                throw new IllegalStateException("File name \"" + fileName+"\"does not match day file pattern");
            }
            return LocalDate.of(
                Integer.parseInt(matcher.group(1)),
                Integer.parseInt(matcher.group(2)),
                Integer.parseInt(matcher.group(3)));
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException("Invalid .tar.zstd day file name: \"" + fileName+"\"", e);
        }
    }
}
