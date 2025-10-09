// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.model;

import com.github.luben.zstd.ZstdOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

/**
 * Utility class to create .tar.zst files from a day's directory. The day directory contains files like:
 * <pre>
 * 2019-09-13T23_38_10.809335Z/
 * 2019-09-13T23_38_10.809335Z/node_0.0.9.rcs_sig
 * 2019-09-13T23_38_10.809335Z/2019-09-13T23_38_10.809335Z.rcd
 * </pre>
 */
public class TarZstdDayWriter {
    private static final Pattern RECORD_FILE_DIR_PATTERN =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}_\\d{2}_\\d{2}\\.\\d+Z");

    /**
     * Compress all record file directories in the day directory into a single tar zstd file.
     * This implementation streams the TAR into zstd in-process using zstd_jni. A simple
     * progress bar (based on number of files written) is printed to stdout. Output is first
     * written to a partial file (suffix "_partial") and atomically renamed to the final name
     * when complete, so partial files are not mistaken for finished archives.
     *
     * @param dayDirectory the directory containing record file directories for the day
     * @param outputDirectory the directory to write the tar zstd file to
     * @param compressionLevel the zstd compression level to use, 1 is fastest, 22 is slowest
     */
    public static void compressDay(Path dayDirectory, Path outputDirectory, int compressionLevel) {
        Path tempPartial = null;
        try {
            if (!Files.exists(outputDirectory)) Files.createDirectories(outputDirectory);
            final Path outputFile =
                    outputDirectory.resolve(dayDirectory.getFileName().toString() + ".tar.zstd");
            final Path partialFile =
                    outputDirectory.resolve(dayDirectory.getFileName().toString() + ".tar.zstd_partial");

            // If the final output already exists, bail out early (higher-level command may also check)
            if (Files.exists(outputFile)) {
                System.out.println("Output exists, skipping: " + outputFile);
                return;
            }

            // remove any stale partial file before starting
            try {
                Files.deleteIfExists(partialFile);
            } catch (IOException ignored) {
            }
            tempPartial = partialFile;

            final List<Path> sortedRecordDirs;
            try (var stream = Files.list(dayDirectory)) {
                sortedRecordDirs = stream.filter(path -> Files.isDirectory(path)
                                && RECORD_FILE_DIR_PATTERN
                                        .matcher(path.getFileName().toString())
                                        .matches())
                        .sorted()
                        .toList();
            }

            // Compute total number of regular files to write (cheap count, no sizes)
            int totalFiles = 0;
            for (Path recordFileDir : sortedRecordDirs) {
                try (var s = Files.list(recordFileDir)) {
                    totalFiles += (int) s.filter(Files::isRegularFile).count();
                }
            }

            System.out.println("Creating " + partialFile + " (level=" + compressionLevel + ")");

            // Open partial output file and wrap with ZstdOutputStream, then TarArchiveOutputStream
            try (OutputStream fout = Files.newOutputStream(partialFile);
                    ZstdOutputStream zOut = new ZstdOutputStream(fout, compressionLevel);
                    TarArchiveOutputStream tar = new TarArchiveOutputStream(zOut)) {

                tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

                int written = 0;
                for (Path recordFileDir : sortedRecordDirs) {
                    final List<Path> files;
                    try (var s2 = Files.list(recordFileDir)) {
                        files = s2.filter(Files::isRegularFile).sorted().toList();
                    }

                    for (Path file : files) {
                        String entryName =
                                dayDirectory.relativize(file).toString().replace(java.io.File.separatorChar, '/');
                        long size = Files.size(file);

                        TarArchiveEntry entry = new TarArchiveEntry(entryName);
                        entry.setSize(size);
                        tar.putArchiveEntry(entry);

                        try (var in = Files.newInputStream(file)) {
                            in.transferTo(tar);
                        }

                        tar.closeArchiveEntry();

                        // progress update
                        written++;
                        if (totalFiles > 0) {
                            printProgress(written, totalFiles);
                        }
                    }
                }

                tar.finish();
                // ensure final progress 100%
                if (totalFiles > 0) {
                    printProgress(totalFiles, totalFiles);
                    System.out.println();
                }
            }

            // Move partial to final, try atomic move, fallback to non-atomic replace if needed
            try {
                Files.move(partialFile, outputFile, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                // fallback: try replace existing without ATOMIC
                Files.move(partialFile, outputFile, StandardCopyOption.REPLACE_EXISTING);
            }
            tempPartial = null; // moved successfully

        } catch (IOException e) {
            // cleanup partial file if present
            if (tempPartial != null) {
                try {
                    Files.deleteIfExists(tempPartial);
                } catch (IOException ignored) {
                }
            }
            throw new RuntimeException(e);
        }
    }

    // Simple console progress bar based on file counts
    private static void printProgress(int written, int total) {
        int width = 40; // progress bar width in chars
        double frac = (total == 0) ? 1.0 : ((double) written) / total;
        int filled = (int) Math.round(frac * width);
        StringBuilder sb = new StringBuilder();
        sb.append('\r');
        sb.append('[');
        for (int i = 0; i < filled; i++) sb.append('=');
        for (int i = filled; i < width; i++) sb.append(' ');
        sb.append(']');
        sb.append(String.format(" %3d%% (%d/%d)", (int) Math.round(frac * 100), written, total));
        System.out.print(sb.toString());
        System.out.flush();
    }
}
