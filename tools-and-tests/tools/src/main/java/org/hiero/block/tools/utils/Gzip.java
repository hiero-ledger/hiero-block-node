package org.hiero.block.tools.utils;

import java.io.IOException;
import java.nio.file.Path;

public class Gzip {
    /**
     * Un-gzip the given file using the system's gunzip command.
     *
     * @param file the path to the .gz file to ungzip
     * @return the path to the ungzipped file
     * @throws IOException if an I/O error occurs
     */
    public static Path ungzip(Path file) throws IOException {
        // ungzip the given file
        if (file.toString().endsWith(".gz")) {
            final Process process = new ProcessBuilder("gunzip", file.getFileName().toString())
                    .directory(file.getParent().toFile())
                    .start();
            try {
                final int exitCode = process.waitFor();
                if (exitCode != 0) {
                    throw new IOException("md5sum command failed with exit code " + exitCode);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            System.out.println(file + " is not a .gz file");
        }
        return Path.of(file.toString().replaceAll("\\.gz$", ""));
    }
}
