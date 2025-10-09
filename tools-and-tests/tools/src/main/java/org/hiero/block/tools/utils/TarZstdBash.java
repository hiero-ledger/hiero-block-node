package org.hiero.block.tools.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class TarZstdBash {

    /**
     * Compresses inputDir (relative to workingDir) into outputFile (absolute or relative)
     * using: tar -C . -cf - <inputDir> | zstd -10 -T0 --long=27 -o <outputFile>
     * The archive is deterministic (sorted entries, clamped mtimes, no atime/ctime in PAX).
     */
    public static void compressDirectory(Path workingDir, String outputFile, String inputDir) throws IOException {
        compressDirectory(workingDir, outputFile, inputDir, 10, 27, 0, true);
    }

    /**
     * Fully configurable: level (1..19), longWindowPow2 (e.g., 27), threads (0=auto), deterministic on/off.
     */
    public static void compressDirectory(Path workingDir,
            String outputFile,
            String inputDir,
            int level,
            int longWindowPow2,
            int threads,
            boolean deterministic) throws IOException {

        if (workingDir == null || !Files.isDirectory(workingDir)) {
            throw new IllegalArgumentException("workingDir must be an existing directory: " + workingDir);
        }
        // Ensure inputDir exists under workingDir (and is not absolute)
        Path relInput = Paths.get(inputDir);
        if (relInput.isAbsolute()) {
            throw new IllegalArgumentException("inputDir must be RELATIVE to workingDir (not absolute): " + inputDir);
        }
        Path fullInput = workingDir.resolve(relInput).normalize();
        if (!Files.isDirectory(fullInput)) {
            throw new IllegalArgumentException("inputDir does not exist (under workingDir): " + fullInput);
        }

        // Deterministic tar options (GNU tar):
        //  --sort=name            stable file order
        //  --mtime='@0'           clamp mtimes to epoch
        //  --clamp-mtime          applies the clamp instead of erroring
        //  --pax-option=delete=atime,delete=ctime   strip volatile timestamps
        String detTar = deterministic
                ? "--sort=name --mtime='@0' --clamp-mtime --pax-option=delete=atime,delete=ctime"
                : "";

        // Build a safe bash command string; we run inside workingDir so paths in archive are relative.
        String cmd = String.join(" ",
                "set -euo pipefail;",
                "command -v tar >/dev/null 2>&1;",
                "command -v zstd >/dev/null 2>&1;",
                "tar", detTar, "-C", shq("."),
                "-cf", "-", shq(relInput.toString()),
                "|",
                "zstd", "-" + level, "-T" + threads, "--long=" + longWindowPow2,
                "-o", shq(outputFile)
        );

        ProcessBuilder pb = new ProcessBuilder("bash", "-lc", cmd);
        pb.directory(workingDir.toFile());  // makes -C . point at workingDir
        pb.inheritIO(); // show progress/errors from tar/zstd

        try {
            Process p = pb.start();
            int exit = p.waitFor();
            if (exit != 0) {
                throw new IOException("tar|zstd failed with exit code " + exit);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted running tar|zstd", ie);
        }
    }

    /** Shell-quote for bash single-quoted strings. */
    private static String shq(String s) {
        return "'" + s.replace("'", "'\"'\"'") + "'";
    }

    // Example
    public static void main(String[] args) throws Exception {
        Path workingDir = Paths.get("/data");           // parent dir
        String inputDir  = "mydir";                     // relative to workingDir
        String output    = "/backups/mydir.tar.zst";    // any path
        compressDirectory(workingDir, output, inputDir); // -10 -T0 --long=27, deterministic
    }
}
