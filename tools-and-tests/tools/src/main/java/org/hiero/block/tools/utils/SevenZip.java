package org.hiero.block.tools.utils;

import java.io.IOException;
import java.nio.file.Path;

public class SevenZip {
    public static void compressDirectory(Path workingDir, String outputFile, String inputDir) throws IOException {
        // use 7zip to compress the inputDir into outputFile
        // using command line 7z a -t7z -m0=LZMA2 -mx=9 -mmt=on -md=256m -ms=on
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(
                "7z",
                "a",
                "-t7z",
                "-m0=LZMA2",
                "-mx=9",
                "-mmt=on",
                "-md=256m",
                "-ms=on",
                outputFile,
                inputDir);
        processBuilder.directory(workingDir.toFile());
        processBuilder.inheritIO();
        try {
            Process process = processBuilder.start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("7z command failed with exit code " + exitCode);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to execute 7z command", e);
        }
    }
}
