// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.model;

import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.tools.days.model.TarZstdDayReader;
import org.hiero.block.tools.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;

/**
 * Standalone benchmark test class for benchmarking TarZstdDayReader. It reads a file and measures performance of
 * throughput in MB/s.
 */
public class TarZstdDayReaderBench {
    public static void main(String[] args) {
        Path dayFile = (args != null && args.length > 0) ? Path.of(args[0]) : Path.of("REAL_DATA/2019-09-16.tar.zstd");

        if (!Files.exists(dayFile)) {
            System.err.println("Day file does not exist: " + dayFile.toAbsolutePath());
            System.exit(2);
        }

        System.out.println("Benchmarking TarZstdDayReader on: " + dayFile.toAbsolutePath());

        try {
            long start = System.nanoTime();
            long totalBytes = TarZstdDayReader.readTarZstd(dayFile).stream()
                    .mapToLong(UnparsedRecordBlock::getTotalSizeBytes)
                    .sum();
            printResult("TarZstdDayReader List", start, System.nanoTime(), totalBytes, 1);

            start = System.nanoTime();
            totalBytes = TarZstdDayReaderUsingExec.readTarZstd(dayFile).stream()
                    .mapToLong(UnparsedRecordBlock::getTotalSizeBytes)
                    .sum();
            printResult("TarZstdDayReaderUsingExec List", start, System.nanoTime(), totalBytes, 1);

            start = System.nanoTime();
            totalBytes = TarZstdDayReaderUsingExec.streamTarZstd(dayFile)
                    .mapToLong(UnparsedRecordBlock::getTotalSizeBytes)
                    .sum();
            printResult("TarZstdDayReaderUsingExec Stream", start, System.nanoTime(), totalBytes, 1);
        } catch (Exception e) {
            System.err.println("Benchmark failed: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    public static void printResult(String testName, long start, long end, long totalBytes, int fileCount) {
        double seconds = (end - start) / 1_000_000_000.0;
        double mb = totalBytes / (1024.0 * 1024.0);
        double mbPerSec = seconds > 0 ? mb / seconds : Double.POSITIVE_INFINITY;
        System.out.printf(
                "Testing: %s, Read %,d blocks with %,d bytes (%.2f MB) in %.3f s -> %.2f MB/s%n",
                testName, fileCount, totalBytes, mb, seconds, mbPerSec);
    }
}
