package org.hiero.block.tools.commands.days.model;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.tools.records.InMemoryBlock;
import org.hiero.block.tools.records.InMemoryFile;

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
            List<InMemoryBlock> blocks = TarZstdDayReader.readTarZstd(dayFile);
            long end = System.nanoTime();

            long totalBytes = 0L;
            long fileCount = 0L;
            for (InMemoryBlock b : blocks) {
                InMemoryFile primary = b.primaryRecordFile();
                if (primary != null) {
                    totalBytes += primary.data().length;
                    fileCount++;
                }
                for (InMemoryFile f : b.otherRecordFiles()) {
                    totalBytes += f.data().length;
                    fileCount++;
                }
                for (InMemoryFile f : b.signatureFiles()) {
                    totalBytes += f.data().length;
                    fileCount++;
                }
                for (InMemoryFile f : b.primarySidecarFiles()) {
                    totalBytes += f.data().length;
                    fileCount++;
                }
                for (InMemoryFile f : b.otherSidecarFiles()) {
                    totalBytes += f.data().length;
                    fileCount++;
                }
            }

            double seconds = (end - start) / 1_000_000_000.0;
            double mb = totalBytes / (1024.0 * 1024.0);
            double mbPerSec = seconds > 0 ? mb / seconds : Double.POSITIVE_INFINITY;

            System.out.printf("Read %,d blocks with %,d files: %,d bytes (%.2f MB) in %.3f s -> %.2f MB/s%n",
                    blocks.size(), fileCount, totalBytes, mb, seconds, mbPerSec);

        } catch (Exception e) {
            System.err.println("Benchmark failed: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
