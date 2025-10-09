package org.hiero.block.tools.commands.days.model;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class TarZstdUtils {
    // Shared helper: walk files/dirs and apply consumer to each InMemoryRecordFileSet
    public static void processPaths(File[] compressedDayOrDaysDirs, Consumer<InMemoryRecordFileSet> consumer) {
        if (compressedDayOrDaysDirs == null || compressedDayOrDaysDirs.length == 0) {
            System.err.println("No input paths provided");
            return;
        }
        // sort the input paths for consistent processing order
        Arrays.sort(compressedDayOrDaysDirs, Comparator.comparing(File::getName));

        for (File compressedDayOrDaysDir : compressedDayOrDaysDirs) {
            if (compressedDayOrDaysDir == null) continue;
            try {
                if (compressedDayOrDaysDir.isDirectory()) {
                    // Walk directory and process each .tar.zstd file
                    try (Stream<Path> fileStream = Files.walk(compressedDayOrDaysDir.toPath())) {
                        fileStream
                                .filter(Files::isRegularFile)
                                .filter(p -> p.toString().endsWith(".tar.zstd"))
                                .sorted(Comparator.comparing(Path::toString))
                                .forEach(p -> processSingleTarZstd(p, consumer));
                    }
                } else if (compressedDayOrDaysDir.getName().endsWith(".tar.zstd")) {
                    System.out.println("Processing single file: " + compressedDayOrDaysDir);
                    processSingleTarZstd(compressedDayOrDaysDir.toPath(), consumer);
                } else {
                    System.err.println("Skipping non .tar.zstd file: " + compressedDayOrDaysDir);
                }
            } catch (IOException ioe) {
                System.err.println("IO error processing path: " + compressedDayOrDaysDir + " -> " + ioe.getMessage());
                ioe.printStackTrace();
            }
        }
    }

    public static void processSingleTarZstd(Path path, Consumer<InMemoryRecordFileSet> consumer) {
        try (Stream<InMemoryRecordFileSet> stream = TarZstdDayReader.streamTarZstd(path)) {
            System.out.println("==================> Processing archive: " + path);
            Objects.requireNonNull(stream).forEach(consumer);
        } catch (Exception e) {
            System.err.println("Error reading archive: " + path + " -> " + e.getMessage());
            e.printStackTrace();
        }
    }
}
