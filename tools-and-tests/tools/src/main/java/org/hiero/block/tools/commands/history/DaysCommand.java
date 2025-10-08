// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.history;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.hiero.block.tools.commands.history.model.InMemoryRecordFileSet;
import org.hiero.block.tools.commands.history.validator.BlockchainValidator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@SuppressWarnings("CallToPrintStackTrace")
@Command(
        name = "days",
        description = "Works with compressed daily record file archives",
        subcommands = {DaysCommand.Ls.class, DaysCommand.Validate.class},
        mixinStandardHelpOptions = true)
public class DaysCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("Please specify a subcommand: ls | validate\nUse --help for more details.");
    }

    @Command(
            name = "ls",
            description = "List record file sets contained in the provided .tar.zstd files or directories")
    public static class Ls implements Runnable {
        @Parameters(index = "0..*", description = "Files or directories to process")
        private final File[] compressedDayOrDaysDirs = new File[0];

        @Override
        public void run() {
            processPaths(compressedDayOrDaysDirs, set -> System.out.println(set.toString()));
        }
    }

    @SuppressWarnings("CallToPrintStackTrace")
    @Command(name = "validate", description = "Validate blocks using BlockchainValidator for each record file set")
    public static class Validate implements Runnable {
        @Parameters(index = "0..*", description = "Files or directories to process")
        private final File[] compressedDayOrDaysDirs = new File[0];

        @Override
        public void run() {
            // Create a new validator instance for this run
            BlockchainValidator validator = new BlockchainValidator();

            processPaths(compressedDayOrDaysDirs, set -> {
                try {
                    boolean ok = validator.validateNextBlock(set);
                    System.out.println(set.recordFileTime() + " -> valid=" + ok);
                } catch (Exception ex) {
                    System.err.println("Validation threw for " + set.recordFileTime() + ": " + ex.getMessage());
                    ex.printStackTrace();
                }
            });
        }
    }

    // Shared helper: walk files/dirs and apply consumer to each InMemoryRecordFileSet
    private static void processPaths(File[] compressedDayOrDaysDirs, Consumer<InMemoryRecordFileSet> consumer) {
        if (compressedDayOrDaysDirs == null || compressedDayOrDaysDirs.length == 0) {
            System.err.println("No input paths provided");
            return;
        }

        for (File compressedDayOrDaysDir : compressedDayOrDaysDirs) {
            if (compressedDayOrDaysDir == null) continue;
            try {
                if (compressedDayOrDaysDir.isDirectory()) {
                    // Walk directory and process each .tar.zstd file
                    try (Stream<Path> fileStream = Files.walk(compressedDayOrDaysDir.toPath())) {
                        fileStream
                                .filter(Files::isRegularFile)
                                .filter(p -> p.toString().endsWith(".tar.zstd"))
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

    private static void processSingleTarZstd(Path path, Consumer<InMemoryRecordFileSet> consumer) {
        try (Stream<InMemoryRecordFileSet> stream = TarZstdDayReader.streamTarZstd(path)) {
            Objects.requireNonNull(stream).forEach(consumer);
        } catch (Exception e) {
            System.err.println("Error reading archive: " + path + " -> " + e.getMessage());
            e.printStackTrace();
        }
    }
}
