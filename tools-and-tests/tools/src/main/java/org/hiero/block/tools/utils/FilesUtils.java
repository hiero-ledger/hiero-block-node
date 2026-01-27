// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

public class FilesUtils {
    /**
     * Calculate the total size of all files in a directory and its subdirectories.
     *
     * @param path the path to the directory
     * @return the total size in bytes
     */
    public static long directorySize(Path path) {
        try (Stream<Path> walk = Files.walk(path)) {
            return walk.filter(Files::isRegularFile)
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (java.io.IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .sum();
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Recursively delete a directory and all its contents.
     *
     * @param path the path to the directory
     */
    public static void deleteDirectory(Path path) {
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()) // delete children before parents
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (java.io.IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }
}
