// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;

/**
 * Utility methods for reliably saving and loading {@link StreamingHasher} state files.
 *
 * <h2>Atomic save pattern</h2>
 *
 * <p>{@link #saveAtomically} writes to a {@code .tmp} file first, then rotates the existing
 * primary to {@code .bak}, and finally renames the {@code .tmp} to the primary path.
 * This means at most one complete copy can be lost: if the JVM is killed between the two renames,
 * the previous good state survives in {@code .bak}.
 *
 * <h2>Fallback load pattern</h2>
 *
 * <p>{@link #loadWithFallback} tries the primary file first. If the file is missing or its
 * {@link StateLoader#load} throws (e.g. an {@link java.io.EOFException} from a truncated write),
 * the {@code .bak} copy is tried. If both fail the hasher is left in its initial empty state
 * and a warning is printed.
 */
public final class HasherStateFiles {

    private HasherStateFiles() {}

    /**
     * Loads hasher state from {@code primaryPath}, falling back to {@code primaryPath.bak}
     * if the primary file is missing or its {@code load} call throws. Logs warnings and starts
     * fresh if both files fail.
     *
     * @param primaryPath the primary state file path
     * @param loader function that restores hasher state from a given path
     */
    public static void loadWithFallback(Path primaryPath, StateLoader loader) {
        if (!Files.exists(primaryPath)) {
            final Path bakPath = Path.of(primaryPath + ".bak");
            if (Files.exists(bakPath)) {
                System.err.println("Warning: primary state file missing, trying backup: " + bakPath);
                try {
                    loader.load(bakPath);
                    System.err.println("Warning: loaded state from backup: " + bakPath);
                } catch (Exception bakEx) {
                    System.err.println("Warning: backup also failed (" + bakEx.getMessage() + "), starting fresh.");
                    try {
                        Files.deleteIfExists(bakPath);
                    } catch (IOException ignored) {
                    }
                }
            }
            return;
        }
        try {
            loader.load(primaryPath);
        } catch (Exception e) {
            System.err.println(
                    "Warning: corrupt primary state file " + primaryPath + " (" + e.getMessage() + "). Trying backup.");
            try {
                Files.deleteIfExists(primaryPath);
            } catch (IOException ignored) {
            }
            final Path bakPath = Path.of(primaryPath + ".bak");
            if (Files.exists(bakPath)) {
                try {
                    loader.load(bakPath);
                    System.err.println("Warning: loaded state from backup: " + bakPath);
                } catch (Exception bakEx) {
                    System.err.println("Warning: backup also failed (" + bakEx.getMessage() + "), starting fresh.");
                    try {
                        Files.deleteIfExists(bakPath);
                    } catch (IOException ignored) {
                    }
                }
            } else {
                System.err.println("Warning: no backup found for " + primaryPath + ", starting fresh.");
            }
        }
    }

    /**
     * Saves state atomically using a write-to-temp, rotate-backup, rename pattern:
     * <ol>
     *   <li>Write to {@code primaryPath.tmp}</li>
     *   <li>Rename existing {@code primaryPath} to {@code primaryPath.bak} (if present)</li>
     *   <li>Rename {@code primaryPath.tmp} to {@code primaryPath}</li>
     * </ol>
     *
     * <p>If the JVM is killed between steps 2 and 3, the previous complete state is preserved in
     * {@code primaryPath.bak} and will be loaded by {@link #loadWithFallback} on the next run.
     *
     * @param primaryPath the target path for the saved state
     * @param saver function that writes hasher state to a given path
     * @throws Exception if saving fails
     */
    public static void saveAtomically(Path primaryPath, StateSaver saver) throws Exception {
        final Path tmpPath = Path.of(primaryPath + ".tmp");
        saver.save(tmpPath);
        if (Files.exists(primaryPath)) {
            final Path bakPath = Path.of(primaryPath + ".bak");
            Files.move(primaryPath, bakPath, StandardCopyOption.REPLACE_EXISTING);
        }
        Files.move(tmpPath, primaryPath, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Saves the streaming hasher atomically via {@link #saveAtomically}.
     *
     * @param streamingFile path for the streaming hasher state
     * @param streamingHasher the streaming hasher to save
     */
    public static void saveStateCheckpoint(Path streamingFile, StreamingHasher streamingHasher) {
        try {
            saveAtomically(streamingFile, streamingHasher::save);
        } catch (Exception e) {
            System.err.println("Warning: could not save " + streamingFile + ": " + e.getMessage());
        }
    }

    /** Functional interface for a state-loader that may throw a checked exception. */
    @FunctionalInterface
    public interface StateLoader {
        /** Restore state from the given path. */
        void load(Path path) throws Exception;
    }

    /** Functional interface for a state-saver that may throw a checked exception. */
    @FunctionalInterface
    public interface StateSaver {
        /** Persist state to the given path. */
        void save(Path path) throws Exception;
    }
}
