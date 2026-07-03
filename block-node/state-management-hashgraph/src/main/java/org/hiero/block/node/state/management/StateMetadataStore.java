// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import org.hiero.block.api.StateMetadata;

/// Persists `StateMetadata` to disk as JSON via PBJ's generated codec.
///
/// Writes are atomic: the new payload is written to a `.tmp` sibling and then
/// moved over the target with `StandardCopyOption.ATOMIC_MOVE`. A crash between
/// those two steps leaves the previous snapshot of the file intact.
final class StateMetadataStore {

    private final Path path;

    /// Create a store backed by the given metadata file path.
    ///
    /// @param path the file the store reads from and writes to
    StateMetadataStore(@NonNull final Path path) {
        this.path = path;
    }

    /// The path the store reads from and writes to.
    @NonNull
    Path path() {
        return path;
    }

    /// Load the persisted metadata.
    ///
    /// @return the persisted metadata, or empty if the file does not exist yet.
    /// @throws IOException on read failure or malformed contents.
    @NonNull
    Optional<StateMetadata> load() throws IOException {
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        final byte[] bytes = Files.readAllBytes(path);
        try {
            return Optional.of(StateMetadata.JSON.parse(Bytes.wrap(bytes)));
        } catch (final Exception e) {
            throw new IOException("Unable to parse state metadata at " + path, e);
        }
    }

    /// Atomic write of the given metadata. Creates parent directories as needed.
    ///
    /// @param metadata the metadata to persist
    /// @throws IOException if the parent directories or the file cannot be written
    void save(@NonNull final StateMetadata metadata) throws IOException {
        Files.createDirectories(path.getParent());
        final Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.write(tmp, StateMetadata.JSON.toBytes(metadata).toByteArray());
        try {
            Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (final IOException atomicFailed) {
            // Some filesystems (e.g. older WSL) reject ATOMIC_MOVE — fall back to a plain replace.
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
