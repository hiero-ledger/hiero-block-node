// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.compression;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.OutputStream;
import java.util.Objects;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;

/**
 * An implementation of {@link Compression} that does not compress the data. It
 * does not use any algorithm, but simply generates a stream that writes the
 * data to it`s destination, as it is received.
 */
public final class NoOpCompression extends CompressionBase {
    /**
     * Constructor.
     */
    private NoOpCompression() {}

    /**
     * Factory method. Returns a new, fully initialized instance of
     * {@link NoOpCompression}.
     *
     * @return a new, fully initialized instance of {@link NoOpCompression}
     */
    @NonNull
    public static NoOpCompression newInstance() {
        return new NoOpCompression();
    }

    /**
     * This implementation does not compress the data. It uses no compression
     * algorithm, but simply generates a stream that writes the data to it`s
     * destination, as it is received.
     * @see Compression#wrap(OutputStream) for API contract
     */
    @NonNull
    @Override
    public OutputStream wrap(@NonNull final OutputStream streamToWrap) {
        return Objects.requireNonNull(streamToWrap);
    }

    @NonNull
    @Override
    public String getCompressionFileExtension() {
        return CompressionType.NONE.getFileExtension();
    }
}
