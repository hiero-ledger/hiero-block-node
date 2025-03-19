// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.compression;

import com.github.luben.zstd.ZstdOutputStream;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;

/**
 * An implementation of {@link Compression} that compresses the data using the
 * Zstandard (Zstd) compression algorithm.
 */
public final class ZstdCompression extends CompressionBase {
    private final int compressionLevel;

    /**
     * Constructor.
     *
     * @param config the {@link PersistenceStorageConfig} instance that provides
     * the configuration for the compression algorithm
     */
    private ZstdCompression(@NonNull final PersistenceStorageConfig config) {
        this.compressionLevel = config.compressionLevel();
    }

    /**
     * Factory method. Returns a new, fully initialized instance of
     * {@link ZstdCompression}.
     *
     * @param config the {@link PersistenceStorageConfig} instance that provides
     * the configuration for the compression algorithm
     * @return a new, fully initialized and valid instance of
     * {@link ZstdCompression}
     */
    @NonNull
    public static ZstdCompression of(@NonNull final PersistenceStorageConfig config) {
        return new ZstdCompression(config);
    }

    @NonNull
    @Override
    public OutputStream wrap(@NonNull final OutputStream streamToWrap) throws IOException {
        return new ZstdOutputStream(Objects.requireNonNull(streamToWrap), compressionLevel);
    }

    @NonNull
    @Override
    public String getCompressionFileExtension() {
        return CompressionType.ZSTD.getFileExtension();
    }
}
