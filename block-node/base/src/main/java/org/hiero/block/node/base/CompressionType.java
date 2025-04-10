// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * An enum that reflects the type of compression that is used to compress the blocks that are stored within the
 * persistence storage.
 */
public enum CompressionType {
    /**
     * This type of compression is used to compress the blocks using the `Zstandard` algorithm and default compression
     * level 3.
     */
    ZSTD(".zstd"),
    /**
     * This type means no compression will be done.
     */
    NONE("");

    /** The default compression level for Zstandard compression. */
    private static final int DEFAULT_ZSTD_COMPRESSION_LEVEL = 3;
    /** The file extension for this compression type. */
    private final String fileExtension;

    /**
     * Constructor.
     *
     * @param fileExtension the file extension for this compression type
     */
    CompressionType(final String fileExtension) {
        this.fileExtension = fileExtension;
    }

    /**
     * Get the file extension for this compression type.
     *
     * @return the file extension for this compression type
     */
    public String extension() {
        return fileExtension;
    }

    /**
     * Wraps the given input stream with the appropriate compression type.
     *
     * @param streamToWrap the stream to wrap
     * @return the wrapped input stream
     * @throws IOException if an I/O error occurs
     */
    public InputStream wrapStream(@NonNull final InputStream streamToWrap) throws IOException {
        Objects.requireNonNull(streamToWrap);
        return switch (this) {
            case ZSTD -> new ZstdInputStream(streamToWrap);
            case NONE -> streamToWrap;
        };
    }

    /**
     * Wraps the given output stream with the appropriate compression type.
     *
     * @param streamToWrap the stream to wrap
     * @return the wrapped output stream
     * @throws IOException if an I/O error occurs
     */
    public OutputStream wrapStream(@NonNull final OutputStream streamToWrap) throws IOException {
        Objects.requireNonNull(streamToWrap);
        return switch (this) {
            case ZSTD -> new ZstdOutputStream(streamToWrap, DEFAULT_ZSTD_COMPRESSION_LEVEL);
            case NONE -> streamToWrap;
        };
    }

    /**
     * Compresses the given data using the appropriate compression type.
     *
     * @param data the data to compress
     * @return the compressed data
     */
    public byte[] compress(@NonNull final byte[] data) {
        Objects.requireNonNull(data);
        return switch (this) {
            case ZSTD -> Zstd.compress(data, DEFAULT_ZSTD_COMPRESSION_LEVEL);
            case NONE -> data;
        };
    }
}
