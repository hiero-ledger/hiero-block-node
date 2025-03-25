package org.hiero.block.node.base;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
    public InputStream wrapStream(InputStream streamToWrap) throws IOException {
        return switch (this) {
            case ZSTD -> new com.github.luben.zstd.ZstdInputStream(streamToWrap);
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
    public OutputStream wrapStream(OutputStream streamToWrap) throws IOException {
        return switch (this) {
            case ZSTD -> new com.github.luben.zstd.ZstdOutputStream(streamToWrap, 3);
            case NONE -> streamToWrap;
        };
    }
}
