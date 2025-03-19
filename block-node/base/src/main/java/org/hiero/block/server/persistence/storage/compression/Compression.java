// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.compression;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;

/**
 * A compression abstractions that allows for the compression of bytes using
 * different compression algorithms based on specific implementation.
 */
public interface Compression {
    /**
     * This method takes a valid, {@code non-null} {@link OutputStream} instance
     * and wraps it with the specific compression algorithm implementation. The
     * resulting {@link OutputStream} is then returned.
     *
     * @param streamToWrap a valid {@code non-null} {@link OutputStream} to wrap
     * @return a {@code non-null} {@link OutputStream} that wraps the provided
     * {@link OutputStream} with the specific compression algorithm
     * implementation
     * @throws IOException if an I/O exception occurs
     */
    @NonNull
    OutputStream wrap(@NonNull final OutputStream streamToWrap) throws IOException;

    /**
     * This method takes a valid, {@code non-null} {@link InputStream} instance
     * and wraps it with the specified compression algorithm implementation. The
     * resulting {@link InputStream} is then returned.
     *
     * @param streamToWrap a valid {@code non-null} {@link InputStream} to wrap
     * @param compressionType valid, {@code non-null} {@link CompressionType}
     * that specifies the compression algorithm to use
     * @return a {@code non-null} {@link InputStream} that wraps the provided
     * {@link InputStream} with the specific compression algorithm implementation
     * @throws IOException if an I/O exception occurs
     */
    @NonNull
    InputStream wrap(@NonNull final InputStream streamToWrap, final CompressionType compressionType) throws IOException;

    /**
     * This method aims to return a valid, {@code non-blank} {@link String} that
     * represents the file extension for the given specific implementation,
     * based on the compression algorithm used.
     *
     * @return a valid, {@code non-blank} {@link String} that represents the
     * file extension for the given specific implementation, based on the
     * compression algorithm used
     */
    @NonNull
    String getCompressionFileExtension();
}
