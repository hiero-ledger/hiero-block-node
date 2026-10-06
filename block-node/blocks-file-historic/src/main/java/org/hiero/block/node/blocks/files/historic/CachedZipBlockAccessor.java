// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Path;

/**
 * The CachedZipBlockAccessor class provides access to a block stored in a zip file, sharing a single open
 * filesystem across every accessor currently reading from the same archive rather than opening (and indexing)
 * its own filesystem per block, unlike {@link ZipBlockAccessor}.
 * <p>
 * Reads go through a {@link ZipArchiveCache.ArchiveHandle} shared and reference-counted across every accessor
 * currently reading from the same archive. {@link #close()} releases this accessor's reference; the underlying
 * filesystem is not necessarily closed at that point, since {@link ZipArchiveCache} keeps recently-used archives
 * cached for reuse by later reads.
 * <p>
 * This class exists alongside {@link ZipBlockAccessor} (rather than replacing it) so the two implementations can
 * be switched between via {@link FilesHistoricConfig#cachedZipAccessorEnabled()} and profiled/compared, and so
 * there is a known-safe fallback if a shared, concurrently-read zip filesystem ever proves unsafe in practice.
 */
final class CachedZipBlockAccessor extends AbstractZipBlockAccessor {
    /** The shared, reference-counted handle to this block's archive filesystem. */
    private final ZipArchiveCache.ArchiveHandle archiveHandle;
    /** Whether this accessor has been closed. */
    private volatile boolean closed = false;

    /**
     * Constructs a CachedZipBlockAccessor for a block resolved within an already-acquired archive handle.
     *
     * @param blockPath the resolved block path
     * @param archiveHandle the caller's reference to the shared archive filesystem, acquired for this accessor;
     *                      released via {@link ZipArchiveCache.ArchiveHandle#close()} on {@link #close()}
     */
    CachedZipBlockAccessor(
            @NonNull final BlockPath blockPath, @NonNull final ZipArchiveCache.ArchiveHandle archiveHandle) {
        super(blockPath, blockPath.zipFilePath());
        this.archiveHandle = requireNonNull(archiveHandle);
    }

    @Override
    protected Bytes readEntry(@NonNull final Format format) throws IOException {
        final Path entry = archiveHandle.fileSystem().getPath(blockPathData.blockFileName());
        return getBytesFromPath(format, entry, blockPathData.compressionType());
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                archiveHandle.close();
            } catch (final RuntimeException e) {
                // Failing to release/close a cached archive handle is not critical to the caller closing this
                // accessor; log for operator visibility rather than throwing out of close().
                LOGGER.log(INFO, "Failed to release archive handle for block " + blockNumber(), e);
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
