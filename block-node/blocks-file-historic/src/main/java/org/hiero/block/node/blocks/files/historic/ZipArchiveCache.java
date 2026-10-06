// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static java.lang.System.Logger.Level.INFO;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A small, bounded, lock-free cache of open zip archive filesystems, keyed by archive path and reference-counted
 * across concurrently active readers. Used by {@link ZipBlockArchive} to back {@link CachedZipBlockAccessor}, so
 * that consecutive reads from the same archive (the common case, since archives typically hold many thousands of
 * blocks) reuse one open filesystem instead of each accessor opening and indexing its own.
 * <p>
 * Callers {@link #acquire} a {@link ArchiveHandle} and release it with {@link ArchiveHandle#close()}. Releasing
 * does not close the underlying filesystem; it stays cached for reuse until evicted.
 * <p>
 * <b>Concurrency protocol.</b> There are no locks. Each handle's reference count is an {@link AtomicInteger}
 * with one sentinel: {@link #CLOSED} (negative) means the handle has been claimed for closing.
 * <ul>
 *   <li>Taking a reference is a CAS loop that increments only while the count is {@code >= 0}; it fails once the
 *   handle is {@link #CLOSED}.</li>
 *   <li>Closing a handle (eviction) is a single CAS from {@code 0} to {@link #CLOSED}, so it can only succeed
 *   while nobody holds a reference, and after it succeeds nobody can take one.</li>
 * </ul>
 * Together these guarantee a filesystem is never closed while a reader holds a reference, and a reader can never
 * be handed an already-closed handle: a reader that loses that race simply discards the handle and retries with a
 * fresh one. Opening a missing archive happens without any exclusion; if two threads race to open the same
 * archive the loser closes its redundant filesystem and uses the winner's.
 */
final class ZipArchiveCache {
    /** Ref count sentinel marking a handle that has been claimed for closing; no reference can be taken from it. */
    private static final int CLOSED = -1;

    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** Maximum number of open archive filesystems to keep cached. */
    private final int maxCachedArchives;
    /** Open archive filesystems, keyed by archive path. */
    private final Map<Path, ArchiveHandle> openArchives = new ConcurrentHashMap<>();

    /**
     * @param maxCachedArchives maximum number of open archive filesystems to keep cached
     */
    ZipArchiveCache(final int maxCachedArchives) {
        this.maxCachedArchives = maxCachedArchives;
    }

    /**
     * A shared, reference-counted handle to an open zip archive filesystem. Multiple concurrent
     * {@link CachedZipBlockAccessor}s reading from the same archive hold a reference to the same handle.
     * <p>
     * Implements {@link AutoCloseable} so callers can pair acquisition with release using try-with-resources
     * (or an explicit {@link #close()} on early-return paths); {@link #close()} releases this caller's
     * reference rather than necessarily closing the underlying filesystem -- see {@link #release}.
     * Not {@code static} so it can call back into the enclosing cache to release itself.
     */
    final class ArchiveHandle implements AutoCloseable {
        private final Path zipFilePath;
        private final FileSystem fileSystem;
        /** Number of active references, or {@link #CLOSED} once claimed for closing. */
        private final AtomicInteger refCount;
        /** {@link System#nanoTime()} of the last acquire or release, for LRU ordering. */
        private volatile long lastUsed;

        private ArchiveHandle(
                @NonNull final Path zipFilePath, @NonNull final FileSystem fileSystem, final int initialRefs) {
            this.zipFilePath = zipFilePath;
            this.fileSystem = fileSystem;
            this.refCount = new AtomicInteger(initialRefs);
            this.lastUsed = System.nanoTime();
        }

        FileSystem fileSystem() {
            return fileSystem;
        }

        /** Takes a reference unless the handle is already claimed for closing. */
        private boolean tryAcquire() {
            int current;
            do {
                current = refCount.get();
                if (current < 0) {
                    return false;
                }
            } while (!refCount.compareAndSet(current, current + 1));
            lastUsed = System.nanoTime();
            return true;
        }

        /** Claims the handle for closing, which only succeeds while it has no references. */
        private boolean tryClaimForClose() {
            return refCount.compareAndSet(0, CLOSED);
        }

        @Override
        public void close() {
            release(this);
        }
    }

    /**
     * Acquires a shared reference to the open filesystem for the given archive, opening and caching it if it is
     * not already cached. Every successful call must be paired with exactly one {@link ArchiveHandle#close()}
     * call.
     *
     * @param zipFilePath the path to the zip archive, must already be known to exist
     * @return a handle with an active reference already counted for the caller
     */
    ArchiveHandle acquire(@NonNull final Path zipFilePath) throws IOException {
        while (true) {
            final ArchiveHandle cached = openArchives.get(zipFilePath);
            if (cached != null) {
                if (cached.tryAcquire()) {
                    return cached;
                }
                // Claimed for closing by an eviction; drop it from the map (a no-op if the evictor already did)
                // and retry with a fresh handle.
                openArchives.remove(zipFilePath, cached);
                continue;
            }
            // Cache miss: open the filesystem. Opening a zip reads and indexes its central directory.
            final FileSystem opened = FileSystems.newFileSystem(zipFilePath);
            final ArchiveHandle created = new ArchiveHandle(zipFilePath, opened, 1);
            final ArchiveHandle raced = openArchives.putIfAbsent(zipFilePath, created);
            if (raced == null) {
                evictOverflow();
                return created;
            }
            // Another thread cached the same archive while we were opening ours: close our redundant
            // filesystem and loop to take a reference on theirs.
            closeQuietly(zipFilePath, opened);
        }
    }

    /**
     * Releases a reference previously acquired via {@link #acquire}. The underlying filesystem is not
     * necessarily closed immediately: it stays cached for reuse until evicted. Called from
     * {@link ArchiveHandle#close()}; not invoked directly.
     */
    private void release(@NonNull final ArchiveHandle handle) {
        handle.lastUsed = System.nanoTime();
        handle.refCount.decrementAndGet();
    }

    /**
     * Evicts least-recently-used idle archives until the cache is back within its bound, or no idle archive is
     * left to evict. Archives with outstanding references are never evicted, so the cache can stay above its
     * bound while they are in use; it shrinks on a later insertion once they are released.
     */
    private void evictOverflow() {
        while (openArchives.size() > maxCachedArchives) {
            ArchiveHandle victim = null;
            for (final ArchiveHandle candidate : openArchives.values()) {
                if (candidate.refCount.get() == 0 && (victim == null || candidate.lastUsed - victim.lastUsed < 0)) {
                    victim = candidate;
                }
            }
            if (victim == null) {
                return;
            }
            // Fails if the candidate was acquired since we looked; just rescan.
            closeIfIdle(victim);
        }
    }

    /** Removes and closes the handle if, and only if, it has no references. Returns whether it did. */
    private boolean closeIfIdle(@NonNull final ArchiveHandle handle) {
        if (!handle.tryClaimForClose()) {
            return false;
        }
        openArchives.remove(handle.zipFilePath, handle);
        closeQuietly(handle.zipFilePath, handle.fileSystem);
        return true;
    }

    private void closeQuietly(@NonNull final Path zipFilePath, @NonNull final FileSystem fileSystem) {
        try {
            fileSystem.close();
        } catch (final IOException | RuntimeException e) {
            // Not expected to cause problems for the running system; INFO so operators still see it happening.
            LOGGER.log(INFO, "Failed to close cached zip archive filesystem for: %s".formatted(zipFilePath), e);
        }
    }

    /**
     * Evicts and closes the cached filesystem for the given archive path, if cached and not currently in use by
     * an active accessor. Intended to be called right after the archive's zip file has been deleted (e.g. by
     * retention policy pruning), so the cache does not keep holding a filesystem open for a file that no longer
     * exists on disk any longer than necessary. If the archive is still actively referenced, this is a no-op;
     * it will be cleaned up by the normal LRU eviction once released.
     *
     * @param zipFilePath the path to the archive that was deleted
     */
    void evict(@NonNull final Path zipFilePath) {
        final ArchiveHandle handle = openArchives.get(zipFilePath);
        if (handle != null) {
            closeIfIdle(handle);
        }
    }

    /**
     * Closes every cached archive filesystem, whether or not it is still referenced, and empties the cache.
     * Should be called when the owning plugin stops.
     */
    void close() {
        for (final ArchiveHandle handle : openArchives.values()) {
            handle.refCount.set(CLOSED);
            openArchives.remove(handle.zipFilePath, handle);
            closeQuietly(handle.zipFilePath, handle.fileSystem);
        }
    }

    /** Returns the number of archive filesystems currently cached. Package-private for testing. */
    int size() {
        return openArchives.size();
    }
}
