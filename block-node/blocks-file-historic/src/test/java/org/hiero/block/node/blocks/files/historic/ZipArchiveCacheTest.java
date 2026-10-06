// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link ZipArchiveCache}, exercised directly against small real zip files.
 */
class ZipArchiveCacheTest {
    @TempDir
    private Path tempDir;

    private Path createZip(final String name) throws IOException {
        final Path zip = tempDir.resolve(name);
        try (final ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip))) {
            out.putNextEntry(new ZipEntry("entry.txt"));
            out.write(name.getBytes());
            out.closeEntry();
        }
        return zip;
    }

    @Test
    @DisplayName("acquire() opens and caches an archive, and release keeps its filesystem open")
    void testAcquireCachesAndReleaseKeepsOpen() throws IOException {
        final ZipArchiveCache cache = new ZipArchiveCache(2);
        final Path zip = createZip("a.zip");

        final ZipArchiveCache.ArchiveHandle handle = cache.acquire(zip);
        assertThat(cache.size()).isEqualTo(1);
        assertThat(handle.fileSystem().isOpen()).isTrue();
        assertThat(Files.readString(handle.fileSystem().getPath("entry.txt"))).isEqualTo("a.zip");

        handle.close();
        assertThat(cache.size()).isEqualTo(1);
        assertThat(handle.fileSystem().isOpen()).isTrue();
        cache.close();
    }

    @Test
    @DisplayName("acquire() of an already cached archive returns the same shared handle")
    void testAcquireTwiceSharesHandle() throws IOException {
        final ZipArchiveCache cache = new ZipArchiveCache(2);
        final Path zip = createZip("a.zip");

        final ZipArchiveCache.ArchiveHandle first = cache.acquire(zip);
        final ZipArchiveCache.ArchiveHandle second = cache.acquire(zip);
        assertThat(second).isSameAs(first);
        assertThat(cache.size()).isEqualTo(1);
        first.close();
        second.close();
        cache.close();
    }

    @Test
    @DisplayName("acquire() of a missing or invalid zip throws and leaves the cache unchanged")
    void testAcquireInvalidArchiveThrows() throws IOException {
        final ZipArchiveCache cache = new ZipArchiveCache(2);
        final Path corrupt = tempDir.resolve("corrupt.zip");
        Files.writeString(corrupt, "not a zip");

        assertThatIOException().isThrownBy(() -> cache.acquire(tempDir.resolve("missing.zip")));
        assertThatIOException().isThrownBy(() -> cache.acquire(corrupt));
        assertThat(cache.size()).isZero();
    }

    @Test
    @DisplayName("Exceeding the bound evicts and closes the least-recently-used idle archive")
    void testLeastRecentlyUsedIdleArchiveIsEvicted() throws IOException {
        final ZipArchiveCache cache = new ZipArchiveCache(2);
        final Path zipA = createZip("a.zip");
        final Path zipB = createZip("b.zip");
        final Path zipC = createZip("c.zip");

        final ZipArchiveCache.ArchiveHandle a = cache.acquire(zipA);
        a.close();
        final ZipArchiveCache.ArchiveHandle b = cache.acquire(zipB);
        b.close();
        // touch A so B becomes the least recently used
        cache.acquire(zipA).close();
        final ZipArchiveCache.ArchiveHandle c = cache.acquire(zipC);
        c.close();

        assertThat(cache.size()).isEqualTo(2);
        assertThat(b.fileSystem().isOpen()).isFalse();
        assertThat(a.fileSystem().isOpen()).isTrue();
        assertThat(c.fileSystem().isOpen()).isTrue();
        cache.close();
    }

    @Test
    @DisplayName("An archive with outstanding references is never evicted, even over the bound")
    void testInUseArchiveIsNotEvicted() throws IOException {
        final ZipArchiveCache cache = new ZipArchiveCache(1);
        final ZipArchiveCache.ArchiveHandle held = cache.acquire(createZip("a.zip"));

        final ZipArchiveCache.ArchiveHandle other = cache.acquire(createZip("b.zip"));
        other.close();

        assertThat(cache.size()).isEqualTo(2);
        assertThat(held.fileSystem().isOpen()).isTrue();

        // once released it becomes evictable by the next insertion
        held.close();
        cache.acquire(createZip("c.zip")).close();
        assertThat(held.fileSystem().isOpen()).isFalse();
        cache.close();
    }

    @Test
    @DisplayName("evict() closes and removes an idle archive but not one in use or unknown")
    void testEvict() throws IOException {
        final ZipArchiveCache cache = new ZipArchiveCache(4);
        final Path zip = createZip("a.zip");
        final ZipArchiveCache.ArchiveHandle handle = cache.acquire(zip);

        cache.evict(zip);
        assertThat(cache.size()).isEqualTo(1);
        assertThat(handle.fileSystem().isOpen()).isTrue();

        handle.close();
        cache.evict(tempDir.resolve("unknown.zip"));
        assertThat(cache.size()).isEqualTo(1);

        cache.evict(zip);
        assertThat(cache.size()).isZero();
        assertThat(handle.fileSystem().isOpen()).isFalse();
    }

    @Test
    @DisplayName("close() closes every cached filesystem and empties the cache, which stays usable")
    void testCloseClosesAll() throws IOException {
        final ZipArchiveCache cache = new ZipArchiveCache(4);
        final ZipArchiveCache.ArchiveHandle a = cache.acquire(createZip("a.zip"));
        final ZipArchiveCache.ArchiveHandle b = cache.acquire(createZip("b.zip"));

        cache.close();

        assertThat(cache.size()).isZero();
        assertThat(a.fileSystem().isOpen()).isFalse();
        assertThat(b.fileSystem().isOpen()).isFalse();
        final ZipArchiveCache.ArchiveHandle reopened = cache.acquire(createZip("a.zip"));
        assertThat(reopened.fileSystem().isOpen()).isTrue();
        cache.close();
    }

    @Test
    @DisplayName("Concurrent first acquire of one archive yields a single shared handle with balanced ref counts")
    void testConcurrentAcquireOfSameArchive() throws Exception {
        final Path zip = createZip("a.zip");
        final int threadCount = 8;
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            for (int round = 0; round < 50; round++) {
                final ZipArchiveCache cache = new ZipArchiveCache(4);
                final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                final List<Callable<ZipArchiveCache.ArchiveHandle>> tasks = new ArrayList<>();
                for (int t = 0; t < threadCount; t++) {
                    tasks.add(() -> {
                        barrier.await(10, TimeUnit.SECONDS);
                        return cache.acquire(zip);
                    });
                }
                final List<ZipArchiveCache.ArchiveHandle> handles = new ArrayList<>();
                for (final Future<ZipArchiveCache.ArchiveHandle> future :
                        executor.invokeAll(tasks, 30, TimeUnit.SECONDS)) {
                    handles.add(future.get());
                }

                assertThat(handles).allMatch(h -> h == handles.get(0));
                assertThat(cache.size()).isEqualTo(1);
                assertThat(handles.get(0).fileSystem().isOpen()).isTrue();
                // still referenced by all threads, so evict is a no-op until every reference is released
                cache.evict(zip);
                assertThat(cache.size()).isEqualTo(1);
                handles.forEach(ZipArchiveCache.ArchiveHandle::close);
                cache.evict(zip);
                assertThat(cache.size()).isZero();
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
