// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;

/**
 * A utility class to write .tar.zstd files concurrently from multiple threads.
 * <p>
 * Usage:
 * <pre>
 * try (ConcurrentTarZstdWriter writer = new ConcurrentTarZstdWriter(Path.of("output.tar.zstd"))) {
 *     writer.putEntry(new InMemoryFile(Path.of("file1.txt"), data1));
 *     writer.putEntry(new InMemoryFile(Path.of("file2.txt"), data2));
 * }
 * </pre>
 * This will create a tar.zstd file with file1.txt and file2.txt entries.
 */
public class ConcurrentTarZstdWriter implements AutoCloseable {
    private final OutputStream fout;
    private final ZstdOutputStream zOut;
    private final TarArchiveOutputStream tar;
    private final Path outFile;
    private final Path partialOutFile;
    private final ArrayBlockingQueue<InMemoryFile> filesToWrite = new ArrayBlockingQueue<>(1000);
    private final Thread writerThread;
    private final AtomicReference<Throwable> writerError = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ConcurrentTarZstdWriter(Path outFile) throws Exception {
        // check the file extension is .tar.zstd
        if (!outFile.toString().endsWith(".tar.zstd")) {
            throw new IllegalArgumentException("Output path must end with .tar.zstd");
        }
        // check file does not already exist
        if (outFile.toFile().exists()) {
            throw new IllegalArgumentException("Output file already exists: " + outFile);
        }
        this.outFile = outFile;
        this.partialOutFile = outFile.getParent().resolve(outFile.getFileName() + "_partial");
        this.fout = Files.newOutputStream(partialOutFile);
        this.zOut = new ZstdOutputStream(
                new BufferedOutputStream(fout, 1024 * 1024 * 128), RecyclingBufferPool.INSTANCE, /*level*/ 3);
        this.tar = new TarArchiveOutputStream(new BufferedOutputStream(zOut, 1024 * 1024 * 128));
        tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
        // start writer thread
        this.writerThread = new Thread(
                () -> {
                    try {
                        final java.util.ArrayList<InMemoryFile> batch = new java.util.ArrayList<>(100);
                        while (true) {
                            InMemoryFile file;
                            try {
                                // poll with timeout so we can wake up when close() sets the closed flag
                                file = filesToWrite.poll(250, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException ie) {
                                // Interrupted while polling: restore interrupt and exit loop
                                Thread.currentThread().interrupt();
                                break;
                            }
                            if (file == null) {
                                // no item this interval; exit if closed and queue empty
                                if (closed.get() && filesToWrite.isEmpty()) break;
                                continue;
                            }
                            // drain up to 100 files at once for batch processing
                            batch.clear();
                            batch.add(file);
                            filesToWrite.drainTo(batch, 99);
                            // process the batch
                            for (InMemoryFile f : batch) {
                                TarArchiveEntry entry =
                                        new TarArchiveEntry(f.path().toString());
                                entry.setSize(f.data().length);
                                tar.putArchiveEntry(entry);
                                tar.write(f.data());
                                tar.closeArchiveEntry();
                            }
                        }
                    } catch (Throwable t) {
                        // record the error for later rethrow in close()
                        writerError.compareAndSet(null, t);
                    }
                },
                "concurrent-tar-writer");
        this.writerThread.setDaemon(true);
        this.writerThread.start();
    }

    /**
     * Add an entry to the tar.zstd file. This method is thread-safe and can be called from multiple threads.
     *
     * @param file the in-memory file to add
     */
    public void putEntry(InMemoryFile file) {
        Objects.requireNonNull(file);
        // don't accept new entries after close() called
        if (closed.get()) {
            throw new IllegalStateException("Writer is closed");
        }
        // propagate writer thread errors early
        Throwable t = writerError.get();
        if (t != null) {
            throw new RuntimeException("Writer thread failed previously", t);
        }
        try {
            // try to enqueue but avoid blocking forever if the writer dies; periodically check status
            while (!filesToWrite.offer(file, 5, TimeUnit.SECONDS)) {
                t = writerError.get();
                if (t != null) {
                    throw new RuntimeException("Writer thread failed previously", t);
                }
                if (!writerThread.isAlive()) {
                    throw new IllegalStateException("Writer thread not alive and queue is full");
                }
                if (closed.get()) {
                    throw new IllegalStateException("Writer is closed");
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // convert to unchecked to avoid forcing callers to handle checked InterruptedException
            throw new RuntimeException("Interrupted while enqueuing file to writer", ie);
        }
    }

    /** Close the writer, waiting for all entries to be written. */
    @Override
    public void close() throws Exception {
        // mark closed so writer thread will finish after it drains the queue
        if (!closed.compareAndSet(false, true)) {
            return; // already closed
        }

        // wait for writer thread to finish (bounded)
        try {
            writerThread.join(60_000);
            if (writerThread.isAlive()) {
                writerError.compareAndSet(
                        null, new IllegalStateException("Writer thread did not terminate within timeout"));
                // interrupt as a last resort
                writerThread.interrupt();
                writerThread.join(5_000);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        // if the writer had an error, rethrow it
        Throwable t = writerError.get();
        if (t != null) {
            throw new RuntimeException("Writer thread failed", t);
        }
        // close streams and finish tar
        try {
            tar.finish();
        } finally {
            try {
                tar.close();
            } catch (Exception ignored) {
            }
            try {
                zOut.close();
            } catch (Exception ignored) {
            }
            try {
                fout.close();
            } catch (Exception ignored) {
            }
        }
        // rename partial file to final file
        Files.move(partialOutFile, outFile);
    }
}
