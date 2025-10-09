package org.hiero.block.tools.utils;

import com.github.luben.zstd.ZstdOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.hiero.block.tools.commands.days.model.InMemoryFile;

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
    private final ArrayBlockingQueue<InMemoryFile> filesToWrite = new ArrayBlockingQueue<>(100);
    private final Thread writerThread;

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
        this. partialOutFile = outFile.getParent().resolve(outFile.getFileName() + "_partial");
        this.fout = Files.newOutputStream(partialOutFile);
        this.zOut = new ZstdOutputStream(fout, /*level*/6);
        this.tar = new TarArchiveOutputStream(zOut);
        tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
        // start writer thread
        this.writerThread = new Thread(() -> {
            try {
                // just keep the thread alive to allow synchronized writes
                while (!Thread.currentThread().isInterrupted()) {
                    InMemoryFile file;
                    file = filesToWrite.take();
                    TarArchiveEntry entry = new TarArchiveEntry(file.path().toString());
                    entry.setSize(file.data().length);
                    tar.putArchiveEntry(entry);
                    tar.write(file.data());
                    tar.closeArchiveEntry();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        this.writerThread.start();
    }

    /**
     * Add an entry to the tar.zstd file. This method is thread-safe and can be called from multiple threads.
     *
     * @param file the in-memory file to add
     */
    public void putEntry(InMemoryFile file) throws Exception {
        // add entry to queue
        filesToWrite.add(Objects.requireNonNull(file));
    }

    /** Close the writer, waiting for all entries to be written. */
    @Override
    public void close() throws Exception {
        // wait for queue to drain
        while (!filesToWrite.isEmpty()) {
            Thread.sleep(100);
        }
        // stop writer thread
        writerThread.interrupt();
        writerThread.join();
        // close streams
        tar.finish();
        tar.close();
        zOut.close();
        fout.close();
        // rename partial file to final file
        Files.move(partialOutFile, outFile);
    }
}
