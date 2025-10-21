package org.hiero.block.tools.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * An InputStream that decompresses data using Zstandard (zstd) command line tool with multiple threads.
 */
public class ZstCmdInputStream extends InputStream {

    private final Process process;
    private final InputStream procIn;
    private final ByteArrayOutputStream procErr = new ByteArrayOutputStream();
    private volatile boolean closed = false;
    // indicates whether the decompressor reached EOF (we consumed entire stdout)
    private volatile boolean eofReached = false;

    public ZstCmdInputStream(Path zstdFilePath) throws IOException {
        this(zstdFilePath, 1);
    }

    public ZstCmdInputStream(Path zstdFilePath, int numberOfThreads) throws IOException {
        if (zstdFilePath == null) throw new IllegalArgumentException("zstdFilePath is null");
        if (!zstdFilePath.toString().endsWith(".zstd")) {
            throw new IllegalArgumentException("file must have .zstd extension: " + zstdFilePath);
        }
        if (numberOfThreads <= 0) numberOfThreads = 1;

        List<String> cmd = new ArrayList<>();
        cmd.add("zstd");
        cmd.add("--decompress");
        cmd.add("--stdout");
        // threads option may be supported as -T or --threads depending on zstd version. Use -T<number> if provided
        // prefer --threads=<n> if available; pass as -T<number> which is widely supported.
        if (numberOfThreads > 1) {
            cmd.add("-T" + numberOfThreads);
        }
        cmd.add(zstdFilePath.toAbsolutePath().toString());

        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            // keep stderr separate so we can capture errors
            pb.redirectErrorStream(false);
            process = pb.start();
            procIn = process.getInputStream();

            // spawn a thread to read stderr so the process doesn't block on full buffer
            Thread errDrainer = new Thread(() -> {
                try (InputStream err = process.getErrorStream()) {
                    byte[] buf = new byte[8192];
                    int r;
                    while ((r = err.read(buf)) != -1) {
                        procErr.write(buf, 0, r);
                    }
                } catch (IOException ignored) {
                    // ignore
                }
            }, "zstd-stderr-drainer");
            errDrainer.setDaemon(true);
            errDrainer.start();
        } catch (IOException e) {
            throw new IOException("Failed to start zstd process: " + e.getMessage(), e);
        }
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        int v = procIn.read();
        if (v == -1) {
            eofReached = true;
            checkProcessExit();
        }
        return v;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        int r = procIn.read(b, off, len);
        if (r == -1) {
            eofReached = true;
            checkProcessExit();
        }
        return r;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            try {
                procIn.close();
            } catch (IOException ignored) {}
            // wait a short time for process to exit
            try {
                process.waitFor();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            int exit = process.exitValue();
            // Only throw if we actually consumed EOF and process exited non-zero. If caller closed early (didn't reach EOF),
            // don't treat a non-zero exit as an error because the process may have been terminated due to broken pipe.
            if (eofReached && exit != 0) {
                String err = procErr.toString(StandardCharsets.UTF_8.name());
                throw new IOException("zstd process exited with code " + exit + ": " + err);
            }
        } finally {
            // ensure streams closed
            try {
                process.getErrorStream().close();
            } catch (IOException ignored) {}
            try {
                process.getOutputStream().close();
            } catch (IOException ignored) {}
            // best-effort destroy process if it's still alive
            try {
                if (process.isAlive()) process.destroyForcibly();
            } catch (Exception ignored) {}
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("Stream closed");
    }

    private void checkProcessExit() throws IOException {
        try {
            int exit = process.waitFor();
            if (exit != 0) {
                String err = procErr.toString(StandardCharsets.UTF_8.name());
                throw new IOException("zstd process exited with code " + exit + ": " + err);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for zstd process", ie);
        }
    }
}
