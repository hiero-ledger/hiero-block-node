package org.hiero.block.tools.utils;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A simple output stream that prints progress to the console.
 */
public class ProgressOutputStream extends FilterOutputStream {
    private static final long MB = 1024 * 1024;
    private final long size;
    private final String name;
    private long bytesWritten = 0;

    /**
     * Create new progress output stream.
     *
     * @param out  the output stream to wrap
     * @param size the size of the output stream
     * @param name the name of the output stream
     */
    public ProgressOutputStream(OutputStream out, long size, String name) {
        super(out);
        this.size = size;
        this.name = name;
    }

    /**
     * Write a byte to the output stream.
     *
     * @param b the byte to write
     * @throws IOException if an error occurs writing the byte
     */
    @Override
    public void write(int b) throws IOException {
        super.write(b);
        bytesWritten++;
        printProgress();
    }

    /**
     * Write a byte array to the output stream.
     *
     * @param b   the byte array to write
     * @param off the offset in the byte array to start writing
     * @param len the number of bytes to write
     * @throws IOException if an error occurs writing the byte array
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        super.write(b, off, len);
        bytesWritten += len;
        printProgress();
    }

    /**
     * Print the progress of the output stream to the console.
     */
    private void printProgress() {
        if (bytesWritten % MB == 0) {
            System.out.printf(
                "\rProgress: %.0f%% - %,d MB written of %s",
                (bytesWritten / (double) size) * 100d, bytesWritten / MB, name);
        }
    }
}
