// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.listing;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;

/**
 * Utility class to write bad lines to a file with a timestamped filename. It is thread-safe so can be called from
 * parallel streams.
 */
public class BadLinesWriter implements AutoCloseable {
    /** A FileWriter in append mode to write bad lines to a file named "badlines_<timestamp>.txt". */
    private final FileWriter writer;

    /** Constructor that initializes the FileWriter and create new bad lines file. */
    public BadLinesWriter() {
        try {
            writer = new FileWriter("badlines_" + Instant.now() + ".txt", true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Synchronized method to write a bad line to the file.
     *
     * @param line the bad line to write
     */
    public synchronized void writeBadLine(String line) {
        try {
            writer.write(line);
            writer.write("\n");
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Closes the FileWriter.
     *
     * @throws Exception if an I/O error occurs
     */
    @Override
    public synchronized void close() throws Exception {
        writer.close();
    }
}
