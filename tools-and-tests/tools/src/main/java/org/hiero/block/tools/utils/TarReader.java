package org.hiero.block.tools.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.hiero.block.tools.records.InMemoryFile;

/**
 * Utility class for reading tar archives and producing streams of InMemoryFile objects.
 * <p>
 * This reader skips non-regular entries (for example directories and symlinks) and only returns regular file
 * entries. It uses only core JDK APIs and streams the archive using a Spliterator to avoid loading the entire
 * archive into memory. For each file entry the reader allocates a single byte[] sized exactly to the file size
 * reported by the tar header and passes that array directly to the {@link InMemoryFile} constructor.
 */
public class TarReader {
    /** The canonical tar block size in bytes (512). Used for header and padding calculations. */
    private static final int BLOCK_SIZE = 512;

    /**
     * Reads the contents of a tar archive from the given InputStream and returns a stream of InMemoryFile objects. Uses
     * a streaming approach with Spliterator to handle large archives efficiently.
     * <p>
     * The returned Stream is auto-closeable and will close the provided InputStream when the stream is closed.</p>
     * <p>
     * Only regular files are returned; directory entries and other non-regular entries are skipped.</p>
     *
     * @param input the InputStream of the tar archive
     * @return a Stream of InMemoryFile objects representing the files in the tar archive
     * @throws NullPointerException if input is null
     */
    public static Stream<InMemoryFile> readTarContents(InputStream input) {
        Objects.requireNonNull(input, "input cannot be null");
        final Spliterator<InMemoryFile> spliterator = new TarSpliterator(input);
        final Stream<InMemoryFile> stream = StreamSupport.stream(spliterator, false);
        // Ensure closing the stream closes the underlying InputStream
        return stream.onClose(() -> {
            try {
                input.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /** Internal spliterator that reads tar entries lazily from the InputStream */
    private static final class TarSpliterator implements Spliterator<InMemoryFile> {
        /** The underlying InputStream providing tar archive data. */
        private final InputStream in;

        /** Flag indicating whether the end of the archive has been reached. */
        private boolean finished = false;

        /** If a GNU long name ('L') record was encountered this holds the next real entry's name. */
        private String pendingLongName = null;

        /**
         * Creates a new TarSpliterator that reads entries from the provided InputStream.
         *
         * @param in the InputStream containing the tar archive (must not be null)
         */
        TarSpliterator(InputStream in) {
            this.in = in;
        }

        /**
         * Attempts to advance the spliterator by reading the next regular file entry from the tar stream.
         * <p>
         * This method will skip non-regular entries (directories, symlinks, etc.) and will honour GNU long-name
         * records (typeflag 'L') by using the subsequent long name for the next entry. When a regular file entry is
         * encountered this method reads its data fully into a byte[] and passes an {@link InMemoryFile} to the given
         * action.
         * <p>
         * The returned boolean follows the {@link Spliterator#tryAdvance} contract: true if an element was produced,
         * false if no more elements are available.</p>
         * <p>
         * Any IO errors encountered while reading will be wrapped in a RuntimeException.</p>
         *
         * @param action a Consumer that accepts the produced InMemoryFile
         * @return true if a file was produced and the action invoked, false when the archive is exhausted
         * @throws NullPointerException if action is null
         * @throws RuntimeException wrapping an IOException on read failures
         */
        @Override
        public boolean tryAdvance(java.util.function.Consumer<? super InMemoryFile> action) {
            Objects.requireNonNull(action);
            if (finished) return false;
            try {
                while (true) {
                    byte[] header = new byte[BLOCK_SIZE];
                    int headRead = readFully(header);
                    if (headRead == 0) {
                        finished = true;
                        return false;
                    }
                    if (isAllZero(header)) {
                        // End of archive (two consecutive zero blocks is canonical, but one is enough here)
                        finished = true;
                        return false;
                    }
                    final String nameField = readString(header, 0, 100);
                    final String prefix = readString(header, 345, 155);
                    String name = nameField;
                    if (!prefix.isEmpty()) {
                        name = prefix + "/" + nameField;
                    }
                    final byte typeflag = header[156];
                    final long size = parseOctal(header, 124, 12);

                    if (typeflag == 'L') { // GNU long name
                        // Read long name data of length 'size'
                        byte[] nameBytes = new byte[(int) size];
                        readExactly(nameBytes);
                        // consume padding
                        skipPadding(size);
                        // parse as trimmed string (stop at first NUL)
                        pendingLongName = extractStringFromBytes(nameBytes);
                        // continue loop to read the next header for the real file
                        continue;
                    }

                    // Use pendingLongName if present
                    if (pendingLongName != null) {
                        name = pendingLongName;
                        pendingLongName = null;
                    }

                    // If the entry is a directory (typeflag '5') skip it
                    if (typeflag == '5') {
                        // consume any data (usually zero) and padding then continue
                        if (size > 0) skipFully(size);
                        skipPadding(size);
                        continue;
                    }

                    // Only treat regular files as returned entries. Regular files are indicated by NUL (0) or '0'.
                    boolean isRegular = (typeflag == 0 || typeflag == '0');
                    if (!isRegular) {
                        // skip data and padding for non-regular entries (symlinks, hardlinks, etc.)
                        if (size > 0) skipFully(size);
                        skipPadding(size);
                        continue;
                    }

                    if (size > Integer.MAX_VALUE) {
                        throw new RuntimeException("File too large to read into byte[]: " + name + " size=" + size);
                    }
                    final int intSize = (int) size;
                    byte[] data = new byte[intSize];
                    if (intSize > 0) {
                        readExactly(data);
                    }
                    // consume padding to 512
                    skipPadding(size);

                    final Path path = Path.of(name);
                    action.accept(new InMemoryFile(path, data));
                    return true;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Splitting is not supported for this spliterator because the underlying InputStream is sequential.
         *
         * @return null to indicate this spliterator cannot be split
         */
        @Override
        public Spliterator<InMemoryFile> trySplit() { return null; }

        /**
         * Returns an estimate of the remaining size. The implementation cannot know the number of entries in advance
         * without reading the stream, so it returns {@code Long.MAX_VALUE} to satisfy the contract.
         *
         * @return an estimated size (here Long.MAX_VALUE)
         */
        @Override
        public long estimateSize() { return Long.MAX_VALUE; }

        /**
         * Returns the characteristics of this spliterator. This implementation does not report any specific
         * characteristics (e.g. SIZED or SUBSIZED) because the source is a streaming InputStream.
         *
         * @return an int bitmask of characteristics (0 indicates none)
         */
        @Override
        public int characteristics() { return 0; }

        /**
         * Reads up to buf.length bytes into buf and returns the number of bytes actually read. This may be less than
         * the requested length if the underlying stream reaches EOF.
         *
         * @param buf destination buffer
         * @return number of bytes read, possibly zero if EOF is reached immediately
         * @throws IOException if an I/O error occurs
         */
        private int readFully(byte[] buf) throws IOException {
            int off = 0;
            int len = buf.length;
            while (len > 0) {
                int r = in.read(buf, off, len);
                if (r < 0) break;
                off += r;
                len -= r;
            }
            return off; // number of bytes read
        }

        /**
         * Reads exactly buf.length bytes into buf or throws an IOException if EOF is encountered before the
         * requested number of bytes are available.
         *
         * @param buf destination buffer to fill fully
         * @throws IOException if EOF is reached before filling the buffer or another I/O error occurs
         */
        private void readExactly(byte[] buf) throws IOException {
            int off = 0;
            int len = buf.length;
            while (len > 0) {
                int r = in.read(buf, off, len);
                if (r < 0) throw new IOException("Unexpected EOF while reading tar entry data");
                off += r;
                len -= r;
            }
        }

        /**
         * Reads and discards exactly n bytes from the stream. Used to skip the body of entries that are not retained.
         *
         * @param n number of bytes to skip
         * @throws IOException if EOF is reached before skipping n bytes or another I/O error occurs
         */
        private void skipFully(long n) throws IOException {
            long remaining = n;
            byte[] tmp = new byte[8192];
            while (remaining > 0) {
                int toRead = (int) Math.min(tmp.length, remaining);
                int r = in.read(tmp, 0, toRead);
                if (r < 0) throw new IOException("Unexpected EOF while skipping tar entry data");
                remaining -= r;
            }
        }

        /**
         * Skips the padding bytes after an entry to align the stream to the next 512-byte block.
         * For a size that is an exact multiple of {@link #BLOCK_SIZE} no padding is skipped.
         *
         * @param size the size of the entry whose padding should be skipped
         * @throws IOException if an I/O error occurs while skipping padding
         */
        private void skipPadding(long size) throws IOException {
            long rem = size % BLOCK_SIZE;
            if (rem == 0) return;
            long pad = BLOCK_SIZE - rem;
            long skipped = 0;
            while (skipped < pad) {
                long s = in.skip(pad - skipped);
                if (s <= 0) {
                    // fallback to reading and discarding
                    int toRead = (int) Math.min(8192, pad - skipped);
                    byte[] tmp = new byte[toRead];
                    int r = in.read(tmp);
                    if (r < 0) throw new IOException("Unexpected EOF while skipping tar padding");
                    skipped += r;
                } else {
                    skipped += s;
                }
            }
        }

        /**
         * Returns true if every byte in the buffer is zero.
         *
         * @param buf buffer to inspect
         * @return true if all bytes are zero, false otherwise
         */
        private static boolean isAllZero(byte[] buf) {
            for (byte b : buf) if (b != 0) return false;
            return true;
        }

        /**
         * Reads a NUL-terminated or space-padded ASCII string from a header buffer region and trims whitespace.
         *
         * @param buf header buffer
         * @param off offset within buffer where the string starts
         * @param len maximum length of the field
         * @return the parsed string, trimmed
         */
        private static String readString(byte[] buf, int off, int len) {
            int end = off;
            int max = off + len;
            while (end < max && buf[end] != 0) end++;
            return new String(buf, off, end - off).trim();
        }

        /**
         * Extracts a string from a byte array stopping at the first NUL (0) byte. Does not trim the result.
         *
         * @param buf source bytes
         * @return string built from bytes up to first NUL
         */
        private static String extractStringFromBytes(byte[] buf) {
            int end = 0;
            while (end < buf.length && buf[end] != 0) end++;
            return new String(buf, 0, end);
        }

        /**
         * Parses an octal number stored as ASCII characters in the specified region of the buffer. Leading spaces
         * and NULs are skipped.
         *
         * @param buf source buffer containing ASCII octal digits
         * @param off offset where the octal field starts
         * @param len length of the octal field
         * @return parsed numeric value
         */
        @SuppressWarnings("SameParameterValue")
        private static long parseOctal(byte[] buf, int off, int len) {
            long result = 0L;
            int end = off + len;
            int i = off;
            // skip leading spaces or NULs
            while (i < end && (buf[i] == 0 || buf[i] == ' ')) i++;
            for (; i < end && buf[i] >= '0' && buf[i] <= '7'; i++) {
                result = (result << 3) + (buf[i] - '0');
            }
            return result;
        }
    }
}
