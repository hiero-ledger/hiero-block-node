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
    private static final int BLOCK_SIZE = 512;

    /**
     * Reads the contents of a tar archive from the given InputStream and returns a stream of InMemoryFile objects. Uses
     * a streaming approach with Spliterator to handle large archives efficiently.
     *
     * The returned Stream is auto-closeable and will close the provided InputStream when the stream is closed.
     *
     * Only regular files are returned; directory entries and other non-regular entries are skipped.
     *
     * @param input the InputStream of the tar archive
     * @return a Stream of InMemoryFile objects representing the files in the tar archive
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

    // Internal spliterator that reads tar entries lazily from the InputStream
    private static final class TarSpliterator implements Spliterator<InMemoryFile> {
        private final InputStream in;
        private boolean finished = false;
        private String pendingLongName = null;

        TarSpliterator(InputStream in) {
            this.in = in;
        }

        @Override
        public boolean tryAdvance(java.util.function.Consumer<? super InMemoryFile> action) {
            Objects.requireNonNull(action);
            if (finished) return false;
            try {
                while (true) {
                    byte[] header = new byte[BLOCK_SIZE];
                    int hread = readFully(header);
                    if (hread == 0) {
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
                        String longName = extractStringFromBytes(nameBytes);
                        pendingLongName = longName;
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

        @Override
        public Spliterator<InMemoryFile> trySplit() { return null; }

        @Override
        public long estimateSize() { return Long.MAX_VALUE; }

        @Override
        public int characteristics() { return 0; }

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

        private static boolean isAllZero(byte[] buf) {
            for (byte b : buf) if (b != 0) return false;
            return true;
        }

        private static String readString(byte[] buf, int off, int len) {
            int end = off;
            int max = off + len;
            while (end < max && buf[end] != 0) end++;
            return new String(buf, off, end - off).trim();
        }

        private static String extractStringFromBytes(byte[] buf) {
            int end = 0;
            while (end < buf.length && buf[end] != 0) end++;
            return new String(buf, 0, end);
        }

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
