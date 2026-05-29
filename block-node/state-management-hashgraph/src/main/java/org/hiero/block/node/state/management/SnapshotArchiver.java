// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Tars a snapshot directory tree to a {@code &lt;historicPath&gt;/&lt;blockNumber&gt;.tar}.
 *
 * <p>The UStar format here is intentionally hand-rolled (no commons-compress dep)
 * and mirrors the encoding used by the block-base {@code TaredBlockIterator} —
 * the same 512-byte header layout, octal numeric fields, ustar magic, and
 * 1024-byte end-of-archive marker. The difference: that iterator emits one tar
 * entry per BlockAccessor; this archiver walks an arbitrary directory tree (the
 * consensus-node {@code data/state/} layout produced by
 * {@code VirtualMapStateLifecycleManager.createSnapshot}) recursively.
 *
 * <p>Writes go through {@code &lt;historicPath&gt;/&lt;blockNumber&gt;.tar.tmp} and
 * atomic move so a crash mid-write leaves the previous archive intact.
 *
 * <p>TODO(STORY-18): the UStar header encoding here duplicates the block-base
 * {@code TaredBlockIterator}. Consolidate the two onto a shared tar-writer
 * utility so the 512-byte header layout lives in exactly one place.
 */
final class SnapshotArchiver {

    private static final int BLOCK_SIZE = 512;
    private static final int END_OF_ARCHIVE_BYTES = 1024;

    private SnapshotArchiver() {}

    /**
     * Archive the contents of {@code sourceSnapshotDir} into
     * {@code historicRoot/<blockNumber>.tar}. Entries inside the tar are rooted at
     * {@code <blockNumber>/} so extracting the archive yields a directory with the
     * block number as its name.
     *
     * @return the path of the created tar file.
     */
    @NonNull
    static Path archive(
            @NonNull final Path sourceSnapshotDir, @NonNull final Path historicRoot, final long blockNumber)
            throws IOException {
        Files.createDirectories(historicRoot);
        final Path target = historicRoot.resolve(blockNumber + ".tar");
        final Path tmp = historicRoot.resolve(blockNumber + ".tar.tmp");
        // Clean up any stale temp left from a previous crash.
        Files.deleteIfExists(tmp);

        try (OutputStream out = Files.newOutputStream(tmp)) {
            final String prefix = blockNumber + "/";
            writeDirEntry(out, prefix);

            final List<Path> entries;
            try (Stream<Path> walk = Files.walk(sourceSnapshotDir)) {
                entries = walk.sorted(Comparator.comparing(Path::toString)).toList();
            }
            for (final Path entry : entries) {
                if (entry.equals(sourceSnapshotDir)) {
                    continue;
                }
                final String rel = prefix
                        + sourceSnapshotDir.relativize(entry).toString().replace(File.separatorChar, '/');
                if (Files.isDirectory(entry)) {
                    writeDirEntry(out, rel + "/");
                } else {
                    writeFileEntry(out, rel, entry);
                }
            }

            // End-of-archive marker: two 512-byte zero blocks.
            out.write(new byte[END_OF_ARCHIVE_BYTES]);
        }

        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (final IOException atomicFailed) {
            // Some filesystems can't do atomic move across volumes — fall back to a plain replace.
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return target;
    }

    private static void writeDirEntry(@NonNull final OutputStream out, @NonNull final String name) throws IOException {
        out.write(createTarHeader(name, 0L, true));
    }

    private static void writeFileEntry(
            @NonNull final OutputStream out, @NonNull final String name, @NonNull final Path file) throws IOException {
        final long size = Files.size(file);
        out.write(createTarHeader(name, size, false));
        try (var in = Files.newInputStream(file)) {
            in.transferTo(out);
        }
        final int padding = (int) ((BLOCK_SIZE - (size % BLOCK_SIZE)) % BLOCK_SIZE);
        if (padding > 0) {
            out.write(new byte[padding]);
        }
    }

    private static byte[] createTarHeader(@NonNull final String fileName, final long contentLength, final boolean dir) {
        final byte[] header = new byte[BLOCK_SIZE];

        // File name (100 bytes). Names longer than 100 bytes are not supported here — the
        // consensus-node state snapshot layout fits inside that envelope.
        final byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(nameBytes, 0, header, 0, Math.min(nameBytes.length, 100));

        // Mode (8 bytes octal). 0755 for dirs, 0644 for files.
        writeOctalBytes(header, 100, 8, dir ? 0755 : 0644);
        // Uid + gid (8 bytes each).
        writeOctalBytes(header, 108, 8, 1000);
        writeOctalBytes(header, 116, 8, 1000);
        // Size (12 bytes octal).
        writeOctalBytes(header, 124, 12, contentLength);
        // Mtime (12 bytes octal).
        writeOctalBytes(header, 136, 12, System.currentTimeMillis() / 1000L);

        // Type flag — '5' for directory, '0' for normal file.
        header[156] = (byte) (dir ? '5' : '0');

        // UStar indicator + version.
        System.arraycopy("ustar\0".getBytes(StandardCharsets.UTF_8), 0, header, 257, 6);
        System.arraycopy("00".getBytes(StandardCharsets.UTF_8), 0, header, 263, 2);

        // Checksum field is spaces while computing.
        Arrays.fill(header, 148, 156, (byte) ' ');
        int checksum = 0;
        for (final byte b : header) {
            checksum += b & 0xFF;
        }
        final String checksumStr = String.format("%06o\0 ", checksum);
        System.arraycopy(checksumStr.getBytes(StandardCharsets.UTF_8), 0, header, 148, 8);

        return header;
    }

    private static void writeOctalBytes(
            @NonNull final byte[] buffer, final int offset, final int length, final long value) {
        final String octal = String.format("%0" + (length - 1) + "o\0", value);
        final byte[] bytes = octal.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(bytes, 0, buffer, offset, Math.min(bytes.length, length));
    }
}
