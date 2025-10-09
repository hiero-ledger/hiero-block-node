package org.hiero.block.tools.commands.days.listing;


import static org.hiero.block.tools.utils.RecordFileUtils.extractRecordFileTimeFromPath;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;

/**
 * A record file, with path, timestamp, size, and MD5 hash.
 *
 * @param path the path to the file in bucket relative to "records/"
 * @param timestamp the consenus timestamp of the file parsed from filename
 * @param sizeBytes the size of the file in bytes
 * @param md5Hex the MD5 hash of the file contents as hex string (16 bytes, 32 chars)
 */
public record ListingRecordFile(String path, LocalDateTime timestamp, int sizeBytes, String md5Hex) {
    public enum Type {
        RECORD,
        RECORD_SIG,
        RECORD_SIDECAR
    }

    /**
     * Get the path to the listing file for a specific day, creating any necessary directories.
     *
     * @param listingDir the base directory for listings
     * @param year the year
     * @param month the month (1-12)
     * @param day the day (1-31)
     * @return the path to the listing file for the specified day
     * @throws IOException if an I/O error occurs
     */
    public static Path getFileForDay(final Path listingDir,
            final int year, final int month, final int day) throws IOException {
        final Path monthDir = listingDir.resolve(String.format("%04d/%02d", year, month));
        Files.createDirectories(monthDir);
        return monthDir.resolve(String.format("%02d.bin", day));
    }

    public ListingRecordFile {
        if (md5Hex == null || md5Hex.length() != 32) {
            throw new IllegalArgumentException(
                    "md5Hex["+md5Hex+"] must be exactly 16 bytes, 32 chars hex string. length is " +
                            (md5Hex == null ? 0 : md5Hex.length()));
        }
    }

    public long timestampEpocMillis() {
        return timestamp.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
    }
    /** Equality purely by MD5 contents (128-bit). */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        return (o instanceof ListingRecordFile rf)
                && this.md5Hex.equals(rf.md5Hex);
    }

    /** Hash purely by MD5 contents (xor-folded 128->32). */
    @Override public int hashCode() {
        return md5Hex.hashCode();
    }

    public Type type() {
        if (path.contains("sidecar")) {
            return Type.RECORD_SIDECAR;
        } else if (path.endsWith(".rcd") || path.endsWith(".rcd.gz")) {
            return Type.RECORD;
        } else if (path.endsWith(".rcd_sig") || path.endsWith(".rcd_sig.gz")) {
            return Type.RECORD_SIG;
        } else {
            throw new IllegalArgumentException("Unknown file type for path: " + path);
        }
    }

    @Override
    public String toString() {
        return "RecordFile{" +
                "type=" + type() +
                ", path='" + path + '\'' +
                ", timestamp=" + timestamp +
                ", sizeBytes=" + sizeBytes +
                ", md5Hex=" + md5Hex +
                '}';
    }

    public void write(DataOutputStream dos) throws IOException {
        dos.writeUTF(path);
        dos.writeInt(sizeBytes);
        dos.writeUTF(md5Hex);
    }

    public static ListingRecordFile read(DataInputStream dis) throws IOException {
        String readPath = dis.readUTF();
        int readSizeBytes = dis.readInt();
        String readMd5Hex = dis.readUTF();
        return new ListingRecordFile(readPath, extractRecordFileTimeFromPath(readPath), readSizeBytes, readMd5Hex);
    }
}
