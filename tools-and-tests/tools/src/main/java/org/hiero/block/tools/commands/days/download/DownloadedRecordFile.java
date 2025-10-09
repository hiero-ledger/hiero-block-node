package org.hiero.block.tools.commands.days.download;


import static org.hiero.block.tools.records.RecordFileUtils.extractRecordFileTimeFromPath;

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
 * @param contents the file contents as byte array
 */
public record DownloadedRecordFile(String path, LocalDateTime timestamp, int sizeBytes, String md5Hex, byte[] contents) {
    public enum Type {
        RECORD,
        RECORD_SIG,
        RECORD_SIDECAR
    }

    public DownloadedRecordFile {
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
        return (o instanceof DownloadedRecordFile rf)
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

    public static DownloadedRecordFile read(DataInputStream dis) throws IOException {
        String readPath = dis.readUTF();
        int readSizeBytes = dis.readInt();
        String readMd5Hex = dis.readUTF();
        return new DownloadedRecordFile(readPath, extractRecordFileTimeFromPath(readPath), readSizeBytes, readMd5Hex);
    }
}
