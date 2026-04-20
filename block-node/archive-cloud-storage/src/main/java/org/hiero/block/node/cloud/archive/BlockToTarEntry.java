// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.CompressionType;

/// Converts a [BlockUnparsed] object into a byte array representing a tar entry (header +
/// content + padding) that can be directly appended to a tar file.
final class BlockToTarEntry {

    /// The extension for block files in the tar file
    private static final String EXTENSION = ".blk.zstd";

    private BlockToTarEntry() {}

    /// Converts the given block into a tar entry byte array consisting of the 512-byte tar header,
    /// the serialized block content, and zero-padding to the next 512-byte boundary.
    ///
    /// @param block the block to convert
    /// @param blockNumber the block number, used to derive the file name inside the tar
    /// @return a byte array representing the complete tar entry for the block
    static byte[] toTarEntry(@NonNull final BlockUnparsed block, final long blockNumber) throws IOException {
        Objects.requireNonNull(block);

        final String currentBlockName = String.format("%019d", blockNumber) + EXTENSION;
        final byte[] rawBytes = BlockUnparsed.PROTOBUF.toBytes(block).toByteArray();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(rawBytes.length);
        try (final OutputStream zstdOut = CompressionType.ZSTD.wrapStream(baos)) {
            zstdOut.write(rawBytes);
        }
        final byte[] currentBlockBytes = baos.toByteArray();

        final byte[] header = createTarHeader(currentBlockName, currentBlockBytes.length);
        final int paddingBytesRemaining = (512 - (currentBlockBytes.length % 512)) % 512;
        final byte[] entry = new byte[512 + currentBlockBytes.length + paddingBytesRemaining];

        System.arraycopy(header, 0, entry, 0, 512);
        System.arraycopy(currentBlockBytes, 0, entry, 512, currentBlockBytes.length);
        // padding bytes are already zero-initialized

        return entry;
    }

    /// Creates a tar header for the given file.
    ///
    /// @param fileName the name of the file
    /// @param contentLength the length of the file content in bytes
    /// @return the tar header as a byte array
    private static byte[] createTarHeader(String fileName, long contentLength) {
        byte[] header = new byte[512]; // Initialize with zeros

        // File name (100 bytes)
        byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(nameBytes, 0, header, 0, Math.min(nameBytes.length, 100));

        // File mode (8 bytes) - 644 in octal
        writeOctalBytes(header, 100, 8, 0644);

        // Owner/Group ID (8 bytes each)
        writeOctalBytes(header, 108, 8, 1000);
        writeOctalBytes(header, 116, 8, 1000);

        // File size (12 bytes)
        writeOctalBytes(header, 124, 12, contentLength);

        // Last modification time (12 bytes)
        writeOctalBytes(header, 136, 12, System.currentTimeMillis() / 1000);

        // Type flag (1 byte) - '0' for normal file
        header[156] = '0';

        // UStar indicator and version
        System.arraycopy("ustar\0".getBytes(StandardCharsets.UTF_8), 0, header, 257, 6);
        System.arraycopy("00".getBytes(StandardCharsets.UTF_8), 0, header, 263, 2);

        // Calculate checksum
        Arrays.fill(header, 148, 156, (byte) ' ');
        int checksum = 0;
        for (byte b : header) {
            checksum += b & 0xFF;
        }

        // Write checksum
        String checksumStr = String.format("%06o\0 ", checksum);
        System.arraycopy(checksumStr.getBytes(StandardCharsets.UTF_8), 0, header, 148, 8);

        return header;
    }

    /// Writes an octal representation of a value into the buffer.
    ///
    /// @param buffer the buffer to write to
    /// @param offset the offset in the buffer
    /// @param length the length of the field
    /// @param value the value to write
    private static void writeOctalBytes(byte[] buffer, int offset, int length, long value) {
        String octalString = String.format("%0" + (length - 1) + "o\0", value);
        byte[] valueBytes = octalString.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(valueBytes, 0, buffer, offset, Math.min(valueBytes.length, length));
    }
}
