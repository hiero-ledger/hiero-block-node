// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.CompressionType;

/// Utilities for reading and writing tar entries that store serialised Hiero blocks.
///
/// **Writing**: [toTarEntry] serialises a [BlockUnparsed] into a complete UStar tar entry
/// (512-byte header + compressed content + 512-byte-aligned padding) suitable for direct
/// concatenation into a tar file.
///
/// **Reading**: [findLastBlockStart] scans raw tar bytes (e.g. a downloaded S3 multipart-upload
/// part) to locate the last valid tar header — used by [StartupRecoveryTask] when trimming a
/// partially-written part.
final class TarEntries {

    /// The extension appended to every block entry's filename inside the tar.
    private static final String EXTENSION = ".blk.zstd";

    /// Width of the zero-padded decimal block-number prefix in each tar entry filename.
    /// Entries are named e.g. 0000000000000000123.tar, so we need the first 19 bytes to determine
    /// the block number.
    static final int BLOCK_NUMBER_WIDTH = 19;

    /// Tar block size in bytes.
    private static final int TAR_BLOCK = 512;
    /// Byte offset of the checksum field in a tar header (8 bytes).
    private static final int HDR_CHECKSUM_OFFSET = 148;
    /// Byte offset of the UStar magic string in a tar header.
    private static final int HDR_USTAR_OFFSET = 257;
    /// Expected UStar magic bytes.
    private static final byte[] USTAR_MAGIC = "ustar".getBytes(StandardCharsets.UTF_8);

    private TarEntries() {}

    /// Converts the given block into a tar entry byte array consisting of the 512-byte tar header,
    /// the serialized block content, and zero-padding to the next 512-byte boundary.
    ///
    /// @param block the block to convert
    /// @param blockNumber the block number, used to derive the file name inside the tar
    /// @return a byte array representing the complete tar entry for the block
    static byte[] toTarEntry(@NonNull final BlockUnparsed block, final long blockNumber) throws IOException {
        Objects.requireNonNull(block);

        final String currentBlockName = String.format("%0" + BLOCK_NUMBER_WIDTH + "d", blockNumber) + EXTENSION;
        final byte[] rawBytes = BlockUnparsed.PROTOBUF.toBytes(block).toByteArray();
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(rawBytes.length)) {
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
    }

    /// Scans `bytes` for the last valid UStar tar header and returns its byte offset.
    ///
    /// Delegates to [#findLastBlockStart(byte[], long)] with `streamOffset` = 0.
    ///
    /// @return the byte offset of the last valid header, or `-1` if no valid header is found
    static int findLastBlockStart(@NonNull byte[] bytes) {
        return findLastBlockStart(bytes, 0);
    }

    /// Scans `bytes` for the last valid UStar tar header and returns its byte offset.
    ///
    /// Because every tar entry is [#TAR_BLOCK]-aligned in the overall stream, the first header in
    /// `bytes` is at `(TAR_BLOCK - streamOffset % TAR_BLOCK) % TAR_BLOCK`.  The scan advances in
    /// [#TAR_BLOCK]-sized steps from that computed start, recording each valid header position.
    /// The entry's content is not required to fit within `bytes` — a header is considered a block
    /// start even if it is truncated.
    ///
    /// @param streamOffset the byte position of `bytes[0]` in the overall tar stream; used to
    ///                     determine the alignment of the first tar-block boundary inside `bytes`
    /// @return the byte offset of the last valid header, or `-1` if no valid header is found
    static int findLastBlockStart(@NonNull byte[] bytes, long streamOffset) {
        final int startOffset = (int) ((TAR_BLOCK - (streamOffset % TAR_BLOCK)) % TAR_BLOCK);
        int last = -1;
        int offset = startOffset;
        while (offset + TAR_BLOCK <= bytes.length) {
            if (!isAllZero(bytes, offset, TAR_BLOCK) && isValidUstarHeader(bytes, offset)) {
                last = offset;
            }
            offset += TAR_BLOCK;
        }
        return last;
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

    /// Returns `true` if the `length`-byte region of `bytes` starting at `offset` contains only zeros.
    private static boolean isAllZero(byte[] bytes, int offset, int length) {
        for (int i = offset; i < offset + length; i++) {
            if (bytes[i] != 0) {
                return false;
            }
        }
        return true;
    }

    /// Returns `true` if the 512-byte region of `bytes` at `offset` is a valid UStar tar header:
    /// the magic bytes match and the checksum is correct.
    private static boolean isValidUstarHeader(byte[] bytes, int offset) {
        // Check UStar magic ("ustar") at the standard position.
        for (int i = 0; i < USTAR_MAGIC.length; i++) {
            if (bytes[offset + HDR_USTAR_OFFSET + i] != USTAR_MAGIC[i]) {
                return false;
            }
        }
        // Recompute the checksum: treat the 8 checksum bytes as spaces (ASCII 32), sum all 512 bytes.
        int sum = 0;
        for (int i = 0; i < TAR_BLOCK; i++) {
            final int b = bytes[offset + i] & 0xFF;
            if (i >= HDR_CHECKSUM_OFFSET && i < HDR_CHECKSUM_OFFSET + 8) {
                sum += 32; // treat checksum field as spaces during calculation
            } else {
                sum += b;
            }
        }
        // The stored checksum is an octal string in bytes 148-155.
        final long storedChecksum = parseOctal(bytes, offset + HDR_CHECKSUM_OFFSET, 8);
        return storedChecksum == sum;
    }

    /// Parses a null- or space-terminated octal ASCII string from `bytes` at `offset` with the
    /// given `length`, returning the numeric value, or `-1` on parse failure.
    private static long parseOctal(byte[] bytes, int offset, int length) {
        int end = offset + length;
        // Trim trailing nulls and spaces.
        while (end > offset && (bytes[end - 1] == 0 || bytes[end - 1] == ' ')) {
            end--;
        }
        if (end == offset) {
            return 0;
        }
        try {
            return Long.parseLong(new String(bytes, offset, end - offset, StandardCharsets.UTF_8), 8);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
