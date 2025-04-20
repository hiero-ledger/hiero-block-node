// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.archive;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;

/**
 * An iterator that converts blocks into tar format. It handles the creation of tar headers, content writing, and
 * padding. The neat bit is it is lazy and only creates the next tar block when asked for it, only getting block bytes
 * from BlockAccessor as needed. This allows a stream of blocks to be converted to tar format without needing to load
 * them all into ram. Each of the blocks is stored in a Zstd compressed protobuf format.
 */
public class TaredBlockIterator implements Iterator<byte[]> {
    /** The format for block numbers in file names */
    private static final NumberFormat BLOCK_NUMBER_FORMAT = new DecimalFormat("0000000000000000000");
    /** The chunk size for tar files = 8KB */
    private static final int CHUNK_SIZE = 8192;
    /** The format for blocks inside the tar file */
    private final Format format;
    /** The iterator over the blocks to be written into the tar file */
    private final Iterator<BlockAccessor> blockIterator;
    /** The extension for the block files in the tar file */
    private final String extension;
    /** The buffer for the current tar block */
    private final byte[] currentBuffer;
    /** The position in the current buffer */
    private int bufferPosition;
    /** The current state of the iterator */
    private State state = State.NEXT_FILE;
    /** The bytes of the current block */
    private byte[] currentBlockBytes;
    /** The name of the current block file */
    private String currentBlockName;
    /** The position in the current block file */
    private int filePosition;
    /** The number of padding bytes remaining to be written */
    private int paddingBytesRemaining;
    /** True if we have written the end of archive */
    private boolean endOfArchiveWritten = false;
    /** The iterate state enum */
    enum State {
        NEXT_FILE,
        WRITE_HEADER,
        WRITE_CONTENT,
        WRITE_PADDING,
        END_OF_ARCHIVE,
        DONE
    }

    /**
     * Creates a new TaredBlockIterator.
     *
     * @param format the format of the block data files written to the tar file
     * @param blockIterator the iterator over the blocks to be written into the tar file
     */
    public TaredBlockIterator(Format format, Iterator<BlockAccessor> blockIterator) {
        this.format = format;
        this.extension = switch (format) {
            case Format.JSON -> ".json";
            case Format.PROTOBUF -> ".blk";
            case Format.ZSTD_PROTOBUF -> ".blk.zstd";};
        this.blockIterator = blockIterator;
        this.currentBuffer = new byte[CHUNK_SIZE];
        this.bufferPosition = 0;
    }

    /**
     * Returns true if there is more data in the tar stream. This method will return false if the end of the tar stream
     * has been reached.
     *
     * @return true if there is more data in the tar stream, false otherwise
     */
    @Override
    public boolean hasNext() {
        return state != State.DONE;
    }

    /**
     * Returns the next chunk of data from the tar stream. This method will return a byte array of size up to
     * {@link #CHUNK_SIZE} bytes. If there is no more data in the tar stream, it will throw a
     * {@link NoSuchElementException}.
     *
     * @return the next chunk of data from the tar stream
     */
    @Override
    public byte[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more data in tar stream");
        }
        while (bufferPosition < CHUNK_SIZE) {
            switch (state) {
                case NEXT_FILE:
                    if (blockIterator.hasNext()) {
                        // get the next block from the iterator
                        final BlockAccessor currentBlock = blockIterator.next();
                        currentBlockBytes = currentBlock.blockBytes(format).toByteArray();
                        currentBlockName = BLOCK_NUMBER_FORMAT.format(currentBlock.blockNumber()) + extension;
                        filePosition = 0;
                        state = State.WRITE_HEADER;
                    } else {
                        state = State.END_OF_ARCHIVE;
                    }
                    break;

                case WRITE_HEADER:
                    byte[] header = createTarHeader(currentBlockName, currentBlockBytes.length);
                    int bytesToCopy = Math.min(header.length, CHUNK_SIZE - bufferPosition);
                    System.arraycopy(header, 0, currentBuffer, bufferPosition, bytesToCopy);
                    bufferPosition += bytesToCopy;
                    state = State.WRITE_CONTENT;
                    break;

                case WRITE_CONTENT:
                    int contentLeft = currentBlockBytes.length - filePosition;
                    int contentBytesToCopy = Math.min(contentLeft, CHUNK_SIZE - bufferPosition);

                    if (contentBytesToCopy > 0) {
                        System.arraycopy(
                                currentBlockBytes, filePosition, currentBuffer, bufferPosition, contentBytesToCopy);
                        filePosition += contentBytesToCopy;
                        bufferPosition += contentBytesToCopy;
                    }

                    if (filePosition >= currentBlockBytes.length) {
                        paddingBytesRemaining = (512 - (currentBlockBytes.length % 512)) % 512;
                        state = State.WRITE_PADDING;
                    }
                    break;

                case WRITE_PADDING:
                    int paddingBytesToCopy = Math.min(paddingBytesRemaining, CHUNK_SIZE - bufferPosition);
                    Arrays.fill(currentBuffer, bufferPosition, bufferPosition + paddingBytesToCopy, (byte) 0);
                    bufferPosition += paddingBytesToCopy;
                    paddingBytesRemaining -= paddingBytesToCopy;

                    if (paddingBytesRemaining <= 0) {
                        state = State.NEXT_FILE;
                    }
                    break;

                case END_OF_ARCHIVE:
                    if (!endOfArchiveWritten) {
                        int eofBytesToCopy = Math.min(1024, CHUNK_SIZE - bufferPosition);
                        Arrays.fill(currentBuffer, bufferPosition, bufferPosition + eofBytesToCopy, (byte) 0);
                        bufferPosition += eofBytesToCopy;

                        if (eofBytesToCopy >= 1024) {
                            endOfArchiveWritten = true;
                            state = State.DONE;
                        }
                    } else {
                        state = State.DONE;
                    }
                    break;

                case DONE:
                    return Arrays.copyOf(currentBuffer, bufferPosition);
            }

            if (bufferPosition >= CHUNK_SIZE) {
                byte[] result = Arrays.copyOf(currentBuffer, CHUNK_SIZE);
                bufferPosition = 0;
                return result;
            }
        }

        byte[] result = Arrays.copyOf(currentBuffer, bufferPosition);
        bufferPosition = 0;
        return result;
    }

    /**
     * Creates a tar header for the given file.
     *
     * @param fileName the name of the file
     * @param contentLength the length of the file content in bytes
     * @return the tar header as a byte array
     */
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

    /**
     * Writes an octal representation of a value into the buffer.
     *
     * @param buffer the buffer to write to
     * @param offset the offset in the buffer
     * @param length the length of the field
     * @param value the value to write
     */
    private static void writeOctalBytes(byte[] buffer, int offset, int length, long value) {
        String octalString = String.format("%0" + (length - 1) + "o\0", value);
        byte[] valueBytes = octalString.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(valueBytes, 0, buffer, offset, Math.min(valueBytes.length, length));
    }
}
