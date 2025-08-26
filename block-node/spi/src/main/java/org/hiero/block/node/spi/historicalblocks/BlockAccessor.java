// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.WritableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.internal.BlockUnparsed;

/**
 * The BlockAccessor interface is used to provide access to a block and its serialized forms. It allows for getting
 * access to the block in many forms to allow the consumer to choose the most efficient form. The aim is for the
 * producer(BlockAccessor provider) and consumer(BlockAccessor user) to be able to work together to move the block data
 * from one to the other in the most efficient way possible.
 */
@SuppressWarnings("unused")
public interface BlockAccessor {
    /**
     * The format of the block data. The consumer can choose the format that is most efficient for them.
     */
    enum Format {
        JSON,
        PROTOBUF,
        ZSTD_PROTOBUF
    }

    /**
     * Get the block number of the block.
     *
     * @return the block number
     */
    long blockNumber();

    /**
     * Get the block as parsed {@code Block} Java object. This is the simplest but usually the least efficient way to
     * get the block.
     *
     * @return the block as a {@code Block} Java object, or null if parsing failed.
     *     Also returns null if the data cannot be read from a source.
     */
    default Block block() {
        try {
            final Bytes rawData = blockBytes(Format.PROTOBUF);
            return rawData == null ? null : Block.PROTOBUF.parse(rawData);
        } catch (final UncheckedIOException | ParseException e) {
            final System.Logger LOGGER = System.getLogger(getClass().getName());
            LOGGER.log(WARNING, "Failed to parse block", e);
            return null;
        }
    }

    /**
     * Get the block as unparsed {@code BlockUnparsed} Java object.
     *
     * @return the block as a {@code BlockUnparsed} Java object, or null if parsing failed.
     *     Also returns null if the data cannot be read from a source.
     */
    default BlockUnparsed blockUnparsed() {
        try {
            final Bytes rawData = blockBytes(Format.PROTOBUF);
            return rawData == null ? null : BlockUnparsed.PROTOBUF.parse(rawData);
        } catch (final ParseException e) {
            final System.Logger LOGGER = System.getLogger(getClass().getName());
            LOGGER.log(WARNING, "Failed to parse block", e);
            return null;
        }
    }

    /**
     * Get the block as Bytes in the specified encoded format. This allows the consumer to choose the most efficient
     * format for them.
     *
     * @param format the desired format of the serialized bytes
     * @return the block as a Bytes object in the specified format, or null if the block cannot be read.
     *     Also returns null if the data cannot be read from a source.
     */
    Bytes blockBytes(Format format);

    /**
     * Write the block in the specified format to the given output stream. This allows the consumer to choose the most
     * efficient format for them.
     * <p>
     * The default implementation uses the {@link #blockBytes(Format)} method to get the block and then writes it encoded to
     * the output stream.
     * </p>
     *
     * @param format the desired format to use to serialize the bytes
     * @param output the output stream to write the block to
     * @throws IOException if there was an error writing the block to given path
     */
    default void writeBytesTo(Format format, WritableSequentialData output) throws IOException {
        final Bytes bytes = blockBytes(format);
        if (bytes != null) {
            bytes.writeTo(output);
        } else {
            final System.Logger LOGGER = System.getLogger(getClass().getName());
            LOGGER.log(WARNING, "Failed to get bytes in writeBytesTo, no data written");
        }
    }

    /**
     * Write the block in the specified format to the given output stream. This allows the consumer to choose the most
     * efficient format for them.
     * <p>
     * The default implementation uses the {@link #blockBytes(Format)} method to get the block and then writes it encoded to
     * the output stream.
     * </p>
     *
     * @param format the desired format to use to serialize the bytes
     * @param output the output stream to write the block to
     * @throws IOException if there was an error writing the block to given path
     */
    default void writeBytesTo(Format format, OutputStream output) throws IOException {
        final Bytes bytes = blockBytes(format);
        if (bytes != null) {
            bytes.writeTo(output);
        } else {
            final System.Logger LOGGER = System.getLogger(getClass().getName());
            LOGGER.log(WARNING, "Failed to get bytes in writeBytesTo, no data written");
        }
    }

    /**
     * Write the block in the specified format to a file at the given path. This allows the consumer to choose the most
     * efficient format for them. The provider can decide that if it already has the block in this format then it can
     * just ask the OS to do a file copy with no need to read the data into the JVM at all.
     * <p>
     * The default implementation uses the {@link #blockBytes(Format)} method to get the block and then writes it
     * encoded to the file.
     * </p>
     *
     * @param format the desired format to use to serialize the bytes
     * @param path to the file to write the block to
     * @throws IOException if there was an error writing the block to given path
     */
    default void writeTo(Format format, Path path) throws IOException {
        try (final WritableStreamingData output = new WritableStreamingData(Files.newOutputStream(path))) {
            writeBytesTo(format, output);
        }
    }
}
