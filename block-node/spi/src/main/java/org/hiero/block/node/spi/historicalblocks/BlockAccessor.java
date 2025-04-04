// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import com.github.luben.zstd.Zstd;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.WritableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hiero.hapi.block.node.BlockUnparsed;

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
     * The unmodifiable list of all available formats.
     */
    List<Format> ALL_FORMATS = List.of(Format.values());

    /**
     * Get the list of available formats from this BlockAccessor. This allows the producer to tell the consumer what
     * formats it has available in an efficient manner.
     *
     * @return  list of one or more of the available formats in the Format enum.
     */
    default List<Format> availableFormats() {
        return ALL_FORMATS;
    }

    /**
     * Delete the block. This is optional and may not be supported by all BlockAccessor implementations. The default
     * implementation throws an UnsupportedOperationException.
     *
     * @throws UnsupportedOperationException if delete is not supported
     */
    default void delete() {
        throw new UnsupportedOperationException("Delete not supported");
    }

    /**
     * Get the block as parsed {@code Block} Java object. This is the simplest but usually the least efficient way to
     * get the block.
     *
     * @return the block as a {@code Block} Java object
     */
    Block block();

    /**
     * Get the block as unparsed {@code BlockUnparsed} Java object.
     *
     * @return the block as a {@code BlockUnparsed} Java object
     */
    default BlockUnparsed blockUnparsed() {
        try {
            return BlockUnparsed.PROTOBUF.parse(blockBytes(Format.PROTOBUF));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the block as Bytes in the specified encoded format. This allows the consumer to choose the most efficient
     * format for them.
     * <p>
     * The default implementation uses the {@link #block()} method to get the block and then encodes it in the specified
     * format.
     * </p>
     *
     * @param format the format to get the block in, has to be one of the available formats returned by {@link #availableFormats()}
     * @return the block as a Bytes object in the specified format
     * @throws IllegalArgumentException if the format is not one of the available formats
     */
    default Bytes blockBytes(Format format) throws IllegalArgumentException {
        if (format == null || !availableFormats().contains(format)) {
            throw new IllegalArgumentException("Format " + format + " not supported");
        }
        return switch (format) {
            case JSON -> Block.JSON.toBytes(block());
            case PROTOBUF -> Block.PROTOBUF.toBytes(block());
            case ZSTD_PROTOBUF -> {
                final Bytes protobufBytes = Block.PROTOBUF.toBytes(block());
                final byte[] compressedBytes = Zstd.compress(protobufBytes.toByteArray());
                yield Bytes.wrap(compressedBytes);
            }
        };
    }

    /**
     * Write the block in the specified format to the given output stream. This allows the consumer to choose the most
     * efficient format for them.
     * <p>
     * The default implementation uses the {@link #blockBytes(Format)} method to get the block and then writes it encoded to
     * the output stream.
     * </p>
     *
     * @param format the format to write the block in, has to be one of the available formats returned by {@link #availableFormats()}
     * @param output the output stream to write the block to
     * @throws IllegalArgumentException if the format is not one of the available formats
     */
    default void writeBytesTo(Format format, WritableSequentialData output) throws IllegalArgumentException {
        blockBytes(format).writeTo(output);
    }

    /**
     * Write the block in the specified format to the given output stream. This allows the consumer to choose the most
     * efficient format for them.
     * <p>
     * The default implementation uses the {@link #blockBytes(Format)} method to get the block and then writes it encoded to
     * the output stream.
     * </p>
     *
     * @param format the format to write the block in, has to be one of the available formats returned by {@link #availableFormats()}
     * @param output the output stream to write the block to
     * @throws IllegalArgumentException if the format is not one of the available formats
     */
    default void writeBytesTo(Format format, OutputStream output) throws IllegalArgumentException {
        blockBytes(format).writeTo(output);
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
     * @param format the format to write the block in, has to be one of the available formats returned by {@link #availableFormats()}
     * @param path to the file to write the block to
     * @throws IOException if there was an error writing the block to given path
     */
    default void writeTo(Format format, Path path) throws IOException {
        try (final WritableStreamingData output = new WritableStreamingData(Files.newOutputStream(path))) {
            writeBytesTo(format, output);
        }
    }
}
