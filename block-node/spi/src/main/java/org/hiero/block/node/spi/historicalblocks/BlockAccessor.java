// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.UncheckedIOException;
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
}
