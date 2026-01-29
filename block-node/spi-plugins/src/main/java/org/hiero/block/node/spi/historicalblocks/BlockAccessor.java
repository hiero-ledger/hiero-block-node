// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.block.internal.BlockUnparsed;

/**
 * The BlockAccessor interface is used to provide access to a block.
 * A BlockAccessor is issued for a block that is available to be accessed.
 * A {@code null} value is expected when an attempt to access the data is made,
 * but that attempt is unsuccessful.
 */
public interface BlockAccessor extends AutoCloseable {
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
     * This method closes the block accessor.
     * After being closed, the data may not be accessed. All accessors must be
     * closed as doing so frees resources. This method must not throw any
     * exceptions, they should be handled appropriately in the implementing
     * class.
     */
    @Override
    void close();

    /**
     * This method tests whether this block accessor is already closed.
     * <p>
     * This method is used to ensure we do not close an accessor twice. There
     * are times when a block accessor _might_ be held by an
     * {@link AutoCloseable} container, but also might not. For those cases we
     * need to test (in the container, typically) if the accessor is closed
     * before calling {@link BlockAccessor#close()} a subsequent time.
     * @return true if, and only if this BlockAccessor instance is closed.
     */
    boolean isClosed();
}
