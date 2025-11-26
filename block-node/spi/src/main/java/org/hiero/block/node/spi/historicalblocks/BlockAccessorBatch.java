// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.Closeable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A simple, read-only view of a batch of {@link BlockAccessor} instances.
 * <p>
 * This class is both an {@link Iterable} and a {@link Closeable}.<br/>
 * Accessors are returned from iterators in the order they were added to the batch.<br/>
 * Closing the batch closes all the accessors.
 * <p>
 * It is possible, but <em>not recommended</em> to reuse an instance of this
 * class after {@link #close()} is called.
 */
public final class BlockAccessorBatch implements Iterable<BlockAccessor>, Closeable {
    private final List<BlockAccessor> blockAccessors;

    public BlockAccessorBatch() {
        this.blockAccessors = new LinkedList<>();
    }

    public final void add(@NonNull final BlockAccessor itemToAdd) {
        blockAccessors.add(itemToAdd);
    }

    public final long getFirstBlockNumber() {
        return blockAccessors.getFirst().blockNumber();
    }

    public final BlockAccessor getFirst() {
        return blockAccessors.getFirst();
    }

    public final boolean isEmpty() {
        return blockAccessors.isEmpty();
    }

    @Override
    @NonNull
    public Iterator<BlockAccessor> iterator() {
        return blockAccessors.iterator();
    }

    @Override
    public void close() {
        for (final BlockAccessor accessor : blockAccessors) {
            if (!accessor.isClosed()) {
                accessor.close();
            }
        }
        blockAccessors.clear();
    }
}
