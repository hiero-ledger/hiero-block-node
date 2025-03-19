// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.write;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockItemUnparsed.ItemOneOfType;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.OneOf;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.Callable;
import java.util.concurrent.TransferQueue;

/**
 * An async block writer that handles writing of blocks in an asynchronous
 * fashion. All writers are also {@link Callable}s that return a result of type
 * {@link BlockPersistenceResult}. All writers must also have a queue that is
 * used to pass in {@link BlockItemUnparsed}s that are to be written to storage.
 * A writer must not propagate exceptions, but rather return a meaningful
 * {@link BlockPersistenceResult}. If an exception is thrown as a result of the
 * callable, it should be considered a bug. If persistence fails, there must
 * always be a reason for it. Other components need to decide what to do with
 * the result of the persistence operation.
 */
public interface AsyncBlockWriter extends Callable<Void> {
    /**
     * The INCOMPLETE_BLOCK_FLAG is used to be sent as a signal by being offered
     * to the queue that is returned from {@link #getQueue()}.
     * All {@link AsyncBlockWriter}s must return a status of
     * {@link BlockPersistenceResult.BlockPersistenceStatus#INCOMPLETE_BLOCK}
     * when their queues are being offered this exact flag, checked by
     * reference, and also they must roll back any side effects produced by
     * themselves before being flagged with the incomplete block flag and need
     * to terminate immediately.
     */
    BlockItemUnparsed INCOMPLETE_BLOCK_FLAG = new BlockItemUnparsed(new OneOf<>(
            ItemOneOfType.BLOCK_HEADER,
            BlockHeader.PROTOBUF.toBytes(new BlockHeader(null, null, -1, null, null, null))));

    /**
     * Returns the queue that is used to pass in {@link BlockItemUnparsed}s that
     * are to be written to storage. This queue can be "pinged" that no more
     * items are to be expected either by offering the incomplete block flag or
     * by offering the block proof (we always expect that to be the case).
     *
     * @return the queue that is used to pass in {@link BlockItemUnparsed}s that
     * are to be written to storage
     */
    @NonNull
    TransferQueue<BlockItemUnparsed> getQueue();
}
