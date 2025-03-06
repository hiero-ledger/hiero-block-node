// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.write;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.BlocksPersisted;
import static java.lang.System.Logger.Level.DEBUG;

import com.hedera.block.server.ack.AckHandler;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.persistence.storage.write.BlockPersistenceResult.BlockPersistenceStatus;
import com.hedera.hapi.block.BlockItemUnparsed;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

/**
 * An async block writer that does nothing with the received block items.
 */
final class AsyncNoOpWriter implements AsyncBlockWriter {
    private static final System.Logger LOGGER = System.getLogger(AsyncNoOpWriter.class.getName());
    private final long blockNumber;
    private final AckHandler ackHandler;
    private final MetricsService metricsService;
    private final TransferQueue<BlockItemUnparsed> queue;

    AsyncNoOpWriter(
            final long blockNumber,
            @NonNull final AckHandler ackHandler,
            @NonNull final MetricsService metricsService) {
        this.blockNumber = blockNumber;
        this.ackHandler = Objects.requireNonNull(ackHandler);
        this.metricsService = Objects.requireNonNull(metricsService);
        this.queue = new LinkedTransferQueue<>();
    }

    @NonNull
    @Override
    public TransferQueue<BlockItemUnparsed> getQueue() {
        return queue;
    }

    /**
     * No Op implementation of the {@link AsyncBlockWriter} interface.
     * It will be accepting items through the queue until a full block is
     * received and will return a successful result, but will do no actual
     * persistence. Also, it will behave as the API expects, and will return
     * an incomplete block status if the queue is offered the incomplete block
     * flag.
     */
    @Override
    public Void call() {
        final BlockPersistenceResult result = doPersistBlock();
        LOGGER.log(DEBUG, "Persistence task completed, publishing Persistence Result: %s".formatted(result));
        ackHandler.blockPersisted(result);
        if (result.status().equals(BlockPersistenceStatus.SUCCESS)) {
            metricsService.get(BlocksPersisted).increment();
        }
        return null;
    }

    private BlockPersistenceResult doPersistBlock() {
        boolean blockComplete = false;
        while (!blockComplete) { // loop until received all items (until block proof arrives)
            try {
                final BlockItemUnparsed nextItem = queue.take();
                if (nextItem == AsyncBlockWriter.INCOMPLETE_BLOCK_FLAG) {
                    return new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.INCOMPLETE_BLOCK);
                } else {
                    if (nextItem.hasBlockProof()) {
                        blockComplete = true;
                        LOGGER.log(DEBUG, "Received Block Proof for Block [%d]".formatted(blockNumber));
                    }
                }
            } catch (final InterruptedException e) {
                // @todo(713) is this the proper handling here?
                LOGGER.log(
                        Level.ERROR,
                        "Interrupted while waiting for next block item for block [%d]".formatted(blockNumber));
                Thread.currentThread().interrupt();
                return new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.PERSISTENCE_INTERRUPTED);
            }
        }
        return new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.SUCCESS);
    }
}
