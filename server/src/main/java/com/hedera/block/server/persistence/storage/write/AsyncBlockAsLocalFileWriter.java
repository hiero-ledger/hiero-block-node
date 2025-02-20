// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.write;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.BlockPersistenceError;
import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.BlocksPersisted;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

import com.hedera.block.common.utils.FileUtilities;
import com.hedera.block.common.utils.Preconditions;
import com.hedera.block.server.ack.AckHandler;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.persistence.storage.compression.Compression;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import com.hedera.block.server.persistence.storage.remove.BlockRemover;
import com.hedera.block.server.persistence.storage.write.BlockPersistenceResult.BlockPersistenceStatus;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

/**
 * An async block writer that handles writing of blocks as a file to local
 * storage.
 */
final class AsyncBlockAsLocalFileWriter implements AsyncBlockWriter {
    private static final System.Logger LOGGER = System.getLogger(AsyncBlockAsLocalFileWriter.class.getName());
    private final BlockPathResolver blockPathResolver;
    private final BlockRemover blockRemover;
    private final Compression compression;
    private final LinkedTransferQueue<BlockItemUnparsed> queue;
    private final long blockNumber;
    private final AckHandler ackHandler;
    private final MetricsService metricsService;

    AsyncBlockAsLocalFileWriter(
            final long blockNumber,
            @NonNull final BlockPathResolver blockPathResolver,
            @NonNull final BlockRemover blockRemover,
            @NonNull final Compression compression,
            @NonNull final AckHandler ackHandler,
            @NonNull final MetricsService metricsService) {
        this.blockPathResolver = Objects.requireNonNull(blockPathResolver);
        this.blockRemover = Objects.requireNonNull(blockRemover);
        this.compression = Objects.requireNonNull(compression);
        this.blockNumber = Preconditions.requireWhole(blockNumber);
        this.ackHandler = Objects.requireNonNull(ackHandler);
        this.metricsService = Objects.requireNonNull(metricsService);
        this.queue = new LinkedTransferQueue<>();
    }

    @Override
    public Void call() {
        final BlockPersistenceResult result = doPersistBlock();
        LOGGER.log(DEBUG, "Persistence task completed, publishing Persistence Result: %s".formatted(result));
        ackHandler.blockPersisted(result);
        if (result.status().equals(BlockPersistenceStatus.SUCCESS)) {
            metricsService.get(BlocksPersisted).increment();
        } else {
            LOGGER.log(ERROR, "Failed to persist block [%d]".formatted(blockNumber));
            metricsService.get(BlockPersistenceError).increment();
        }

        return null;
    }

    @NonNull
    @Override
    public TransferQueue<BlockItemUnparsed> getQueue() {
        return queue;
    }

    private BlockPersistenceResult doPersistBlock() {
        // @todo(545) think about possible race conditions, it is possible that
        // the persistence handler to start two tasks for the same block number
        // simultaneously theoretically. If that happens, then maybe both of them
        // will enter in the else statement. Should the persistence handler
        // follow along writers for which block numbers have been already started
        // and if so, what kind of handling there must be?
        // @todo(599) have a way to stop long running writers
        if (blockPathResolver.existsVerifiedBlock(blockNumber)) {
            return new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.DUPLICATE_BLOCK);
        } else {
            boolean blockComplete = false;
            // @todo(598) write (append to file) items as they come in as an improvement
            final List<BlockItemUnparsed> localBlockItems = new LinkedList<>();
            while (!blockComplete) { // loop until received all items (until block proof arrives)
                try {
                    final BlockItemUnparsed nextItem = queue.take();
                    if (nextItem == AsyncBlockWriter.INCOMPLETE_BLOCK_FLAG) {
                        return new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.INCOMPLETE_BLOCK);
                    } else {
                        localBlockItems.add(nextItem);
                        if (nextItem.hasBlockProof()) {
                            blockComplete = true;
                            LOGGER.log(DEBUG, "Received Block Proof for Block [%d]".formatted(blockNumber));
                        }
                    }
                } catch (final InterruptedException e) {
                    // @todo(545) if we have entered here, something has cancelled the task.
                    // Is this the proper handling here?
                    LOGGER.log(
                            ERROR,
                            "Interrupted while waiting for next block item for block [%d]".formatted(blockNumber));
                    final BlockPersistenceResult result = revertWrite(BlockPersistenceStatus.PERSISTENCE_INTERRUPTED);
                    Thread.currentThread().interrupt();
                    return result;
                }
            }
            // proceed to persist the items
            try (final WritableStreamingData wsd = new WritableStreamingData(
                    compression.wrap(Files.newOutputStream(getResolvedUnverifiedBlockPath())))) {
                final BlockUnparsed blockToWrite =
                        BlockUnparsed.newBuilder().blockItems(localBlockItems).build();
                BlockUnparsed.PROTOBUF.toBytes(blockToWrite).writeTo(wsd);
            } catch (final IOException e) {
                LOGGER.log(ERROR, "Failed to write block [%d] to local storage!".formatted(blockNumber), e);
                return revertWrite(BlockPersistenceStatus.FAILURE_DURING_WRITE);
            }
            return new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.SUCCESS);
        }
    }

    // @todo(582) implement the unverified block logic
    private Path getResolvedUnverifiedBlockPath() throws IOException {
        // for now, we will not use the unverified block logic, deferring it for
        // a future PR - simply, we resolve the raw path to the block instead
        // of resolving it as unverified
        final Path rawBlockPath = blockPathResolver.resolveLiveRawPathToBlock(blockNumber);
        final Path resolvedBlockPath =
                FileUtilities.appendExtension(rawBlockPath, compression.getCompressionFileExtension());
        //        if (Files.notExists(resolvedBlockPath)) {
        // We should not throw, unverified blocks are allowed to be overwritten
        // in the beginning of the task we check if the block is already persisted
        // and verified. Those must never be overwritten! If we have reached
        // here, we must know that the block has never been persisted before.
        // Else we need not create the file, it will be overwritten since is
        // unverified. If the createFile method throws an exception here,
        // it is either a bug, or a potential race condition!
        FileUtilities.createFile(resolvedBlockPath);
        //        }
        return resolvedBlockPath;
    }

    /**
     * Method to revert the write operation.
     * ONLY USE IN CASE OF FAILURE AS CLEANUP.
     */
    private BlockPersistenceResult revertWrite(final BlockPersistenceStatus statusIfSuccessfulRevert) {
        try {
            blockRemover.removeLiveUnverified(blockNumber);
            return new BlockPersistenceResult(blockNumber, statusIfSuccessfulRevert);
        } catch (final IOException e) {
            LOGGER.log(ERROR, "Failed to remove block [%d]".formatted(blockNumber), e);
            return new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.FAILURE_DURING_REVERT);
        }
    }
}
