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
import java.nio.file.OpenOption;
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
        // @todo(713) think about possible race conditions, it is possible that
        //    the persistence handler to start two tasks for the same block number
        //    simultaneously theoretically. If that happens, then maybe both of them
        //    will enter in the else statement. Should the persistence handler
        //    follow along writers for which block numbers have been already started
        //    and if so, what kind of handling there must be?
        // @todo(599) have a way to stop long running writers
        if (blockPathResolver.existsVerifiedBlock(blockNumber)) {
            return new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.DUPLICATE_BLOCK);
        } else {
            boolean blockComplete = false;
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
                    // @todo(713) if we have entered here, something has cancelled the task.
                    //    Is this the proper handling here?
                    LOGGER.log(
                            ERROR,
                            "Interrupted while waiting for next block item for block [%d]".formatted(blockNumber));
                    final BlockPersistenceResult result = revertWrite(BlockPersistenceStatus.PERSISTENCE_INTERRUPTED);
                    Thread.currentThread().interrupt();
                    return result;
                }
            }
            // proceed to persist the items
            // providing no {@link OpenOption} to the newOutputStream method
            // will create the file if it does not exist or truncate it if it does
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

    /**
     * This method will resolve the path to where the unverified block must be
     * written. We only need to resolve the path to the block. Unverified blocks
     * can be overwritten. This path will be used to open a new output stream
     * using {@link Files#newOutputStream(Path, OpenOption...)}. By default, if
     * no options are provided, the file will be created if it does not exist or
     * truncated if it does exist (effectively overwritten).
     *
     * @return the resolved path to the unverified block
     */
    private Path getResolvedUnverifiedBlockPath() {
        final Path rawUnverifiedBlockPath = blockPathResolver.resolveLiveRawUnverifiedPathToBlock(blockNumber);
        return FileUtilities.appendExtension(rawUnverifiedBlockPath, compression.getCompressionFileExtension());
    }

    /**
     * Method to revert the write operation.
     * ONLY USE IN CASE OF FAILURE AS CLEANUP.
     */
    private BlockPersistenceResult revertWrite(final BlockPersistenceStatus statusIfSuccessfulRevert) {
        try {
            blockRemover.removeUnverified(blockNumber);
            return new BlockPersistenceResult(blockNumber, statusIfSuccessfulRevert);
        } catch (final IOException e) {
            LOGGER.log(ERROR, "Failed to remove block [%d]".formatted(blockNumber), e);
            return new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.FAILURE_DURING_REVERT);
        }
    }
}
