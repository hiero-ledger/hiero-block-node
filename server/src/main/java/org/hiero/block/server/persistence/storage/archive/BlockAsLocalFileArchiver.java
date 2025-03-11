// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.archive;

import static java.lang.System.Logger.Level.TRACE;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import javax.inject.Inject;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.path.BlockPathResolver;

/**
 * An implementation of {@link LocalBlockArchiver} that utilizes the
 * {@link PersistenceStorageConfig.StorageType#BLOCK_AS_LOCAL_FILE}
 * persistence type.
 */
public final class BlockAsLocalFileArchiver implements LocalBlockArchiver {
    private final Logger LOGGER = System.getLogger(BlockAsLocalFileArchiver.class.getName());
    private final PersistenceStorageConfig config;
    private final BlockPathResolver blockPathResolver;
    private final CompletionService<Long> completionService;
    private final int archiveGroupSize;

    @Inject
    public BlockAsLocalFileArchiver(
            @NonNull final PersistenceStorageConfig config,
            @NonNull final BlockPathResolver blockPathResolver,
            @NonNull final Executor executor) {
        this.config = Objects.requireNonNull(config);
        this.blockPathResolver = Objects.requireNonNull(blockPathResolver);
        this.completionService = new ExecutorCompletionService<>(executor);
        this.archiveGroupSize = config.archiveGroupSize();
    }

    @Override
    public void notifyBlockPersisted(final long blockNumber) {
        final boolean validThresholdPassed = (blockNumber >= 10) && (blockNumber % archiveGroupSize == 0);
        final boolean canArchive = blockNumber - archiveGroupSize * 2L >= 0;
        if (validThresholdPassed && canArchive) {
            // here we need to archive everything below 1 group size lower than the threshold passed
            final long thresholdOneGroupSizeLower = blockNumber - archiveGroupSize;
            final Callable<Long> archivingTask =
                    new LocalGroupZipArchiveTask(thresholdOneGroupSizeLower, config, blockPathResolver);
            completionService.submit(archivingTask);
        }
        handleSubmittedResults();
    }

    private void handleSubmittedResults() {
        Future<Long> completionResult;
        while ((completionResult = completionService.poll()) != null) {
            try {
                if (completionResult.isCancelled()) {
                    // @todo(713) when we have infrastructure for publishing
                    //    results, we should do so
                } else {
                    // We call get here to verify that the task has run to completion
                    // we do not expect it to throw an exception, but to publish
                    // a meaningful result, if an exception is thrown, it should be
                    // either considered a bug or an unhandled exception.
                    // We do not expect a null result from the get method.
                    // The result should be the number of actual block items
                    // archived.
                    final long result = completionResult.get();
                    // @todo(713) this is a good place to do some metrics
                    LOGGER.log(TRACE, "Archived [{0}] BlockFiles", result);
                }
            } catch (final ExecutionException e) {
                // we do not expect to enter here, if we do, then there is
                // either a bug in the archiving task, or an unhandled case
                throw new BlockArchivingException(e.getCause());
            } catch (final InterruptedException e) {
                // @todo(713) What shall we do here? How to handle?
                Thread.currentThread().interrupt();
            }
        }
    }
}
