// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import javax.inject.Inject;

/**
 * An implementation of {@link LocalBlockArchiver} that utilizes the
 * {@link com.hedera.block.server.persistence.storage.PersistenceStorageConfig.StorageType#BLOCK_AS_LOCAL_FILE}
 * persistence type.
 */
public final class BlockAsLocalFileArchiver implements LocalBlockArchiver {
    private final PersistenceStorageConfig config;
    private final BlockPathResolver blockPathResolver;
    private final CompletionService<Void> completionService;
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
            final Callable<Void> archivingTask =
                    new LocalGroupZipArchiveTask(thresholdOneGroupSizeLower, config, blockPathResolver);
            completionService.submit(archivingTask);
        }
        handleSubmittedResults();
    }

    private void handleSubmittedResults() {
        Future<Void> completionResult;
        while ((completionResult = completionService.poll()) != null) {
            try {
                if (completionResult.isCancelled()) {
                    // @todo(517) when we have infrastructure for publishing
                    // results, we should do so
                } else {
                    // we call get here to verify that the task has run to completion
                    // we do not expect it to throw an exception, but to publish
                    // a meaningful result, if an exception is thrown, it should be
                    // either considered a bug or an unhandled exception
                    completionResult.get();
                }
            } catch (final ExecutionException e) {
                // we do not expect to enter here, if we do, then there is
                // either a bug in the archiving task, or an unhandled case
                throw new RuntimeException(e.getCause());
            } catch (final InterruptedException e) {
                // @todo(517) What shall we do here? How to handle?
                Thread.currentThread().interrupt();
            }
        }
    }
}
