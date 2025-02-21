// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
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
public final class LocalBlockFileArchiver implements LocalBlockArchiver {
    private final CompletionService<Void> completionService;
    private final int archiveGroupSize;
    private final AsyncLocalBlockArchiverFactory archiverFactory;

    @Inject
    public LocalBlockFileArchiver(
            @NonNull final PersistenceStorageConfig config,
            @NonNull final AsyncLocalBlockArchiverFactory archiverFactory,
            @NonNull final Executor executor) {
        this.archiveGroupSize = config.archiveGroupSize();
        this.archiverFactory = Objects.requireNonNull(archiverFactory);
        this.completionService = new ExecutorCompletionService<>(executor);
    }

    @Override
    public void submitThresholdPassed(final long blockNumberThreshold) {
        final boolean validThresholdPassed =
                (blockNumberThreshold > 1) && (blockNumberThreshold % archiveGroupSize == 0);
        final boolean canArchive = blockNumberThreshold - archiveGroupSize * 2L >= 0;
        if (validThresholdPassed && canArchive) {
            // here we need to archive everything below one order of magnitude of the threshold passed
            final AsyncLocalBlockArchiver archiver = archiverFactory.create(blockNumberThreshold - archiveGroupSize);
            completionService.submit(archiver, null);
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
