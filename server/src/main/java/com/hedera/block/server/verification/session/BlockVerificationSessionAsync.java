// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.verification.session;

import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.verification.signature.SignatureVerifier;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.hiero.block.common.hasher.ConcurrentStreamingTreeHasher;

/**
 * An asynchronous implementation of the BlockVerificationSession. It processes the block items
 * asynchronously using an executor.
 */
public class BlockVerificationSessionAsync extends BlockVerificationSessionBase {

    /**
     * The logger for this class.
     */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final ExecutorService taskExecutor;

    /**
     * Constructs an asynchronous block verification session.
     *
     * @param blockHeader        the header of the block being verified
     * @param metricsService     the service to record metrics
     * @param signatureVerifier  the signature verifier
     * @param executorService    the executor service to use for processing block items
     * @param hashCombineBatchSize the batch size for combining hashes
     */
    public BlockVerificationSessionAsync(
            @NonNull final BlockHeader blockHeader,
            @NonNull final MetricsService metricsService,
            @NonNull final SignatureVerifier signatureVerifier,
            @NonNull final ExecutorService executorService,
            final int hashCombineBatchSize) {

        super(
                blockHeader,
                metricsService,
                signatureVerifier,
                new ConcurrentStreamingTreeHasher(executorService, hashCombineBatchSize),
                new ConcurrentStreamingTreeHasher(executorService, hashCombineBatchSize));

        this.taskExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "block-verification-session-" + this.blockNumber);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Appends new block items to be processed by this verification session.
     * The block items are processed asynchronously.
     *
     * @param blockItems the list of block items to process.
     */
    @Override
    public void appendBlockItems(@NonNull final List<BlockItemUnparsed> blockItems) {
        if (!isRunning()) {
            LOGGER.log(System.Logger.Level.ERROR, "Block verification session is not running");
            return;
        }

        // Submit a task that processes the block items asynchronously
        Callable<Void> task = () -> {
            try {
                processBlockItems(blockItems);
            } catch (Exception ex) {
                handleProcessingError(ex);
            }
            return null;
        };
        taskExecutor.submit(task);
    }

    @Override
    protected void shutdownSession() {
        super.shutdownSession();
        this.taskExecutor.shutdown();
    }
}
