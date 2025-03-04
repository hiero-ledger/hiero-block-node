// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.verification.session;

import static java.lang.System.Logger.Level.WARNING;

import com.hedera.block.common.hasher.Hashes;
import com.hedera.block.common.hasher.HashingUtilities;
import com.hedera.block.common.hasher.StreamingTreeHasher;
import com.hedera.block.server.metrics.BlockNodeMetricTypes;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.verification.BlockVerificationStatus;
import com.hedera.block.server.verification.VerificationResult;
import com.hedera.block.server.verification.signature.SignatureVerifier;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * An abstract base class providing common functionality for block verification sessions.
 * Concrete classes handle how block items are appended and processed (synchronously or asynchronously).
 */
public abstract class BlockVerificationSessionBase implements BlockVerificationSession {

    /**
     * The logger for this class.
     */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /**
     * The metrics service.
     */
    protected final MetricsService metricsService;
    /**
     * The signature verifier.
     */
    protected final SignatureVerifier signatureVerifier;
    /**
     * The block number being verified.
     */
    protected final long blockNumber;
    /**
     * The tree hasher for input hashes.
     */
    protected final StreamingTreeHasher inputTreeHasher;
    /**
     * The tree hasher for output hashes.
     */
    protected final StreamingTreeHasher outputTreeHasher;
    /**
     * The time when block verification started.
     */
    protected final long blockWorkStartTime;
    /**
     * A flag indicating whether the session is running.
     */
    protected volatile boolean running = true;
    /**
     * The future for the verification result.
     */
    protected final CompletableFuture<VerificationResult> verificationResultFuture = new CompletableFuture<>();

    /**
     * Constructs the session with shared initialization logic.
     *
     * @param blockHeader the block header
     * @param metricsService the metrics service
     * @param signatureVerifier the signature verifier
     * @param inputTreeHasher the input tree hasher (e.g. naive or concurrent)
     * @param outputTreeHasher the output tree hasher (e.g. naive or concurrent)
     */
    protected BlockVerificationSessionBase(
            @NonNull final BlockHeader blockHeader,
            @NonNull final MetricsService metricsService,
            @NonNull final SignatureVerifier signatureVerifier,
            @NonNull final StreamingTreeHasher inputTreeHasher,
            @NonNull final StreamingTreeHasher outputTreeHasher) {
        this.blockNumber = Objects.requireNonNull(blockHeader).number();
        this.metricsService = Objects.requireNonNull(metricsService);
        this.signatureVerifier = Objects.requireNonNull(signatureVerifier);
        this.inputTreeHasher = Objects.requireNonNull(inputTreeHasher);
        this.outputTreeHasher = Objects.requireNonNull(outputTreeHasher);

        this.blockWorkStartTime = System.nanoTime();
    }

    @Override
    public final boolean isRunning() {
        return running;
    }

    @Override
    public final CompletableFuture<VerificationResult> getVerificationResult() {
        return verificationResultFuture;
    }

    /**
     * Processes the provided block items by updating the tree hashers.
     * If the last item has a block proof, final verification is triggered.
     *
     * @param blockItems the block items to process
     * @throws ParseException if a parsing error occurs
     */
    protected void processBlockItems(List<BlockItemUnparsed> blockItems) throws ParseException {

        Hashes hashes = HashingUtilities.getBlockHashes(blockItems);
        while (hashes.inputHashes().hasRemaining()) {
            inputTreeHasher.addLeaf(hashes.inputHashes());
        }
        while (hashes.outputHashes().hasRemaining()) {
            outputTreeHasher.addLeaf(hashes.outputHashes());
        }

        // Check if this batch contains the final block proof
        final BlockItemUnparsed lastItem = blockItems.getLast();
        if (lastItem.hasBlockProof()) {
            BlockProof blockProof = BlockProof.PROTOBUF.parse(lastItem.blockProof());
            finalizeVerification(blockProof);
        }
    }

    /**
     * Finalizes the block verification by computing the final block hash,
     * verifying its signature, and updating metrics accordingly.
     *
     * @param blockProof the block proof
     */
    protected void finalizeVerification(BlockProof blockProof) {
        Bytes blockHash = HashingUtilities.computeFinalBlockHash(blockProof, inputTreeHasher, outputTreeHasher);
        VerificationResult result;
        boolean verified = signatureVerifier.verifySignature(blockHash, blockProof.blockSignature());
        if (verified) {
            long verificationLatency = System.nanoTime() - blockWorkStartTime;
            metricsService
                    .get(BlockNodeMetricTypes.Counter.VerificationBlockTime)
                    .add(verificationLatency);
            metricsService
                    .get(BlockNodeMetricTypes.Counter.VerificationBlocksVerified)
                    .increment();

            result = new VerificationResult(blockNumber, blockHash, BlockVerificationStatus.VERIFIED);
        } else {
            LOGGER.log(WARNING, "Block verification failed for block number: {0}", blockNumber);
            metricsService
                    .get(BlockNodeMetricTypes.Counter.VerificationBlocksFailed)
                    .increment();

            result = new VerificationResult(blockNumber, blockHash, BlockVerificationStatus.INVALID_HASH_OR_SIGNATURE);
        }
        shutdownSession();
        verificationResultFuture.complete(result);
    }

    /**
     * Shuts down this session, marking it as no longer running.
     */
    protected void shutdownSession() {
        running = false;
    }

    /**
     * A helper method that handles errors encountered during block item processing.
     *
     * @param ex the exception encountered
     */
    protected void handleProcessingError(Throwable ex) {
        LOGGER.log(System.Logger.Level.ERROR, "Error processing block items", ex);
        metricsService.get(BlockNodeMetricTypes.Counter.VerificationBlocksError).increment();
        verificationResultFuture.completeExceptionally(ex);
    }
}
