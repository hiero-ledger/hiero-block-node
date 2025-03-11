// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification.session;

import com.hedera.hapi.block.stream.output.BlockHeader;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.verification.VerificationConfig;
import org.hiero.block.server.verification.signature.SignatureVerifier;

/**
 * A factory for creating block verification sessions.
 */
public class BlockVerificationSessionFactory {

    private final VerificationConfig config;
    private final MetricsService metricsService;
    private final SignatureVerifier signatureVerifier;
    private final ExecutorService executorService;
    private final int hashCombineBatchSize;

    /**
     * Constructs a block verification session factory.
     *
     * @param verificationConfig the verification configuration
     * @param metricsService the metrics service
     * @param signatureVerifier the signature verifier
     * @param executorService the executor service
     */
    @Inject
    public BlockVerificationSessionFactory(
            @NonNull final VerificationConfig verificationConfig,
            @NonNull final MetricsService metricsService,
            @NonNull final SignatureVerifier signatureVerifier,
            @NonNull final ExecutorService executorService) {
        Objects.requireNonNull(verificationConfig);
        Objects.requireNonNull(metricsService);
        Objects.requireNonNull(signatureVerifier);
        Objects.requireNonNull(executorService);

        this.config = verificationConfig;
        this.metricsService = metricsService;
        this.signatureVerifier = signatureVerifier;
        this.executorService = executorService;
        this.hashCombineBatchSize = verificationConfig.hashCombineBatchSize();
    }

    /**
     * Creates a new block verification session.
     *
     * @param blockHeader the block header
     * @return the block verification session
     */
    public BlockVerificationSession createSession(@NonNull final BlockHeader blockHeader) {

        BlockVerificationSessionType type =
                BlockVerificationSessionType.valueOf(config.sessionType().name());

        return switch (type) {
            case ASYNC -> new BlockVerificationSessionAsync(
                    blockHeader, metricsService, signatureVerifier, executorService, hashCombineBatchSize);
            case SYNC -> new BlockVerificationSessionSync(blockHeader, metricsService, signatureVerifier);
        };
    }
}
