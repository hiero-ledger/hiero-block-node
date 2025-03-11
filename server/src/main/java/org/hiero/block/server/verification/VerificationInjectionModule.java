// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import javax.inject.Singleton;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.verification.service.BlockVerificationService;
import org.hiero.block.server.verification.service.BlockVerificationServiceImpl;
import org.hiero.block.server.verification.service.NoOpBlockVerificationService;
import org.hiero.block.server.verification.session.BlockVerificationSessionFactory;
import org.hiero.block.server.verification.signature.SignatureVerifier;
import org.hiero.block.server.verification.signature.SignatureVerifierDummy;

/**
 * The module used to inject the verification service and signature verifier into the application.
 */
@Module
public interface VerificationInjectionModule {

    /**
     * Provides the signature verifier.
     *
     * @param signatureVerifier the signature verifier to be used
     * @return the signature verifier
     */
    @Binds
    @Singleton
    SignatureVerifier bindSignatureVerifier(SignatureVerifierDummy signatureVerifier);

    /**
     * Provides the block verification service.
     *
     * @param verificationConfig the verification configuration to be used
     * @param metricsService the metrics service to be used
     * @param blockVerificationSessionFactory the block verification session factory to be used
     * @return the block verification service
     */
    @Provides
    @Singleton
    static BlockVerificationService provideBlockVerificationService(
            @NonNull final VerificationConfig verificationConfig,
            @NonNull final MetricsService metricsService,
            @NonNull final BlockVerificationSessionFactory blockVerificationSessionFactory,
            @NonNull final AckHandler ackHandler) {
        if (verificationConfig.type() == VerificationConfig.VerificationServiceType.NO_OP) {
            return new NoOpBlockVerificationService();
        } else {
            return new BlockVerificationServiceImpl(metricsService, blockVerificationSessionFactory, ackHandler);
        }
    }

    /**
     * Provides the block verification session factory.
     * Uses the common fork join pool for the executor service, of the concurrent hashing tree for now.
     *
     * @param verificationConfig the verification configuration to be used
     * @param metricsService the metrics service to be used
     * @param signatureVerifier the signature verifier to be used
     * @return the block verification session factory
     */
    @Provides
    @Singleton
    static BlockVerificationSessionFactory provideBlockVerificationSessionFactory(
            @NonNull final VerificationConfig verificationConfig,
            @NonNull final MetricsService metricsService,
            @NonNull final SignatureVerifier signatureVerifier) {
        final ExecutorService executorService = ForkJoinPool.commonPool();
        return new BlockVerificationSessionFactory(
                verificationConfig, metricsService, signatureVerifier, executorService);
    }
}
