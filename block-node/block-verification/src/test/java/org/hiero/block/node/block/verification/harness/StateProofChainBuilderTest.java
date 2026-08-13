// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.harness;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.hasher.BlockHasher;
import org.hiero.block.node.block.verification.hasher.HashingResult;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.SessionFailureType;
import org.hiero.block.node.block.verification.verifier.StateProofVerifier;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.signing.TssBlockSigner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Smoke test proving StateProofChainBuilder emits blocks that pass the real StateProofVerifier. */
@Timeout(unit = SECONDS, value = 45)
@DisplayName("StateProofChainBuilder end-to-end")
class StateProofChainBuilderTest {

    private MetricsHolder metricsHolder;
    private VerificationDataProvider verificationDataProvider;

    @BeforeEach
    void setUp() {
        final BlockNodeContext context = TestUtils.testContext(
                new TestConfigurationBuilder().getOrCreateConfig(),
                new TestThreadPoolManager<>(
                        new BlockingExecutor(new LinkedBlockingQueue<>()),
                        new ScheduledBlockingExecutor(new LinkedBlockingQueue<>())));
        metricsHolder = MetricsHolder.create(context.metricRegistry());
        verificationDataProvider = new VerificationDataProvider(context);
    }

    @Test
    @DisplayName("Every gap block in a 3-block chain verifies via StateProofVerifier")
    void threeGapBlockChainAllVerify() {
        final TssBlockSigner signer = TssBlockSigner.create();
        verificationDataProvider.safeUpdateTssData(signer.verificationMaterial().tssData(), false);
        final StateProofChainBuilder builder =
                new StateProofChainBuilder(signer, verificationDataProvider, metricsHolder);

        final StateProofChainBuilder.Signed block0 = builder.genesisWithPublication();
        final StateProofChainBuilder.Signed block1 = builder.next(1L);
        final StateProofChainBuilder.Signed block2 = builder.next(2L);

        for (final StateProofChainBuilder.Signed signed :
                new StateProofChainBuilder.Signed[] {block0, block1, block2}) {
            final HashingResult hashing = runHashing(signed.block());
            final com.hedera.hapi.block.stream.StateProof stateProof =
                    hashing.blockProofs().getFirst().blockStateProof();
            assertThat(stateProof)
                    .withFailMessage(
                            "Block %d must carry a state proof", signed.block().number())
                    .isNotNull();
            final StateProofVerifier verifier = new StateProofVerifier(
                    new AtomicBoolean(false),
                    metricsHolder.proofVerificationMetrics(),
                    signed.block().number(),
                    stateProof,
                    signed.rootHash(),
                    verificationDataProvider);
            final SessionFailureType result = verifier.verify();
            assertThat(result)
                    .withFailMessage(
                            "Block %d state proof must verify, got %s",
                            signed.block().number(), result)
                    .isNull();
        }
    }

    private HashingResult runHashing(final TestBlock block) {
        final ConcurrentLinkedDeque<BlockItems> deque = new ConcurrentLinkedDeque<>();
        final BlockHasher hasher = new BlockHasher(
                new AtomicBoolean(false),
                deque,
                metricsHolder.hashingMetrics(),
                block.number(),
                BlockSource.PUBLISHER,
                verificationDataProvider);
        deque.add(block.asBlockItems());
        return hasher.get();
    }
}
