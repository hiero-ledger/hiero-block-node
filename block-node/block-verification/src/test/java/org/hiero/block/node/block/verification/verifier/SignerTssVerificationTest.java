// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.hasher.BlockHasher;
import org.hiero.block.node.block.verification.hasher.HashingResult;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.SessionFailureType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.signing.TssBlockSigner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// End-to-end test that a [TssBlockSigner] signature is accepted by the real [TSSVerifier].
///
/// Unlike [TSSVerifierTest], which replays captured consensus-node blocks, this test generates the
/// roster and keys locally, provisions the real [VerificationDataProvider] with the signer's
/// [org.hiero.block.signing.VerificationMaterial], signs the root hash that [BlockHasher] computes for
/// a locally built block, and confirms the production verifier accepts it.
@Timeout(unit = SECONDS, value = 30)
@DisplayName("Signer -> TSSVerifier end-to-end")
class SignerTssVerificationTest {

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
    @DisplayName("verify() accepts a locally signed block")
    void verifyAcceptsLocallySignedBlock() {
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3L);
        final Bytes rootHash = runHashing(block).rootHash();

        final TssBlockSigner signer = TssBlockSigner.create();
        verificationDataProvider.safeUpdateTssData(signer.verificationMaterial().tssData(), false);
        final Bytes signature = signer.signBlockProof(block.number(), rootHash)
                .signedBlockProof()
                .blockSignature();

        final TSSVerifier verifier = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(), rootHash, signature, verificationDataProvider);

        assertThat(verifier.verify())
                .withFailMessage("Locally signed TSS block should verify successfully")
                .isNull();
    }

    @Test
    @DisplayName("verify() rejects the signature against a different root hash")
    void verifyRejectsMismatchedRootHash() {
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3L);
        final Bytes rootHash = runHashing(block).rootHash();

        final TssBlockSigner signer = TssBlockSigner.create();
        verificationDataProvider.safeUpdateTssData(signer.verificationMaterial().tssData(), false);
        final Bytes signature = signer.signBlockProof(block.number(), rootHash)
                .signedBlockProof()
                .blockSignature();

        final byte[] tamperedHash = rootHash.toByteArray();
        tamperedHash[0] = (byte) ~tamperedHash[0];
        final TSSVerifier verifier = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(),
                Bytes.wrap(tamperedHash),
                signature,
                verificationDataProvider);

        assertThat(verifier.verify()).isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    private HashingResult runHashing(final TestBlock block) {
        final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
        final BlockHasher hasher = new BlockHasher(
                new AtomicBoolean(false),
                blockItemsDeque,
                metricsHolder.hashingMetrics(),
                block.number(),
                BlockSource.PUBLISHER,
                verificationDataProvider);
        blockItemsDeque.add(block.asBlockItems());
        return hasher.get();
    }
}
