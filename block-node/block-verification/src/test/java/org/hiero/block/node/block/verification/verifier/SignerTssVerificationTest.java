// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
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
/// Generates the roster and keys locally, provisions the real [VerificationDataProvider] with the
/// signer's [org.hiero.block.signing.VerificationMaterial], signs the root hash that [BlockHasher]
/// computes for a locally built block, and confirms the production verifier accepts it — covering
/// both the genesis-Schnorr (2920 B) and settled-WRAPS (3432 B) paths plus tampering rejections.
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

    @Test
    @DisplayName("verify() accepts a locally signed block on the settled WRAPS path")
    void verifyAcceptsSettledWrapsSignedBlock() {
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3L);
        final Bytes rootHash = runHashing(block).rootHash();

        final TssBlockSigner signer = TssBlockSigner.createDeterministicSettled();
        verificationDataProvider.safeUpdateTssData(signer.verificationMaterial().tssData(), false);
        final Bytes signature = signer.signBlockProof(block.number(), rootHash)
                .signedBlockProof()
                .blockSignature();
        // settled path: hintsVk (1096) + blsSig (1632) + WRAPS proof (704) = 3432
        assertThat(signature.length())
                .withFailMessage("Settled-WRAPS signature must be 3432 bytes")
                .isEqualTo(3_432);

        final TSSVerifier verifier = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(), rootHash, signature, verificationDataProvider);

        assertThat(verifier.verify())
                .withFailMessage("Locally signed settled-WRAPS block should verify successfully")
                .isNull();
    }

    @Test
    @DisplayName("verify() accepts a locally signed block carrying state_changes")
    void verifyAcceptsLocallySignedBlockWithStateChanges() {
        // Regression coverage for the class of bug found in tools-and-tests/suites'
        // BlockItemBuilderUtils (PR #2903): its hand-duplicated mirror of this exact
        // hash-then-sign-then-verify path had three independent bugs (wrong leaf count,
        // missing TSS bootstrap, a fake non-TSS "signature") that only ever surfaced via a
        // ~1-2 minute Docker E2E run. This test exercises the same real BlockHasher ->
        // TssBlockSigner -> TSSVerifier round trip for a state_changes-carrying block, in
        // milliseconds, as part of this module's normal test task.
        final StateChange change = StateChange.newBuilder()
                .stateId(1)
                .singletonUpdate(SingletonUpdateChange.newBuilder()
                        .bytesValue(Bytes.fromHex("aabbcc"))
                        .build())
                .build();
        final StateChanges stateChanges =
                StateChanges.newBuilder().stateChanges(change).build();
        final TestBlock block = generateBlockWithStateChanges(3L, stateChanges);
        final Bytes rootHash = runHashing(block).rootHash();

        final TssBlockSigner signer = TssBlockSigner.create();
        verificationDataProvider.safeUpdateTssData(signer.verificationMaterial().tssData(), false);
        final Bytes signature = signer.signBlockProof(block.number(), rootHash)
                .signedBlockProof()
                .blockSignature();

        final TSSVerifier verifier = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(), rootHash, signature, verificationDataProvider);

        assertThat(verifier.verify())
                .withFailMessage("Locally signed state_changes-carrying block should verify successfully")
                .isNull();
    }

    @Test
    @DisplayName("verify() rejects a tampered signature")
    void verifyRejectsTamperedSignature() {
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3L);
        final Bytes rootHash = runHashing(block).rootHash();

        final TssBlockSigner signer = TssBlockSigner.create();
        verificationDataProvider.safeUpdateTssData(signer.verificationMaterial().tssData(), false);
        final byte[] signature = signer.signBlockProof(block.number(), rootHash)
                .signedBlockProof()
                .blockSignature()
                .toByteArray();
        signature[0] = (byte) ~signature[0];

        final TSSVerifier verifier = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(), rootHash, Bytes.wrap(signature), verificationDataProvider);

        assertThat(verifier.verify()).isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    /// Builds a test block carrying a {@code state_changes} item, mirroring {@link
    /// TestBlockBuilder#generateBlockWithSignedTransaction} for a different extra item type
    /// rather than modifying the shared fixture (every other block shape there needs plain
    /// header/round-header/footer/proof, not this one).
    private static TestBlock generateBlockWithStateChanges(final long blockNumber, final StateChanges stateChanges) {
        final List<BlockItemUnparsed> items = new ArrayList<>();
        items.add(TestBlockBuilder.sampleHeaderUnparsed(blockNumber));
        items.add(TestBlockBuilder.sampleRoundHeaderUnparsed(blockNumber * 10L));
        items.add(BlockItemUnparsed.newBuilder()
                .stateChanges(StateChanges.PROTOBUF.toBytes(stateChanges))
                .build());
        items.add(TestBlockBuilder.sampleFooterUnparsed(blockNumber));
        items.add(TestBlockBuilder.sampleProofUnparsed(blockNumber));
        return new TestBlock(
                blockNumber, BlockUnparsed.newBuilder().blockItems(items).build());
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
