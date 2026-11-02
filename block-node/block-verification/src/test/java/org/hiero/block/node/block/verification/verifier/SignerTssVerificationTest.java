// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// End-to-end test that a [TssBlockSigner] signature is accepted by the real [TSSVerifier].
///
/// Generates the roster and keys locally, provisions the real [VerificationDataProvider] with the
/// signer's [org.hiero.block.signing.VerificationMaterial], signs the root hash that [BlockHasher]
/// computes for a locally built block, and confirms the production verifier accepts it, plus the
/// rejections of tampered input. The signature and the ledger id are opaque to the verifier.
@Timeout(unit = SECONDS, value = 30)
@DisplayName("Signer to TSSVerifier end-to-end")
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

    /// Tests of signatures the verifier accepts.
    @Nested
    @DisplayName("Accepted signatures")
    class AcceptedSignatureTests {
        /// This test aims to assert that a block signed by the test signer verifies with the production
        /// [TSSVerifier] once the provider holds the signer's TSS data, which is the only verification
        /// input besides the signature and the block root hash.
        @Test
        @DisplayName("verify() accepts a locally signed block")
        void verifyAcceptsLocallySignedBlock() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3L);
            final Bytes rootHash = runHashing(block).rootHash();
            final TssBlockSigner signer = TssBlockSigner.create();
            verificationDataProvider.safeUpdateTssData(
                    signer.verificationMaterial().tssData(), false);
            final Bytes signature = signer.signBlockProof(block.number(), rootHash)
                    .signedBlockProof()
                    .blockSignature();

            final TSSVerifier verifier = new TSSVerifier(
                    metricsHolder.proofVerificationMetrics(), rootHash, signature, verificationDataProvider);

            assertThat(verifier.verify())
                    .withFailMessage("Locally signed TSS block should verify successfully")
                    .isNull();
        }

        /// This test aims to assert that TSS data without the informational roster is enough to verify:
        /// the provider installs it and the verifier accepts a signed block with it.
        @Test
        @DisplayName("verify() accepts a signed block with TSS data that has no roster")
        void verifyAcceptsWithTssDataWithoutRoster() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3L);
            final Bytes rootHash = runHashing(block).rootHash();
            final TssBlockSigner signer = TssBlockSigner.create();
            final TssData withoutRoster = signer.verificationMaterial()
                    .tssData()
                    .copyBuilder()
                    .currentRoster((TssRoster) null)
                    .build();
            verificationDataProvider.safeUpdateTssData(withoutRoster, false);
            final Bytes signature = signer.signBlockProof(block.number(), rootHash)
                    .signedBlockProof()
                    .blockSignature();

            final TSSVerifier verifier = new TSSVerifier(
                    metricsHolder.proofVerificationMetrics(), rootHash, signature, verificationDataProvider);

            assertThat(verifier.verify()).isNull();
        }
    }

    /// Tests of signatures and data the verifier rejects.
    @Nested
    @DisplayName("Rejected signatures")
    class RejectedSignatureTests {
        /// This test aims to assert that a valid signature checked against a different root hash than the
        /// one it signs is reported as a bad block proof.
        @Test
        @DisplayName("verify() rejects the signature against a different root hash")
        void verifyRejectsMismatchedRootHash() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3L);
            final Bytes rootHash = runHashing(block).rootHash();
            final TssBlockSigner signer = TssBlockSigner.create();
            verificationDataProvider.safeUpdateTssData(
                    signer.verificationMaterial().tssData(), false);
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

        /// This test aims to assert that a signature with one byte flipped, at its start, in its middle or
        /// at its end, is reported as a bad block proof.
        ///
        /// @param position where to flip a byte, as a fraction of the signature length in percent
        @ParameterizedTest(name = "flipped byte at {0} percent")
        @ValueSource(ints = {0, 50, 100})
        @DisplayName("verify() rejects a tampered signature")
        void verifyRejectsTamperedSignature(final int position) {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3L);
            final Bytes rootHash = runHashing(block).rootHash();
            final TssBlockSigner signer = TssBlockSigner.create();
            verificationDataProvider.safeUpdateTssData(
                    signer.verificationMaterial().tssData(), false);
            final byte[] signature = signer.signBlockProof(block.number(), rootHash)
                    .signedBlockProof()
                    .blockSignature()
                    .toByteArray();
            final int index = Math.min(signature.length - 1, signature.length * position / 100);
            signature[index] = (byte) ~signature[index];

            final TSSVerifier verifier = new TSSVerifier(
                    metricsHolder.proofVerificationMetrics(),
                    rootHash,
                    Bytes.wrap(signature),
                    verificationDataProvider);

            assertThat(verifier.verify()).isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
        }

        /// This test aims to assert that a ledger id the TSS library considers malformed, as held by TSS data
        /// from a network that used an earlier TSS library, makes the library throw and the verifier report a
        /// bad block proof instead of letting the exception escape.
        @Test
        @DisplayName("verify() reports a bad block proof for a ledger id the library rejects")
        void verifyRejectsMalformedLedgerId() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3L);
            final Bytes rootHash = runHashing(block).rootHash();
            final TssBlockSigner signer = TssBlockSigner.create();
            final TssData signerData = signer.verificationMaterial().tssData();
            final Bytes truncatedLedgerId =
                    signerData.ledgerId().slice(0, signerData.ledgerId().length() / 2);
            verificationDataProvider.safeUpdateTssData(
                    signerData.copyBuilder().ledgerId(truncatedLedgerId).build(), false);
            final Bytes signature = signer.signBlockProof(block.number(), rootHash)
                    .signedBlockProof()
                    .blockSignature();

            final TSSVerifier verifier = new TSSVerifier(
                    metricsHolder.proofVerificationMetrics(), rootHash, signature, verificationDataProvider);

            assertThat(verifier.verify()).isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
        }

        /// This test aims to assert that without any TSS data the verifier reports missing verification data
        /// rather than a bad block proof, since the signature was never checked.
        @Test
        @DisplayName("verify() reports missing verification data without TSS data")
        void verifyReportsMissingTssData() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3L);
            final Bytes rootHash = runHashing(block).rootHash();
            final Bytes signature = TssBlockSigner.create()
                    .signBlockProof(block.number(), rootHash)
                    .signedBlockProof()
                    .blockSignature();

            final TSSVerifier verifier = new TSSVerifier(
                    metricsHolder.proofVerificationMetrics(), rootHash, signature, verificationDataProvider);

            assertThat(verifier.verify()).isEqualTo(SessionFailureType.MISSING_VERIFICATION_DATA);
        }
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
