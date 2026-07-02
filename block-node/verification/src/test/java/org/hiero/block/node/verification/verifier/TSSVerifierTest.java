// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.verifier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
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
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlock;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.WRAPS;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.verification.VerificationDataProvider;
import org.hiero.block.node.verification.hasher.BlockHasher;
import org.hiero.block.node.verification.hasher.HashingResult;
import org.hiero.block.node.verification.metrics.MetricsHolder;
import org.hiero.block.node.verification.session.SessionFailureType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Tests for the [TSSVerifier].
@Timeout(unit = SECONDS, value = 5)
@DisplayName("TSS Verifier Tests")
class TSSVerifierTest {
    private MetricsHolder metricsHolder;
    private VerificationDataProvider verificationDataProvider;

    /// Setup before each.
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
    @DisplayName("verify() successful verification block 0 before settled")
    void testShouldVerifyTssWrapsBlock0BeforeSettled() throws IOException, ParseException {
        final ResourceTestBlock block0 = ResourceTestBlockBuilder.load(WRAPS.BLOCK_0);
        initializeTssData(verificationDataProvider, block0);
        final Bytes hashToVerify = runHashing(verificationDataProvider, block0).rootHash();
        final Bytes signature = extractSignature(block0.blockUnparsed());
        // genesis: vk (1096) + blsSig (1632) + aggregate Schnorr (192) = 2920
        assertThat(signature.length())
                .withFailMessage("Block 0 signature must be 2920 bytes (genesis path)")
                .isEqualTo(2_920);
        final TSSVerifier toTest = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(), hashToVerify, signature, verificationDataProvider);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual)
                .withFailMessage("TssWraps block 0 aggregate Schnorr signature should verify successfully")
                .isNull();
    }

    @Test
    @DisplayName("verify() successful verification block 467 settled path")
    void testShouldVerifyTssWrapsBlock467SettledPath() throws ParseException, IOException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(WRAPS.BLOCK_0));
        final ResourceTestBlock block467 = ResourceTestBlockBuilder.load(WRAPS.BLOCK_467);
        final Bytes hashToVerify =
                runHashing(verificationDataProvider, block467).rootHash();
        final Bytes signature = extractSignature(block467.blockUnparsed());
        // settled path: vk (1096) + blsSig (1632) + WRAPS proof (704) = 3432
        assertThat(signature.length())
                .withFailMessage("Block 467 signature must be 3432 bytes (settled WRAPS path)")
                .isEqualTo(3_432);
        final TSSVerifier toTest = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(), hashToVerify, signature, verificationDataProvider);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual)
                .withFailMessage("TssWraps block 467 settled WRAPS signature should verify successfully")
                .isNull();
    }

    @Test
    @DisplayName("verify() rejects tampered signature")
    void testShouldRejectTamperedSignature() throws ParseException, IOException {
        final ResourceTestBlock block0 = ResourceTestBlockBuilder.load(WRAPS.BLOCK_0);
        initializeTssData(verificationDataProvider, block0);
        final Bytes hashToVerify = runHashing(verificationDataProvider, block0).rootHash();
        final byte[] signature = extractSignature(block0.blockUnparsed()).toByteArray();
        signature[0] = (byte) ~signature[0];
        final TSSVerifier toTest = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(),
                hashToVerify,
                Bytes.wrap(signature),
                verificationDataProvider);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual).isNotNull().isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    @Test
    @DisplayName("verify() rejects tampered hash")
    void testShouldRejectTamperedHash() throws ParseException, IOException {
        final ResourceTestBlock block0 = ResourceTestBlockBuilder.load(WRAPS.BLOCK_0);
        initializeTssData(verificationDataProvider, block0);
        final byte[] hashToVerify =
                runHashing(verificationDataProvider, block0).rootHash().toByteArray();
        hashToVerify[0] = (byte) ~hashToVerify[0];
        final Bytes signature = extractSignature(block0.blockUnparsed());
        final TSSVerifier toTest = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(),
                Bytes.wrap(hashToVerify),
                signature,
                verificationDataProvider);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual).isNotNull().isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    /// Initialize TSS Data so we can verify TSS signatures.
    private void initializeTssData(final VerificationDataProvider vdp, final TestBlock block) {
        runHashing(vdp, block);
        assertThat(vdp.hasTssData()).isTrue();
    }

    private HashingResult runHashing(final VerificationDataProvider vdp, final TestBlock block) {
        final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
        final BlockHasher hasher = new BlockHasher(
                new AtomicBoolean(false),
                blockItemsDeque,
                metricsHolder.hashingMetrics(),
                block.number(),
                BlockSource.PUBLISHER,
                vdp);
        blockItemsDeque.add(block.asBlockItems());
        return hasher.get();
    }

    private static Bytes extractSignature(final BlockUnparsed block) throws ParseException {
        for (final BlockItemUnparsed item : block.blockItems().reversed()) {
            if (item.item().kind() == BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                BlockProof proof = BlockProof.PROTOBUF.parse(item.blockProofOrThrow());
                if (proof.hasSignedBlockProof()) {
                    return proof.signedBlockProof().blockSignature();
                }
            }
        }
        throw new IllegalStateException("No BlockProof with signedBlockProof found in block");
    }
}
