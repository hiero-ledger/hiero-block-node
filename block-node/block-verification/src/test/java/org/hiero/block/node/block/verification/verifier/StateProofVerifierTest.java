// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.SiblingNode;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlock;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.StateProof;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.hasher.BlockHasher;
import org.hiero.block.node.block.verification.hasher.HashingResult;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.SessionFailureType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/// Tests for [StateProofVerifier].
@DisplayName("State Proof Verifier Tests")
class StateProofVerifierTest {
    private AtomicBoolean isCanceled;
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
        isCanceled = new AtomicBoolean(false);
    }

    @ParameterizedTest
    @MethodSource("blocksWithStateProof")
    @DisplayName("verify() passes for valid state proof")
    void testPassingStateProofVerification(final ResourceTestBlock block) throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        final HashingResult hashingResult = runHashing(verificationDataProvider, block);
        final StateProofVerifier toTest = new StateProofVerifier(
                isCanceled,
                metricsHolder.proofVerificationMetrics(),
                block.number(),
                hashingResult.blockProofs().getFirst().blockStateProof(),
                hashingResult.rootHash(),
                verificationDataProvider);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual).isNull();
    }

    @Test
    @DisplayName("verify() reject tampered sibling hash")
    void testShouldRejectTamperedSiblingHash() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Tamper with a sibling hash - the reconstructed root will differ, TSS verification must fail
        final TestBlock tamperedBlock = tamperSiblingHash(ResourceTestBlockBuilder.load(StateProof.BLOCK_3));
        final HashingResult hashingResult = runHashing(verificationDataProvider, tamperedBlock);
        final StateProofVerifier toTest = new StateProofVerifier(
                isCanceled,
                metricsHolder.proofVerificationMetrics(),
                tamperedBlock.number(),
                hashingResult.blockProofs().getFirst().blockStateProof(),
                hashingResult.rootHash(),
                verificationDataProvider);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual).isNotNull().isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    @Test
    @DisplayName("verify() reject tampered timestamp leaf")
    void testShouldRejectTamperedTimestampLeaf() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Tamper with the timestamp leaf in path 0 - the signed block root will differ
        final TestBlock tamperedBlock = tamperTimestampLeaf(ResourceTestBlockBuilder.load(StateProof.BLOCK_3));
        final HashingResult hashingResult = runHashing(verificationDataProvider, tamperedBlock);
        final StateProofVerifier toTest = new StateProofVerifier(
                isCanceled,
                metricsHolder.proofVerificationMetrics(),
                tamperedBlock.number(),
                hashingResult.blockProofs().getFirst().blockStateProof(),
                hashingResult.rootHash(),
                verificationDataProvider);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual).isNotNull().isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    @Test
    @DisplayName("verify() reject tampered block content with valid state proof")
    void testShouldRejectTamperedBlockContentWithValidStateProof() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Tamper with a transaction in the block body while leaving the state proof unchanged.
        // The independently computed blockRootHash will differ from what the state proof chain
        // encodes, so the first-iteration integrity check must reject the block.
        final TestBlock tamperedBlock = tamperSignedTransaction(ResourceTestBlockBuilder.load(StateProof.BLOCK_3));
        final HashingResult hashingResult = runHashing(verificationDataProvider, tamperedBlock);
        final StateProofVerifier toTest = new StateProofVerifier(
                isCanceled,
                metricsHolder.proofVerificationMetrics(),
                tamperedBlock.number(),
                hashingResult.blockProofs().getFirst().blockStateProof(),
                hashingResult.rootHash(),
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

    /// Creates a copy of the block with a tampered sibling hash in the state proof's path 1.
    /// Flips the first byte of the first sibling's hash.
    private TestBlock tamperSiblingHash(final TestBlock block) throws ParseException {
        List<BlockItemUnparsed> items = new ArrayList<>(block.blockUnparsed().blockItems());
        for (int i = items.size() - 1; i >= 0; i--) {
            BlockItemUnparsed item = items.get(i);
            if (item.item().kind() != BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                continue;
            }
            BlockProof proof = BlockProof.PROTOBUF.parse(item.blockProofOrThrow());
            if (!proof.hasBlockStateProof()) {
                continue;
            }
            com.hedera.hapi.block.stream.StateProof stateProof = proof.blockStateProof();
            List<SiblingNode> siblings =
                    new ArrayList<>(stateProof.paths().get(1).siblings());
            SiblingNode original = siblings.get(0);
            byte[] tamperedHash = original.hash().toByteArray();
            tamperedHash[0] = (byte) ~tamperedHash[0];
            siblings.set(0, new SiblingNode(original.isLeft(), Bytes.wrap(tamperedHash)));

            com.hedera.hapi.block.stream.StateProof tamperedStateProof = stateProof
                    .copyBuilder()
                    .paths(List.of(
                            stateProof.paths().get(0),
                            stateProof
                                    .paths()
                                    .get(1)
                                    .copyBuilder()
                                    .siblings(siblings)
                                    .build(),
                            stateProof.paths().get(2)))
                    .build();
            BlockProof tamperedProof =
                    proof.copyBuilder().blockStateProof(tamperedStateProof).build();
            Bytes tamperedProofBytes = BlockProof.PROTOBUF.toBytes(tamperedProof);
            items.set(
                    i,
                    BlockItemUnparsed.newBuilder()
                            .blockProof(tamperedProofBytes)
                            .build());
            break;
        }
        return new TestBlock(block.number(), new BlockUnparsed(items));
    }

    /// Creates a copy of the block with a tampered signed transaction item. Flips the first byte of the first signed
    /// transaction's raw bytes, leaving the state proof intact.
    private TestBlock tamperSignedTransaction(final TestBlock block) {
        List<BlockItemUnparsed> items = new ArrayList<>(block.blockUnparsed().blockItems());
        for (int i = 0; i < items.size(); i++) {
            BlockItemUnparsed item = items.get(i);
            if (item.item().kind() != BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION) {
                continue;
            }
            byte[] tamperedTx = item.signedTransactionOrThrow().toByteArray();
            tamperedTx[0] = (byte) ~tamperedTx[0];
            items.set(
                    i,
                    BlockItemUnparsed.newBuilder()
                            .signedTransaction(Bytes.wrap(tamperedTx))
                            .build());
            return new TestBlock(block.number(), new BlockUnparsed(items));
        }
        throw new IllegalStateException("No SIGNED_TRANSACTION found in block");
    }

    /// Creates a copy of the block with a tampered timestamp leaf in the state proof's path 0. Flips the first byte of
    /// the timestamp leaf data.
    private TestBlock tamperTimestampLeaf(final TestBlock block) throws ParseException {
        List<BlockItemUnparsed> items = new ArrayList<>(block.blockUnparsed().blockItems());
        for (int i = items.size() - 1; i >= 0; i--) {
            BlockItemUnparsed item = items.get(i);
            if (item.item().kind() != BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                continue;
            }
            BlockProof proof = BlockProof.PROTOBUF.parse(item.blockProofOrThrow());
            if (!proof.hasBlockStateProof()) {
                continue;
            }
            com.hedera.hapi.block.stream.StateProof stateProof = proof.blockStateProof();
            byte[] tamperedTimestamp = stateProof.paths().get(0).timestampLeaf().toByteArray();
            tamperedTimestamp[0] = (byte) ~tamperedTimestamp[0];

            com.hedera.hapi.block.stream.StateProof tamperedStateProof = stateProof
                    .copyBuilder()
                    .paths(List.of(
                            stateProof
                                    .paths()
                                    .get(0)
                                    .copyBuilder()
                                    .timestampLeaf(Bytes.wrap(tamperedTimestamp))
                                    .build(),
                            stateProof.paths().get(1),
                            stateProof.paths().get(2)))
                    .build();
            BlockProof tamperedProof =
                    proof.copyBuilder().blockStateProof(tamperedStateProof).build();
            Bytes tamperedProofBytes = BlockProof.PROTOBUF.toBytes(tamperedProof);
            items.set(
                    i,
                    BlockItemUnparsed.newBuilder()
                            .blockProof(tamperedProofBytes)
                            .build());
            break;
        }
        return new TestBlock(block.number(), new BlockUnparsed(items));
    }

    private static Stream<Arguments> blocksWithStateProof() throws IOException, ParseException {
        return Stream.of(
                Arguments.of(ResourceTestBlockBuilder.load(StateProof.BLOCK_1)),
                Arguments.of(ResourceTestBlockBuilder.load(StateProof.BLOCK_2)),
                Arguments.of(ResourceTestBlockBuilder.load(StateProof.BLOCK_3)),
                Arguments.of(ResourceTestBlockBuilder.load(StateProof.BLOCK_4)));
    }
}
