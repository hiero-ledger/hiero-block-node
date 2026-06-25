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
import org.junit.jupiter.params.provider.ValueSource;

/// Tests for [StateProofVerifier].
@DisplayName("State Proof Verifier Tests")
class StateProofVerifierTest {
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
    @DisplayName("verify() successful verification block 3 with 1 gap state proof")
    void testShouldVerifyBlock3With1GapStateProof() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Block 3 is an indirect proof with 1 gap block (7 siblings in path 1)
        final ResourceTestBlock block3 = ResourceTestBlockBuilder.load(StateProof.BLOCK_3);
        final HashingResult hashingResult = runHashing(verificationDataProvider, block3);
        final StateProofVerifier toTest = new StateProofVerifier(
                metricsHolder.proofVerificationMetrics(),
                block3.number(),
                hashingResult.blockProofs().getFirst().blockStateProof(),
                hashingResult.rootHash(),
                verificationDataProvider);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual).isNull();
    }

    @Test
    @DisplayName("verify() successful verification block 2 with 2 gap state proof")
    void testShouldVerifyBlock2With2GapStateProof() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Block 2 is an indirect proof with 2 gap blocks (11 siblings in path 1)
        final ResourceTestBlock block2 = ResourceTestBlockBuilder.load(StateProof.BLOCK_2);
        final HashingResult hashingResult = runHashing(verificationDataProvider, block2);
        final StateProofVerifier toTest = new StateProofVerifier(
                metricsHolder.proofVerificationMetrics(),
                block2.number(),
                hashingResult.blockProofs().getFirst().blockStateProof(),
                hashingResult.rootHash(),
                verificationDataProvider);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual).isNull();
    }

    @Test
    @DisplayName("verify() successful verification block 1 with 3 gap state proof")
    void testShouldVerifyBlock1With3GapStateProof() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Block 1 is an indirect proof with 3 gap blocks (15 siblings in path 1)
        final ResourceTestBlock block1 = ResourceTestBlockBuilder.load(StateProof.BLOCK_1);
        final HashingResult hashingResult = runHashing(verificationDataProvider, block1);
        final StateProofVerifier toTest = new StateProofVerifier(
                metricsHolder.proofVerificationMetrics(),
                block1.number(),
                hashingResult.blockProofs().getFirst().blockStateProof(),
                hashingResult.rootHash(),
                verificationDataProvider);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual).isNull();
    }

    @Test
    @DisplayName("Data Sanity Test - sibling count matches gap size")
    void testShouldVerifySiblingCountMatchesGapSize() throws IOException, ParseException {
        assertThat(extractSiblingCount(
                        ResourceTestBlockBuilder.load(StateProof.BLOCK_3).blockUnparsed()))
                .withFailMessage("1-gap proof should have 7 siblings")
                .isEqualTo(7);
        assertThat(extractSiblingCount(
                        ResourceTestBlockBuilder.load(StateProof.BLOCK_2).blockUnparsed()))
                .withFailMessage("2-gap proof should have 11 siblings")
                .isEqualTo(11);
        assertThat(extractSiblingCount(
                        ResourceTestBlockBuilder.load(StateProof.BLOCK_1).blockUnparsed()))
                .withFailMessage("3-gap proof should have 15 siblings")
                .isEqualTo(15);
    }

    @Test
    @DisplayName("verify() reject tampered sibling hash")
    void testShouldRejectTamperedSiblingHash() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Tamper with a sibling hash — the reconstructed root will differ, TSS verification must fail
        final TestBlock tamperedBlock = tamperSiblingHash(ResourceTestBlockBuilder.load(StateProof.BLOCK_3));
        final HashingResult hashingResult = runHashing(verificationDataProvider, tamperedBlock);
        final StateProofVerifier toTest = new StateProofVerifier(
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
        // Tamper with the timestamp leaf in path 0 — the signed block root will differ
        final TestBlock tamperedBlock = tamperTimestampLeaf(ResourceTestBlockBuilder.load(StateProof.BLOCK_3));
        final HashingResult hashingResult = runHashing(verificationDataProvider, tamperedBlock);
        final StateProofVerifier toTest = new StateProofVerifier(
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
                metricsHolder.proofVerificationMetrics(),
                tamperedBlock.number(),
                hashingResult.blockProofs().getFirst().blockStateProof(),
                hashingResult.rootHash(),
                verificationDataProvider);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual).isNotNull().isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    @DisplayName("verify() reject invalid sibling count")
    void testShouldRejectInvalidSiblingCount(final int siblingCount) throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // 1 and 2 fail the "< 3" guard; 4 fails the "(count - 3) % 4 == 0" guard.
        // Block 3 has 7 siblings, so truncating to 1, 2, or 4 always produces an invalid count.
        final TestBlock tamperedBlock =
                withSiblingCount(ResourceTestBlockBuilder.load(StateProof.BLOCK_3), siblingCount);
        final HashingResult hashingResult = runHashing(verificationDataProvider, tamperedBlock);
        final StateProofVerifier toTest = new StateProofVerifier(
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

    private int extractSiblingCount(BlockUnparsed block) throws ParseException {
        for (BlockItemUnparsed item : block.blockItems().reversed()) {
            if (item.item().kind() != BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                continue;
            }
            BlockProof proof = BlockProof.PROTOBUF.parse(item.blockProofOrThrow());
            if (proof.hasBlockStateProof()) {
                return proof.blockStateProof().paths().get(1).siblings().size();
            }
        }
        throw new IllegalStateException("No state proof found in block");
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

    /// Creates a copy of the block with the state proof's path 1 siblings truncated to {@code count} entries. Used to
    /// exercise the sibling-count validation in {@code verifyStateProof}.
    private TestBlock withSiblingCount(final TestBlock block, final int count) throws ParseException {
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
            List<SiblingNode> truncated =
                    new ArrayList<>(stateProof.paths().get(1).siblings().subList(0, count));
            com.hedera.hapi.block.stream.StateProof tamperedStateProof = stateProof
                    .copyBuilder()
                    .paths(List.of(
                            stateProof.paths().get(0),
                            stateProof
                                    .paths()
                                    .get(1)
                                    .copyBuilder()
                                    .siblings(truncated)
                                    .build(),
                            stateProof.paths().get(2)))
                    .build();
            BlockProof tamperedProof =
                    proof.copyBuilder().blockStateProof(tamperedStateProof).build();
            items.set(
                    i,
                    BlockItemUnparsed.newBuilder()
                            .blockProof(BlockProof.PROTOBUF.toBytes(tamperedProof))
                            .build());
            return new TestBlock(block.number(), new BlockUnparsed(items));
        }
        throw new IllegalStateException("No state proof found in block");
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
}
