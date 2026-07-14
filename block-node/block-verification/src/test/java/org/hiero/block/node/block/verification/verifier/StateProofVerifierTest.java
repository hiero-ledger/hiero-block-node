// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.MerklePath;
import com.hedera.hapi.block.stream.MerklePath.Builder;
import com.hedera.hapi.block.stream.SiblingNode;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
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
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
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
import org.junit.jupiter.params.provider.ValueSource;

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

    @Test
    @DisplayName("verify() reject when null signed proof")
    void testShouldRejectNullSignedProof() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Swap state proof with null signed proof.
        final ResourceTestBlock tamperedBlock =
                swapSignedProof(ResourceTestBlockBuilder.load(StateProof.BLOCK_3), null);
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
    @DisplayName("verify() reject when empty signed proof")
    void testShouldRejectEmptySignedProof() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Swap state proof with empty signed proof
        final TssSignedBlockProof emptySignedProof =
                TssSignedBlockProof.newBuilder().blockSignature(Bytes.EMPTY).build();
        final ResourceTestBlock tamperedBlock =
                swapSignedProof(ResourceTestBlockBuilder.load(StateProof.BLOCK_3), emptySignedProof);
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
    @DisplayName("verify() reject when tampered signed proof")
    void testShouldRejectTamperedSignedProof() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Swap state proof with tampered signed proof
        final TssSignedBlockProof emptySignedProof = TssSignedBlockProof.newBuilder()
                .blockSignature(Bytes.wrap("tampered"))
                .build();
        final ResourceTestBlock tamperedBlock =
                swapSignedProof(ResourceTestBlockBuilder.load(StateProof.BLOCK_3), emptySignedProof);
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

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("verify() reject when not enough merkle paths")
    void testShouldRejectNoMerklePaths(final int pathCount) throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Swap state proof
        final List<MerklePath> paths = new ArrayList<>();
        for (int i = 0; i < pathCount; i++) {
            paths.add(generateGenericMerklePath(true));
        }
        final ResourceTestBlock tamperedBlock = swapPaths(ResourceTestBlockBuilder.load(StateProof.BLOCK_3), paths);
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
    @DisplayName("verify() reject when first path is not a leaf")
    void testShouldRejectFirstPathNotLeaf() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Swap state proof
        final List<MerklePath> paths = new ArrayList<>();
        paths.add(generateGenericMerklePath(false));
        paths.add(generateGenericMerklePath(true));
        paths.add(generateGenericMerklePath(true));
        final ResourceTestBlock tamperedBlock = swapPaths(ResourceTestBlockBuilder.load(StateProof.BLOCK_3), paths);
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
    @DisplayName("verify() reject when a leaf has a path outside of bounds")
    void testShouldRejectWhenNextPathOfALeafIsOutsideOfBounds() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Swap state proof
        final List<MerklePath> paths = new ArrayList<>();
        paths.add(generateGenericMerklePath(true, 1_000));
        paths.add(generateGenericMerklePath(true));
        paths.add(generateGenericMerklePath(true));
        final ResourceTestBlock tamperedBlock = swapPaths(ResourceTestBlockBuilder.load(StateProof.BLOCK_3), paths);
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
    @DisplayName("verify() reject when the next path of leaf is not a join point")
    void testShouldRejectWhenNextPathOfALeafIsNotAJoinPoint() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        // Swap state proof
        final List<MerklePath> paths = new ArrayList<>();
        paths.add(generateGenericMerklePath(true, 1));
        paths.add(generateGenericMerklePath(true, 2));
        paths.add(generateGenericMerklePath(true));
        final ResourceTestBlock tamperedBlock = swapPaths(ResourceTestBlockBuilder.load(StateProof.BLOCK_3), paths);
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
    @DisplayName("verify() reject if canceled")
    void testShouldRejectIfCanceled() throws IOException, ParseException {
        initializeTssData(verificationDataProvider, ResourceTestBlockBuilder.load(StateProof.BLOCK_0));
        final ResourceTestBlock block3 = ResourceTestBlockBuilder.load(StateProof.BLOCK_3);
        final HashingResult hashingResult = runHashing(verificationDataProvider, block3);
        final StateProofVerifier toTest = new StateProofVerifier(
                isCanceled,
                metricsHolder.proofVerificationMetrics(),
                block3.number(),
                hashingResult.blockProofs().getFirst().blockStateProof(),
                hashingResult.rootHash(),
                verificationDataProvider);
        isCanceled.set(true);
        final SessionFailureType actual = toTest.verify();
        assertThat(actual).isNotNull().isEqualTo(SessionFailureType.CANCELLED);
    }

    private MerklePath generateGenericMerklePath(final boolean isLeaf) {
        return generateGenericMerklePath(isLeaf, -1);
    }

    private MerklePath generateGenericMerklePath(final boolean isLeaf, final int nextPathIndex) {
        final Builder builder = MerklePath.newBuilder();
        builder.siblings(List.of());
        builder.nextPathIndex(nextPathIndex);
        if (isLeaf) {
            builder.hash(Bytes.wrap(new byte[32]));
        }
        return builder.build();
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

    private ResourceTestBlock swapSignedProof(final ResourceTestBlock block, final TssSignedBlockProof signedProof) {
        assertThat(block.proofs()).size().isEqualTo(1);
        final BlockProof proof = block.proofs().getFirst();
        final com.hedera.hapi.block.stream.StateProof stateProof = proof.blockStateProof();
        assertThat(stateProof).isNotNull();
        final com.hedera.hapi.block.stream.StateProof.Builder builder =
                com.hedera.hapi.block.stream.StateProof.newBuilder().paths(stateProof.paths());
        if (signedProof != null) {
            builder.signedBlockProof(signedProof);
        }
        final com.hedera.hapi.block.stream.StateProof toSwap = builder.build();
        final List<BlockItem> proofsExcluded =
                block.asBlockItemFiltered(i -> i.item().kind() != BlockItem.ItemOneOfType.BLOCK_PROOF);
        final ArrayList<BlockItem> resultItems = new ArrayList<>(proofsExcluded);
        final BlockItem swappedProof = BlockItem.newBuilder()
                .blockProof(BlockProof.newBuilder().blockStateProof(toSwap).build())
                .build();
        resultItems.add(swappedProof);
        return new ResourceTestBlock(
                block.number(),
                new BlockUnparsed(TestBlockBuilder.convertToUnparsedItems(resultItems)),
                block.blockRootHash());
    }

    private ResourceTestBlock swapPaths(final ResourceTestBlock block, final List<MerklePath> paths) {
        assertThat(block.proofs()).size().isEqualTo(1);
        final BlockProof proof = block.proofs().getFirst();
        final com.hedera.hapi.block.stream.StateProof stateProof = proof.blockStateProof();
        assertThat(stateProof).isNotNull();
        final com.hedera.hapi.block.stream.StateProof toSwap = com.hedera.hapi.block.stream.StateProof.newBuilder()
                .paths(paths)
                .signedBlockProof(stateProof.signedBlockProof())
                .build();
        final List<BlockItem> proofsExcluded =
                block.asBlockItemFiltered(i -> i.item().kind() != BlockItem.ItemOneOfType.BLOCK_PROOF);
        final ArrayList<BlockItem> resultItems = new ArrayList<>(proofsExcluded);
        final BlockItem swappedProof = BlockItem.newBuilder()
                .blockProof(BlockProof.newBuilder().blockStateProof(toSwap).build())
                .build();
        resultItems.add(swappedProof);
        return new ResourceTestBlock(
                block.number(),
                new BlockUnparsed(TestBlockBuilder.convertToUnparsedItems(resultItems)),
                block.blockRootHash());
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
