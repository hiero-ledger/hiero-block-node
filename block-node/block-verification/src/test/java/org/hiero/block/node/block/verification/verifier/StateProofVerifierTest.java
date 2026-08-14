// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.MerklePath;
import com.hedera.hapi.block.stream.MerklePath.Builder;
import com.hedera.hapi.block.stream.SiblingNode;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.pbj.runtime.ParseException;
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
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.harness.StateProofChainBuilder;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// Tests for [StateProofVerifier] using the harness-generated indirect state proofs.
@DisplayName("State Proof Verifier Tests")
class StateProofVerifierTest {
    private AtomicBoolean isCanceled;
    private MetricsHolder metricsHolder;
    private VerificationDataProvider verificationDataProvider;
    private StateProofChainBuilder builder;

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
        final TssBlockSigner signer = TssBlockSigner.create();
        verificationDataProvider.safeUpdateTssData(signer.verificationMaterial().tssData(), false);
        builder = new StateProofChainBuilder(signer, verificationDataProvider, metricsHolder);
    }

    @ParameterizedTest
    @ValueSource(longs = {1L, 2L, 3L, 4L})
    @DisplayName("verify() passes for valid state proof")
    void testPassingStateProofVerification(final long blockNumber) {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed signed = advanceTo(blockNumber);
        assertVerifies(signed);
    }

    @Test
    @DisplayName("verify() reject tampered sibling hash")
    void testShouldRejectTamperedSiblingHash() throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final TestBlock tampered = tamperSiblingHash(original.block());
        assertRejects(tampered);
    }

    @Test
    @DisplayName("verify() reject tampered timestamp leaf")
    void testShouldRejectTamperedTimestampLeaf() throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final TestBlock tampered = tamperTimestampLeaf(original.block());
        assertRejects(tampered);
    }

    @Test
    @DisplayName("verify() reject tampered block content with valid state proof")
    void testShouldRejectTamperedBlockContentWithValidStateProof() {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final TestBlock tampered = tamperSignedTransaction(original.block());
        assertRejects(tampered);
    }

    @Test
    @DisplayName("verify() reject when null signed proof")
    void testShouldRejectNullSignedProof() throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final TestBlock tampered = swapSignedProof(original.block(), null);
        assertRejects(tampered);
    }

    @Test
    @DisplayName("verify() reject when empty signed proof")
    void testShouldRejectEmptySignedProof() throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final TssSignedBlockProof emptySigned =
                TssSignedBlockProof.newBuilder().blockSignature(Bytes.EMPTY).build();
        final TestBlock tampered = swapSignedProof(original.block(), emptySigned);
        assertRejects(tampered);
    }

    @Test
    @DisplayName("verify() reject when tampered signed proof")
    void testShouldRejectTamperedSignedProof() throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final TssSignedBlockProof tamperedSigned = TssSignedBlockProof.newBuilder()
                .blockSignature(Bytes.wrap("tampered"))
                .build();
        final TestBlock tampered = swapSignedProof(original.block(), tamperedSigned);
        assertRejects(tampered);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("verify() reject when not enough merkle paths")
    void testShouldRejectNoMerklePaths(final int pathCount) throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final List<MerklePath> paths = new ArrayList<>();
        for (int i = 0; i < pathCount; i++) {
            paths.add(generateGenericMerklePath(true));
        }
        final TestBlock tampered = swapPaths(original.block(), paths);
        assertRejects(tampered);
    }

    @Test
    @DisplayName("verify() passes for valid state proof that does not start with a leaf")
    void testPassingStateProofWithNonLeafFirstPath() throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        // Reorder [leaf, leaf, join] → [join, leaf, leaf] with next indices remapped to the join's new position.
        final List<MerklePath> pathsOriginal = statePaths(original.block());
        final MerklePath join = pathsOriginal.get(2);
        final MerklePath firstLeaf =
                pathsOriginal.get(0).copyBuilder().nextPathIndex(0).build();
        final MerklePath secondLeaf =
                pathsOriginal.get(1).copyBuilder().nextPathIndex(0).build();
        final TestBlock reordered = swapPaths(original.block(), List.of(join, firstLeaf, secondLeaf));
        // Path reorder doesn't change the block content so the computed root hash is unchanged.
        assertVerifies(new StateProofChainBuilder.Signed(reordered, original.rootHash()));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("verify() reject valid state proof carrying a join point that is never followed")
    void testShouldRejectJoinPointNeverFollowed(final int orphanIndex) throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final List<MerklePath> pathsOriginal = statePaths(original.block());
        final List<MerklePath> paths = new ArrayList<>();
        paths.add(pathsOriginal.get(0).copyBuilder().nextPathIndex(3).build());
        paths.add(pathsOriginal.get(1).copyBuilder().nextPathIndex(3).build());
        paths.add(pathsOriginal.get(2));
        paths.add(orphanIndex, generateGenericMerklePath(false));
        final TestBlock tampered = swapPaths(original.block(), paths);
        assertRejects(tampered);
    }

    @Test
    @DisplayName("verify() reject when no leaf leads to a join point")
    void testShouldRejectUnreachableJoinPoint() throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final List<MerklePath> paths = new ArrayList<>();
        paths.add(generateGenericMerklePath(false));
        paths.add(generateGenericMerklePath(true));
        paths.add(generateGenericMerklePath(true));
        final TestBlock tampered = swapPaths(original.block(), paths);
        assertRejects(tampered);
    }

    @Test
    @DisplayName("verify() reject when a leaf has a path outside of bounds")
    void testShouldRejectWhenNextPathOfALeafIsOutsideOfBounds() throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final List<MerklePath> paths = new ArrayList<>();
        paths.add(generateGenericMerklePath(true, 1_000));
        paths.add(generateGenericMerklePath(true));
        paths.add(generateGenericMerklePath(true));
        final TestBlock tampered = swapPaths(original.block(), paths);
        assertRejects(tampered);
    }

    @Test
    @DisplayName("verify() reject when the next path of leaf is not a join point")
    void testShouldRejectWhenNextPathOfALeafIsNotAJoinPoint() throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final List<MerklePath> paths = new ArrayList<>();
        paths.add(generateGenericMerklePath(true, 1));
        paths.add(generateGenericMerklePath(true, 2));
        paths.add(generateGenericMerklePath(true));
        final TestBlock tampered = swapPaths(original.block(), paths);
        assertRejects(tampered);
    }

    @Test
    @DisplayName("verify() reject if canceled")
    void testShouldRejectIfCanceled() {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed signed = advanceTo(3L);
        final HashingResult hashing = runHashing(signed.block());
        final StateProofVerifier toTest = new StateProofVerifier(
                isCanceled,
                metricsHolder.proofVerificationMetrics(),
                signed.block().number(),
                hashing.blockProofs().getFirst().blockStateProof(),
                hashing.rootHash(),
                verificationDataProvider);
        isCanceled.set(true);
        assertThat(toTest.verify()).isEqualTo(SessionFailureType.CANCELLED);
    }

    @Test
    @DisplayName("verify() reject when not all paths visited")
    void testShouldRejectWhenNotAllPathsVisited() throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final List<MerklePath> paths = new ArrayList<>();
        paths.add(generateGenericMerklePath(true, 2));
        paths.add(generateGenericMerklePath(true, 2));
        paths.add(generateGenericMerklePath(false, -1));
        paths.add(generateGenericMerklePath(true, -1));
        final TestBlock tampered = swapPaths(original.block(), paths);
        assertRejects(tampered);
    }

    @Test
    @DisplayName("verify() reject when checkpoints left after visiting all indices")
    void testShouldRejectWhenCheckpointsLeftAfterVisitingAllIndices() throws ParseException {
        builder.genesisWithPublication();
        final StateProofChainBuilder.Signed original = advanceTo(3L);
        final List<MerklePath> paths = new ArrayList<>();
        paths.add(generateGenericMerklePath(true, 2));
        paths.add(generateGenericMerklePath(true, 2));
        paths.add(generateGenericMerklePath(false, 3));
        paths.add(generateGenericMerklePath(false, -1));
        final TestBlock tampered = swapPaths(original.block(), paths);
        assertRejects(tampered);
    }

    /// Emits blocks 1..blockNumber through the builder, returning the last signed one.
    private StateProofChainBuilder.Signed advanceTo(final long blockNumber) {
        StateProofChainBuilder.Signed signed = null;
        for (long n = 1L; n <= blockNumber; n++) {
            signed = builder.next(n);
        }
        return signed;
    }

    private void assertVerifies(final StateProofChainBuilder.Signed signed) {
        final HashingResult hashing = runHashing(signed.block());
        final StateProofVerifier toTest = new StateProofVerifier(
                isCanceled,
                metricsHolder.proofVerificationMetrics(),
                signed.block().number(),
                hashing.blockProofs().getFirst().blockStateProof(),
                hashing.rootHash(),
                verificationDataProvider);
        assertThat(toTest.verify()).isNull();
    }

    private void assertRejects(final TestBlock tampered) {
        final HashingResult hashing = runHashing(tampered);
        final StateProofVerifier toTest = new StateProofVerifier(
                isCanceled,
                metricsHolder.proofVerificationMetrics(),
                tampered.number(),
                hashing.blockProofs().getFirst().blockStateProof(),
                hashing.rootHash(),
                verificationDataProvider);
        assertThat(toTest.verify()).isNotNull().isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
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

    private MerklePath generateGenericMerklePath(final boolean isLeaf) {
        return generateGenericMerklePath(isLeaf, -1);
    }

    private MerklePath generateGenericMerklePath(final boolean isLeaf, final int nextPathIndex) {
        final Builder b = MerklePath.newBuilder();
        b.siblings(List.of());
        b.nextPathIndex(nextPathIndex);
        if (isLeaf) {
            b.hash(Bytes.wrap(new byte[32]));
        }
        return b.build();
    }

    private static List<MerklePath> statePaths(final TestBlock block) throws ParseException {
        final BlockProof proof = extractProof(block);
        final com.hedera.hapi.block.stream.StateProof sp = proof.blockStateProof();
        assertThat(sp).isNotNull();
        final List<MerklePath> paths = sp.paths();
        assertThat(paths).hasSize(3);
        return paths;
    }

    private static BlockProof extractProof(final TestBlock block) throws ParseException {
        for (final BlockItemUnparsed item : block.blockUnparsed().blockItems()) {
            if (item.item().kind() == BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                return BlockProof.PROTOBUF.parse(item.blockProofOrThrow());
            }
        }
        throw new IllegalStateException("no BlockProof item");
    }

    private static TestBlock swapSignedProof(final TestBlock block, final TssSignedBlockProof signedProof)
            throws ParseException {
        final BlockProof original = extractProof(block);
        final com.hedera.hapi.block.stream.StateProof sp = original.blockStateProof();
        final com.hedera.hapi.block.stream.StateProof.Builder spBuilder =
                com.hedera.hapi.block.stream.StateProof.newBuilder().paths(sp.paths());
        if (signedProof != null) {
            spBuilder.signedBlockProof(signedProof);
        }
        return replaceProof(
                block, original.copyBuilder().blockStateProof(spBuilder.build()).build());
    }

    private static TestBlock swapPaths(final TestBlock block, final List<MerklePath> paths) throws ParseException {
        final BlockProof original = extractProof(block);
        final com.hedera.hapi.block.stream.StateProof sp = original.blockStateProof();
        final com.hedera.hapi.block.stream.StateProof swapped = com.hedera.hapi.block.stream.StateProof.newBuilder()
                .paths(paths)
                .signedBlockProof(sp.signedBlockProof())
                .build();
        return replaceProof(
                block, original.copyBuilder().blockStateProof(swapped).build());
    }

    private static TestBlock tamperSiblingHash(final TestBlock block) throws ParseException {
        final BlockProof original = extractProof(block);
        final com.hedera.hapi.block.stream.StateProof sp = original.blockStateProof();
        final List<SiblingNode> siblings = new ArrayList<>(sp.paths().get(1).siblings());
        final SiblingNode s0 = siblings.get(0);
        final byte[] tampered = s0.hash().toByteArray();
        tampered[0] = (byte) ~tampered[0];
        siblings.set(0, new SiblingNode(s0.isLeft(), Bytes.wrap(tampered)));
        final com.hedera.hapi.block.stream.StateProof updated = sp.copyBuilder()
                .paths(List.of(
                        sp.paths().get(0),
                        sp.paths().get(1).copyBuilder().siblings(siblings).build(),
                        sp.paths().get(2)))
                .build();
        return replaceProof(
                block, original.copyBuilder().blockStateProof(updated).build());
    }

    private static TestBlock tamperTimestampLeaf(final TestBlock block) throws ParseException {
        final BlockProof original = extractProof(block);
        final com.hedera.hapi.block.stream.StateProof sp = original.blockStateProof();
        final byte[] tampered = sp.paths().get(0).timestampLeaf().toByteArray();
        tampered[0] = (byte) ~tampered[0];
        final com.hedera.hapi.block.stream.StateProof updated = sp.copyBuilder()
                .paths(List.of(
                        sp.paths()
                                .get(0)
                                .copyBuilder()
                                .timestampLeaf(Bytes.wrap(tampered))
                                .build(),
                        sp.paths().get(1),
                        sp.paths().get(2)))
                .build();
        return replaceProof(
                block, original.copyBuilder().blockStateProof(updated).build());
    }

    private static TestBlock tamperSignedTransaction(final TestBlock block) {
        final List<BlockItemUnparsed> items =
                new ArrayList<>(block.blockUnparsed().blockItems());
        for (int i = 0; i < items.size(); i++) {
            final BlockItemUnparsed item = items.get(i);
            if (item.item().kind() != BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION) {
                continue;
            }
            final byte[] tamperedTx = item.signedTransactionOrThrow().toByteArray();
            tamperedTx[0] = (byte) ~tamperedTx[0];
            items.set(
                    i,
                    BlockItemUnparsed.newBuilder()
                            .signedTransaction(Bytes.wrap(tamperedTx))
                            .build());
            return new TestBlock(
                    block.number(), BlockUnparsed.newBuilder().blockItems(items).build());
        }
        throw new IllegalStateException("No SIGNED_TRANSACTION found in block");
    }

    private static TestBlock replaceProof(final TestBlock block, final BlockProof newProof) {
        final List<BlockItemUnparsed> items = new ArrayList<>();
        boolean replaced = false;
        for (final BlockItemUnparsed item : block.blockUnparsed().blockItems()) {
            if (!replaced && item.item().kind() == BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                items.add(BlockItemUnparsed.newBuilder()
                        .blockProof(BlockProof.PROTOBUF.toBytes(newProof))
                        .build());
                replaced = true;
            } else {
                items.add(item);
            }
        }
        return new TestBlock(
                block.number(), BlockUnparsed.newBuilder().blockItems(items).build());
    }
}
