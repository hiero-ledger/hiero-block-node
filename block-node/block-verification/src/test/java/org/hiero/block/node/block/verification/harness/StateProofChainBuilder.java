// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.harness;

import static org.hiero.block.common.hasher.HashingUtilities.hashInternalNode;
import static org.hiero.block.common.hasher.HashingUtilities.hashLeaf;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.MerklePath;
import com.hedera.hapi.block.stream.SiblingNode;
import com.hedera.hapi.block.stream.StateProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.StreamingHasher;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.hasher.BlockHasher;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.signing.TssBlockSigner;

/**
 * Builds a chain of {@link TestBlock}s where each block is verified indirectly via a
 * {@link StateProof} that reconstructs a signed root hash from a 3-path merkle chain and a
 * {@link TssSignedBlockProof} signing that reconstructed root.
 *
 * <p>Path structure emitted per gap block matches what {@code CN_11_12_TSS_SCHNORR} fixtures
 * carry (the {@code statePaths} contract enforced by {@code StateProofVerifierTest}):
 * <ol>
 *   <li><b>path[0]</b> — {@code TIMESTAMP_LEAF} content plus one right sibling, {@code nextPathIndex=2}</li>
 *   <li><b>path[1]</b> — {@code HASH} content (equal to the gap block's computed root) plus one left
 *       sibling, {@code nextPathIndex=2}</li>
 *   <li><b>path[2]</b> — join point (no content, no siblings), {@code nextPathIndex=-1}</li>
 * </ol>
 *
 * <p>The reconstructed signed root is
 * {@code hashInternalNode(mergeSiblings(hashLeaf(timestamp), path0Siblings),
 *                         mergeSiblings(gapBlockRoot,             path1Siblings))}
 * — the harness computes it, signs it, and drops the signature into the state proof.
 */
public final class StateProofChainBuilder {

    private final TssBlockSigner signer;
    private final VerificationDataProvider verificationDataProvider;
    private final MetricsHolder metricsHolder;
    private final StreamingHasher allBlocksHasher;
    private byte[] previousBlockRootHash;

    public StateProofChainBuilder(
            final TssBlockSigner signer,
            final VerificationDataProvider verificationDataProvider,
            final MetricsHolder metricsHolder) {
        this.signer = signer;
        this.verificationDataProvider = verificationDataProvider;
        this.metricsHolder = metricsHolder;
        try {
            this.allBlocksHasher = new StreamingHasher();
        } catch (final Exception e) {
            throw new IllegalStateException("SHA-384 unavailable", e);
        }
    }

    /** Factory with an isolated MetricRegistry, safe to call inside plugin-based tests. */
    public static StateProofChainBuilder create(final TssBlockSigner signer) {
        final org.hiero.block.node.spi.BlockNodeContext isolated =
                org.hiero.block.node.app.fixtures.TestUtils.testContext(
                        new org.hiero.block.node.app.fixtures.TestConfigurationBuilder().getOrCreateConfig(),
                        new org.hiero.block.node.app.fixtures.async.TestThreadPoolManager<>(
                                new org.hiero.block.node.app.fixtures.async.BlockingExecutor(
                                        new java.util.concurrent.LinkedBlockingQueue<>()),
                                new org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor(
                                        new java.util.concurrent.LinkedBlockingQueue<>())));
        return new StateProofChainBuilder(
                signer, new VerificationDataProvider(isolated), MetricsHolder.create(isolated.metricRegistry()));
    }

    /** A gap block carrying a valid indirect state proof plus its computed root hash. */
    public record Signed(TestBlock block, Bytes rootHash) {}

    /**
     * Emits a genesis (block 0) gap block with the signer's LedgerIdPublication in a signed
     * transaction (so the verifier self-provisions TSS state) and an indirect state proof.
     */
    public Signed genesisWithPublication() {
        final TestBlock draft =
                TestBlockBuilder.generateGenesisBlockWithSignedTransaction(signer.genesisLedgerIdSignedTransaction());
        return finalize(0L, draft);
    }

    /**
     * Emits a gap block at the given block number with an indirect state proof. Every gap block
     * carries a dummy signed-transaction item so tampering tests that mutate SIGNED_TRANSACTION
     * always have something to modify.
     */
    public Signed next(final long blockNumber) {
        return finalize(
                blockNumber,
                TestBlockBuilder.generateBlockWithSignedTransaction(blockNumber, Bytes.wrap(new byte[16])));
    }

    private Signed finalize(final long blockNumber, final TestBlock draft) {
        final TestBlock chained = withChainedFooter(draft);
        final Bytes rootHash = computeRootHash(chained, blockNumber);
        final TestBlock signed = withStateProof(chained, blockNumber, rootHash);
        previousBlockRootHash = rootHash.toByteArray();
        allBlocksHasher.addNodeByHash(previousBlockRootHash);
        return new Signed(signed, rootHash);
    }

    private TestBlock withChainedFooter(final TestBlock draft) {
        final byte[] prev = previousBlockRootHash != null ? previousBlockRootHash : HashingUtilities.EMPTY_TREE_HASH;
        final byte[] allBlocksRoot =
                allBlocksHasher.leafCount() > 0 ? allBlocksHasher.computeRootHash() : HashingUtilities.EMPTY_TREE_HASH;
        final BlockFooter footer = BlockFooter.newBuilder()
                .previousBlockRootHash(Bytes.wrap(prev))
                .rootHashOfAllBlockHashesTree(Bytes.wrap(allBlocksRoot))
                .startOfBlockStateRootHash(Bytes.wrap(HashingUtilities.EMPTY_TREE_HASH))
                .build();
        final BlockItemUnparsed footerItem = BlockItemUnparsed.newBuilder()
                .blockFooter(BlockFooter.PROTOBUF.toBytes(footer))
                .build();
        return replace(draft, StateProofChainBuilder::isFooter, footerItem);
    }

    private TestBlock withStateProof(final TestBlock draft, final long blockNumber, final Bytes rootHash) {
        // Path 0: TIMESTAMP_LEAF + one right sibling. Deterministic zero-filled placeholders.
        final byte[] timestampBytes = new byte[48];
        final byte[] path0SiblingHash = new byte[48];
        // Path 1: HASH == gap block root + one left sibling.
        final byte[] path1SiblingHash = new byte[48];

        final SiblingNode path0Sibling = new SiblingNode(false, Bytes.wrap(path0SiblingHash));
        final SiblingNode path1Sibling = new SiblingNode(true, Bytes.wrap(path1SiblingHash));

        // Reconstruct the same signed root the verifier would compute.
        // result0 = combineSibling(hashLeaf(timestamp), path0Sibling) — right sibling → parent = hash(content, sibling)
        final byte[] leaf0Content = hashLeaf(timestampBytes);
        final byte[] result0 =
                hashInternalNode(leaf0Content, path0Sibling.hash().toByteArray());
        // result1 = combineSibling(gapBlockRoot, path1Sibling) — left sibling → parent = hash(sibling, content)
        final byte[] leaf1Content = rootHash.toByteArray();
        final byte[] result1 = hashInternalNode(path1Sibling.hash().toByteArray(), leaf1Content);
        // Path 2 (join) has no siblings, so the reconstructed signed root is hashInternalNode(result0, result1).
        final byte[] signedRoot = hashInternalNode(result0, result1);

        final BlockProof signedProof = signer.signBlockProof(blockNumber, Bytes.wrap(signedRoot));
        final TssSignedBlockProof tssSigned = signedProof.signedBlockProof();

        final StateProof stateProof = StateProof.newBuilder()
                .paths(List.of(
                        MerklePath.newBuilder()
                                .timestampLeaf(Bytes.wrap(timestampBytes))
                                .siblings(List.of(path0Sibling))
                                .nextPathIndex(2)
                                .build(),
                        MerklePath.newBuilder()
                                .hash(rootHash)
                                .siblings(List.of(path1Sibling))
                                .nextPathIndex(2)
                                .build(),
                        MerklePath.newBuilder()
                                .siblings(List.of())
                                .nextPathIndex(-1)
                                .build()))
                .signedBlockProof(tssSigned)
                .build();

        final BlockProof indirectProof = BlockProof.newBuilder()
                .block(blockNumber)
                .blockStateProof(stateProof)
                .build();
        final BlockItemUnparsed proofItem = BlockItemUnparsed.newBuilder()
                .blockProof(BlockProof.PROTOBUF.toBytes(indirectProof))
                .build();
        return replace(draft, StateProofChainBuilder::isBlockProof, proofItem);
    }

    private Bytes computeRootHash(final TestBlock block, final long blockNumber) {
        final ConcurrentLinkedDeque<BlockItems> deque = new ConcurrentLinkedDeque<>();
        final BlockHasher hasher = new BlockHasher(
                new AtomicBoolean(false),
                deque,
                metricsHolder.hashingMetrics(),
                blockNumber,
                BlockSource.PUBLISHER,
                verificationDataProvider);
        deque.add(block.asBlockItems());
        return hasher.get().rootHash();
    }

    private static TestBlock replace(
            final TestBlock draft,
            final java.util.function.Predicate<BlockItemUnparsed> predicate,
            final BlockItemUnparsed replacement) {
        final List<BlockItemUnparsed> items = new ArrayList<>();
        boolean substituted = false;
        for (final BlockItemUnparsed item : draft.blockUnparsed().blockItems()) {
            if (!substituted && predicate.test(item)) {
                items.add(replacement);
                substituted = true;
            } else {
                items.add(item);
            }
        }
        return new TestBlock(
                draft.number(), BlockUnparsed.newBuilder().blockItems(items).build());
    }

    private static boolean isFooter(final BlockItemUnparsed item) {
        return item.item().kind() == BlockItemUnparsed.ItemOneOfType.BLOCK_FOOTER;
    }

    private static boolean isBlockProof(final BlockItemUnparsed item) {
        return item.item().kind() == BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF;
    }
}
