// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.harness;

import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.pbj.runtime.ParseException;
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
 * Builds a chain of TSS-signed {@link TestBlock}s that pass end-to-end verification
 * against the real {@link org.hiero.block.node.block.verification.verifier.TSSVerifier}.
 *
 * <p>Each call to {@link #next(long)} — or {@link #genesisWithPublication()} for block 0 —
 * emits a block whose {@code previousBlockRootHash} chains to the previous emission,
 * whose {@code rootHashOfAllBlockHashesTree} matches the running all-previous-blocks tree,
 * and whose {@link BlockProof} carries a real signature over the computed block root hash.
 *
 * <p>Not thread safe. Reusing the same builder across tests is fine, but tests that expect
 * a fresh chain state should construct a new builder in {@code @BeforeEach}.
 */
public final class HarnessChainBuilder {

    private final TssBlockSigner signer;
    private final VerificationDataProvider verificationDataProvider;
    private final MetricsHolder metricsHolder;
    private final StreamingHasher allBlocksHasher;
    private byte[] previousBlockRootHash;

    public HarnessChainBuilder(
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

    /**
     * Factory that constructs a {@link HarnessChainBuilder} with its own isolated
     * {@link org.hiero.metrics.core.MetricRegistry} — safe to use inside plugin-based tests where
     * the plugin's shared registry already holds the block-verification metric names and would
     * throw on a second {@link MetricsHolder#create} against it.
     *
     * <p>The internal {@link VerificationDataProvider} is unprovisioned. It's used only by the
     * harness's local {@link BlockHasher} for root-hash computation during signing; the plugin
     * under test manages its own {@code VerificationDataProvider} and self-provisions from the
     * genesis-block {@link com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody}.
     */
    public static HarnessChainBuilder create(final TssBlockSigner signer) {
        final org.hiero.block.node.spi.BlockNodeContext isolated =
                org.hiero.block.node.app.fixtures.TestUtils.testContext(
                        new org.hiero.block.node.app.fixtures.TestConfigurationBuilder().getOrCreateConfig(),
                        new org.hiero.block.node.app.fixtures.async.TestThreadPoolManager<>(
                                new org.hiero.block.node.app.fixtures.async.BlockingExecutor(
                                        new java.util.concurrent.LinkedBlockingQueue<>()),
                                new org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor(
                                        new java.util.concurrent.LinkedBlockingQueue<>())));
        return new HarnessChainBuilder(
                signer, new VerificationDataProvider(isolated), MetricsHolder.create(isolated.metricRegistry()));
    }

    /**
     * A block emitted by the harness together with the computed root hash the signature covers.
     * The root hash is what the verifier's {@code VerificationNotification.blockHash()} will report
     * for this block; tests that assert on that field should compare against it.
     */
    public record Signed(TestBlock block, Bytes rootHash) {}

    /**
     * Emits block 0 with the signer's {@link com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody}
     * embedded as a signed transaction, so the verifier self-provisions TSS parameters from the block
     * stream (rather than needing a pre-written tss-parameters.bin bootstrap file). Advances chain state.
     */
    public Signed genesisWithPublication() {
        final TestBlock draft =
                TestBlockBuilder.generateGenesisBlockWithSignedTransaction(signer.genesisLedgerIdSignedTransaction());
        return finalize(0L, draft);
    }

    /**
     * Emits a plain block at the given block number with the chained footer and signed proof.
     * Advances chain state.
     */
    public Signed next(final long blockNumber) {
        return finalize(blockNumber, TestBlockBuilder.generateBlockWithNumber(blockNumber));
    }

    private Signed finalize(final long blockNumber, final TestBlock draft) {
        final TestBlock chained = withChainedFooter(draft);
        final Bytes rootHash = computeRootHash(chained, blockNumber);
        final TestBlock signed = withSignedProof(chained, blockNumber, rootHash);
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
        return replace(draft, HarnessChainBuilder::isFooter, footerItem);
    }

    private TestBlock withSignedProof(final TestBlock draft, final long blockNumber, final Bytes rootHash) {
        final BlockProof signedProof = signer.signBlockProof(blockNumber, rootHash);
        final BlockItemUnparsed proofItem = BlockItemUnparsed.newBuilder()
                .blockProof(BlockProof.PROTOBUF.toBytes(signedProof))
                .build();
        return replace(draft, HarnessChainBuilder::isBlockProof, proofItem);
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

    /**
     * Extracts the {@link com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody} from the
     * signer's genesis publication so callers that need it directly (e.g. legacy
     * {@code VerificationServicePlugin.initializeTssParameters}) can pass it through.
     */
    public static com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody extractPublication(
            final TssBlockSigner signer) throws ParseException {
        final com.hedera.hapi.node.transaction.SignedTransaction signedTx = standardParse(
                com.hedera.hapi.node.transaction.SignedTransaction.PROTOBUF, signer.genesisLedgerIdSignedTransaction());
        final com.hedera.hapi.node.transaction.TransactionBody body =
                standardParse(com.hedera.hapi.node.transaction.TransactionBody.PROTOBUF, signedTx.bodyBytes());
        return body.ledgerIdPublicationOrThrow();
    }
}
