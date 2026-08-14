// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.harness;

import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.StreamingHasher;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.VerificationProofMetrics;
import org.hiero.block.node.verification.session.impl.ExtendedMerkleTreeSession;
import org.hiero.block.signing.TssBlockSigner;

/**
 * Legacy-verification-module twin of {@code HarnessChainBuilder}. Emits a chain of TSS-signed
 * {@link TestBlock}s whose {@code previousBlockRootHash}, {@code rootHashOfAllBlockHashesTree},
 * and {@link BlockProof} signature all line up so the legacy {@link ExtendedMerkleTreeSession}
 * accepts each block on a multi-block chain — without any {@code .blk.gz} fixture.
 *
 * <p>Uses {@link ExtendedMerkleTreeSession#processBlockItems(BlockItems)} for hash computation
 * during signing (the newer {@code block-verification} module isn't visible from this module).
 * The pre-sign hash-compute call verifies the draft block against a stub signature and returns
 * a failure notification, but its {@code blockHash()} field carries the computed root — that's
 * the value we sign.
 */
public final class LegacyHarnessChainBuilder {

    private final TssBlockSigner signer;
    private final StreamingHasher allBlocksHasher;
    private byte[] previousBlockRootHash;

    public LegacyHarnessChainBuilder(final TssBlockSigner signer) {
        this.signer = signer;
        try {
            this.allBlocksHasher = new StreamingHasher();
        } catch (final Exception e) {
            throw new IllegalStateException("SHA-384 unavailable", e);
        }
    }

    public static LegacyHarnessChainBuilder create(final TssBlockSigner signer) {
        return new LegacyHarnessChainBuilder(signer);
    }

    public record Signed(TestBlock block, Bytes rootHash) {}

    /**
     * Emits block 0 with the signer's {@link LedgerIdPublicationTransactionBody} embedded as
     * a signed transaction, so a session sees the publication during {@code processBlockItems}
     * and self-provisions the plugin's static TSS state.
     */
    public Signed genesisWithPublication() {
        final TestBlock draft =
                TestBlockBuilder.generateGenesisBlockWithSignedTransaction(signer.genesisLedgerIdSignedTransaction());
        return finalize(0L, draft);
    }

    /** Plain block at the given number with the chained footer and signed proof. */
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
        return replace(draft, LegacyHarnessChainBuilder::isFooter, footerItem);
    }

    private TestBlock withSignedProof(final TestBlock draft, final long blockNumber, final Bytes rootHash) {
        final BlockProof signedProof = signer.signBlockProof(blockNumber, rootHash);
        final BlockItemUnparsed proofItem = BlockItemUnparsed.newBuilder()
                .blockProof(BlockProof.PROTOBUF.toBytes(signedProof))
                .build();
        return replace(draft, LegacyHarnessChainBuilder::isBlockProof, proofItem);
    }

    private Bytes computeRootHash(final TestBlock block, final long blockNumber) {
        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                blockNumber, BlockSource.PUBLISHER, null, null, null, Map.of(), VerificationProofMetrics.NONE);
        final VerificationNotification notification;
        try {
            notification = session.processBlockItems(
                    new BlockItems(block.blockUnparsed().blockItems(), blockNumber, true, true));
        } catch (final ParseException e) {
            throw new IllegalStateException("failed to compute root hash", e);
        }
        return notification.blockHash();
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
     * Extracts the {@link LedgerIdPublicationTransactionBody} from the signer's genesis publication
     * so callers that need it directly (e.g. {@code VerificationServicePlugin.initializeTssParameters})
     * can bypass the block-stream self-provisioning path.
     */
    public static LedgerIdPublicationTransactionBody extractPublication(final TssBlockSigner signer)
            throws ParseException {
        final SignedTransaction signedTx =
                standardParse(SignedTransaction.PROTOBUF, signer.genesisLedgerIdSignedTransaction());
        final TransactionBody body = standardParse(TransactionBody.PROTOBUF, signedTx.bodyBytes());
        return body.ledgerIdPublicationOrThrow();
    }
}
