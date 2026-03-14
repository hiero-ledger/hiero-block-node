// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.common.hasher.HashingUtilities.getBlockItemHash;
import static org.hiero.block.common.hasher.HashingUtilities.noThrowSha384HashOf;

import com.hedera.cryptography.tss.TSS;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.NaiveStreamingTreeHasher;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.verification.VerificationServicePlugin;
import org.hiero.block.node.verification.session.VerificationSession;

public class ExtendedMerkleTreeSession implements VerificationSession {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** Byte length of a legacy SHA384 signature (non-TSS blocks). */
    private static final int HASH_LENGTH = 48;

    private final long blockNumber;
    // Stream Hashers
    /** The tree hasher for input hashes. */
    private final StreamingTreeHasher inputTreeHasher;
    /** The tree hasher for output hashes. */
    private final StreamingTreeHasher outputTreeHasher;
    /** The tree hasher for consensus header hashes. */
    private final StreamingTreeHasher consensusHeaderHasher;
    /** The tree hasher for state changes hashes. */
    private final StreamingTreeHasher stateChangesHasher;
    /** The tree hasher for trace data hashes. */
    private final StreamingTreeHasher traceDataHasher;
    /** The source of the block, used to construct the final notification. */
    private final BlockSource blockSource;

    /**
     * The block items for the block this session is responsible for. We collect them here so we can provide the
     * complete block in the final notification.
     */
    protected final List<BlockItemUnparsed> blockItems = new ArrayList<>();

    private BlockHeader blockHeader = null;

    private BlockFooter blockFooter = null;

    private List<BlockProof> blockProofs = new ArrayList<>();

    private final Bytes previousBlockHash;

    private final Bytes allPreviousBlockRootHash;

    /**
     * Trusted ledger ID for TSS verification. Passed from the plugin (may be null for block 0).
     * For block 0, this is set internally when LedgerIdPublicationTransactionBody is found.
     */
    private Bytes ledgerId;

    /**
     * Constructor for ExtendedMerkleTreeSession.
     *
     * @param blockNumber the block number
     * @param blockSource the source of the block
     * @param previousBlockHash the previous block hash, may be null
     * @param allPreviousBlocksRootHash the all previous blocks root hash, may be null
     * @param ledgerId the trusted ledger ID for TSS verification, may be null for block 0
     */
    public ExtendedMerkleTreeSession(
            final long blockNumber,
            final BlockSource blockSource,
            final Bytes previousBlockHash,
            final Bytes allPreviousBlocksRootHash,
            final Bytes ledgerId) {
        this.blockNumber = blockNumber;
        this.previousBlockHash = previousBlockHash;
        this.allPreviousBlockRootHash = allPreviousBlocksRootHash;
        this.ledgerId = ledgerId;
        // using NaiveStreamingTreeHasher as we should only need single threaded
        this.inputTreeHasher = new NaiveStreamingTreeHasher();
        this.outputTreeHasher = new NaiveStreamingTreeHasher();
        this.consensusHeaderHasher = new NaiveStreamingTreeHasher();
        this.stateChangesHasher = new NaiveStreamingTreeHasher();
        this.traceDataHasher = new NaiveStreamingTreeHasher();
        this.blockSource = Objects.requireNonNull(blockSource, "BlockSource must not be null");
        LOGGER.log(INFO, "Created ExtendedMerkleTreeSession for block {0}", blockNumber);
    }

    @Override
    public VerificationNotification processBlockItems(BlockItems blockItemsMessage) throws ParseException {
        List<BlockItemUnparsed> blockItems = blockItemsMessage.blockItems();

        this.blockItems.addAll(blockItems);

        for (BlockItemUnparsed item : blockItems) {
            final BlockItemUnparsed.ItemOneOfType kind = item.item().kind();
            switch (kind) {
                case BLOCK_HEADER -> {
                    this.blockHeader = BlockHeader.PROTOBUF.parse(item.blockHeader());
                    outputTreeHasher.addLeaf(getBlockItemHash(item));
                }
                case ROUND_HEADER, EVENT_HEADER -> consensusHeaderHasher.addLeaf(getBlockItemHash(item));
                case SIGNED_TRANSACTION -> {
                    inputTreeHasher.addLeaf(getBlockItemHash(item));
                    if (blockItemsMessage.blockNumber() == 0) {
                        LedgerIdPublicationTransactionBody publication =
                                findLedgerIdPublication(item.signedTransaction());
                        if (publication != null) {
                            VerificationServicePlugin.initializeTssParameters(publication);
                            this.ledgerId = publication.ledgerId();
                        }
                    }
                }
                case TRANSACTION_RESULT, TRANSACTION_OUTPUT -> outputTreeHasher.addLeaf(getBlockItemHash(item));
                case STATE_CHANGES -> stateChangesHasher.addLeaf(getBlockItemHash(item));
                case TRACE_DATA -> traceDataHasher.addLeaf(getBlockItemHash(item));
                // save footer for later
                case BLOCK_FOOTER -> this.blockFooter = BlockFooter.PROTOBUF.parse(item.blockFooter());
                // append block proofs
                case BLOCK_PROOF -> {
                    BlockProof blockProof = BlockProof.PROTOBUF.parse(item.blockProof());
                    blockProofs.add(blockProof);
                }
            }
        }

        if (blockItemsMessage.isEndOfBlock()) {
            return finalizeVerification(allPreviousBlockRootHash, previousBlockHash);
        }
        return null;
    }

    public VerificationNotification finalizeVerification(Bytes rootHashOfAllBlockHashesTree, Bytes previousBlockHash) {
        // since we always need block footer to finalize, we check for its presence
        if (this.blockFooter == null) {
            // return failed verification notification
            return new VerificationNotification(false, blockNumber, null, null, blockSource);
        }

        // if provided, use the provided root hash of all previous block hashes tree, otherwise use the one from the
        // footer
        Bytes rootOfAllPreviousBlockHashes = rootHashOfAllBlockHashesTree != null
                ? rootHashOfAllBlockHashesTree
                : this.blockFooter.rootHashOfAllBlockHashesTree();
        // if provided, use the provided previous block hash, otherwise use the one from the footer
        Bytes previousBlockHashToUse =
                previousBlockHash != null ? previousBlockHash : this.blockFooter.previousBlockRootHash();
        // while we don't have state management, use the start of block state root hash from the footer
        Bytes startOfBlockStateRootHash = this.blockFooter.startOfBlockStateRootHash();

        // for now, we only support TSS based signature proofs, we expect only 1 of these.
        // @todo(2019) extend to support other proof types as well
        BlockProof tssBasedProof = getSingle(blockProofs, BlockProof::hasSignedBlockProof);
        if (tssBasedProof != null) {
            return getVerificationResult(
                    tssBasedProof, previousBlockHashToUse, rootOfAllPreviousBlockHashes, startOfBlockStateRootHash);
        }

        // was not able to finalize verification yet
        return null;
    }

    /**
     * Finalizes the block verification by computing the final block hash,
     * verifying its signature, and updating metrics accordingly.
     *
     * @param blockProof the block proof
     * @return VerificationNotification indicating the result of the verification
     */
    protected VerificationNotification getVerificationResult(
            BlockProof blockProof,
            Bytes previousBlockHash,
            Bytes rootOfAllPreviousBlockHashes,
            Bytes startOfBlockStateRootHash) {

        final Bytes blockRootHash = HashingUtilities.computeFinalBlockHash(
                blockHeader.blockTimestamp(),
                previousBlockHash,
                rootOfAllPreviousBlockHashes,
                startOfBlockStateRootHash,
                inputTreeHasher,
                outputTreeHasher,
                consensusHeaderHasher,
                stateChangesHasher,
                traceDataHasher);

        final boolean verified =
                verifySignature(blockRootHash, blockProof.signedBlockProof().blockSignature());

        return new VerificationNotification(
                verified, blockNumber, blockRootHash, verified ? new BlockUnparsed(blockItems) : null, blockSource);
    }

    protected Boolean verifySignature(@NonNull Bytes hash, @NonNull Bytes signature) {
        // Legacy path: non-TSS blocks carry SHA384(blockHash) as the signature (48 bytes).
        // TODO: Remove this path before production — real network block proofs are never hash-of-hash.
        if (signature.length() == HASH_LENGTH) {
            return signature.equals(noThrowSha384HashOf(hash));
        }
        if (this.ledgerId == null) {
            return false;
        }
        // TSS.verifyTSS() handles both the genesis (Schnorr aggregate) and post-genesis (WRAPS) paths.
        // Signatures without a recognized proof suffix are rejected by the library.
        try {
            return TSS.verifyTSS(this.ledgerId.toByteArray(), signature.toByteArray(), hash.toByteArray());
        } catch (IllegalArgumentException | IllegalStateException e) {
            return false;
        }
    }

    /**
     * Extracts a {@link LedgerIdPublicationTransactionBody} from a signed transaction, if present.
     * Pure parsing — no side effects.
     *
     * @param signedTxBytes the raw signed transaction bytes
     * @return the parsed publication body, or {@code null} if not present
     */
    @Nullable
    private LedgerIdPublicationTransactionBody findLedgerIdPublication(Bytes signedTxBytes) throws ParseException {
        if (signedTxBytes == null || signedTxBytes.length() == 0) {
            return null;
        }
        SignedTransaction signedTx = SignedTransaction.PROTOBUF.parse(
                signedTxBytes.toReadableSequentialData(),
                false,
                false,
                Codec.DEFAULT_MAX_DEPTH,
                BlockAccessor.MAX_BLOCK_SIZE_BYTES);
        TransactionBody body = TransactionBody.PROTOBUF.parse(
                signedTx.bodyBytes().toReadableSequentialData(),
                false,
                false,
                Codec.DEFAULT_MAX_DEPTH,
                BlockAccessor.MAX_BLOCK_SIZE_BYTES);
        if (!body.hasLedgerIdPublication()) {
            return null;
        }
        return body.ledgerIdPublicationOrThrow();
    }

    public static <T> T getSingle(List<T> list, Predicate<T> predicate) {
        List<T> filtered = list.stream().filter(predicate).toList();

        if (filtered.size() != 1) {
            throw new IllegalStateException(String.format(
                    "Expected exactly 1 element matching predicate [%s], but found %d.", predicate, filtered.size()));
        }

        return filtered.get(0);
    }
}
