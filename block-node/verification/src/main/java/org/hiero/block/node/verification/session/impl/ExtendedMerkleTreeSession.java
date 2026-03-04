// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.common.hasher.HashingUtilities.getBlockItemHash;
import static org.hiero.block.common.hasher.HashingUtilities.noThrowSha384HashOf;

import com.hedera.cryptography.tss.TSS;
import com.hedera.cryptography.wraps.WRAPSVerificationKey;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.tss.LedgerIdNodeContribution;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.NaiveStreamingTreeHasher;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.VerificationSession;

public class ExtendedMerkleTreeSession implements VerificationSession {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** Byte length of the hinTS verification key embedded at the start of every TSS blockSignature. */
    private static final int VK_LENGTH = 1_096;

    /** Byte length of a legacy SHA384 signature (non-TSS blocks). */
    private static final int HASH_LENGTH = 48;

    /**
     * Trusted ledger ID parsed from LedgerIdPublicationTransactionBody in block 0.
     * Null until the first such transaction is encountered in the block stream.
     * TSS.setAddressBook() and WRAPSVerificationKey.setCurrentKey() are called when set.
     * May be pre-seeded at startup from {@code verification.ledgerId} configuration.
     */
    public static final AtomicReference<Bytes> ACTIVE_LEDGER_ID = new AtomicReference<>(null);

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
     * Constructor for ExtendedMerkleTreeSession.
     *
     * @param blockNumber the block number
     * @param blockSource the source of the block
     * @param previousBlockHash the previous block hash, may be null
     * @param allPreviousBlocksRootHash the all previous blocks root hash, may be null
     */
    public ExtendedMerkleTreeSession(
            final long blockNumber,
            final BlockSource blockSource,
            final Bytes previousBlockHash,
            final Bytes allPreviousBlocksRootHash) {
        this.blockNumber = blockNumber;
        this.previousBlockHash = previousBlockHash;
        this.allPreviousBlockRootHash = allPreviousBlocksRootHash;
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
                    // LedgerIdPublicationTransactionBody is only present in block 0; stop scanning
                    // once found (ACTIVE_LEDGER_ID non-null) to avoid redundant protobuf parsing.
                    if (blockNumber == 0 && ACTIVE_LEDGER_ID.get() == null) {
                        loadLedgerId(item.signedTransaction());
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
        // Reject signatures too short to hold a VK prefix.
        if (signature.length() < VK_LENGTH) {
            return false;
        }
        Bytes ledgerId = ACTIVE_LEDGER_ID.get();
        if (ledgerId == null) {
            LOGGER.log(
                    WARNING,
                    "Cannot verify block {0}: ledger ID not initialized (block 0 not seen and none configured)",
                    blockNumber);
            return false;
        }
        // TSS.verifyTSS() handles both the genesis (Schnorr aggregate) and post-genesis (WRAPS) paths.
        // Signatures without a recognised proof suffix are rejected by the library.
        try {
            return TSS.verifyTSS(ledgerId.toByteArray(), signature.toByteArray(), hash.toByteArray());
        } catch (IllegalArgumentException | IllegalStateException e) {
            return false;
        }
    }

    // Parses a signed transaction looking for LedgerIdPublicationTransactionBody; when found,
    // bootstraps TSS native state (address book + WRAPS VK) and sets ACTIVE_LEDGER_ID.
    private static void loadLedgerId(Bytes signedTxBytes) throws ParseException {
        if (signedTxBytes == null || signedTxBytes.length() == 0) {
            return;
        }
        SignedTransaction signedTx = SignedTransaction.PROTOBUF.parse(signedTxBytes);
        TransactionBody body = TransactionBody.PROTOBUF.parse(signedTx.bodyBytes());
        if (!body.hasLedgerIdPublication()) {
            return;
        }
        LedgerIdPublicationTransactionBody ledgerPub = body.ledgerIdPublicationOrThrow();
        List<LedgerIdNodeContribution> contributions = ledgerPub.nodeContributions();
        int nodeCount = contributions.size();
        byte[][] publicKeys = new byte[nodeCount][];
        long[] nodeIds = new long[nodeCount];
        long[] weights = new long[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            LedgerIdNodeContribution contribution = contributions.get(i);
            publicKeys[i] = contribution.historyProofKey().toByteArray();
            nodeIds[i] = contribution.nodeId();
            weights[i] = contribution.weight();
        }
        TSS.setAddressBook(publicKeys, weights, nodeIds);
        Bytes historyProofVk = ledgerPub.historyProofVerificationKey();
        if (historyProofVk.length() > 0) {
            WRAPSVerificationKey.setCurrentKey(historyProofVk.toByteArray());
        }
        ACTIVE_LEDGER_ID.set(ledgerPub.ledgerId());
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
