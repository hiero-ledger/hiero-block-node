// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.common.hasher.HashingUtilities.getBlockItemHash;
import static org.hiero.block.common.hasher.HashingUtilities.hashInternalNode;
import static org.hiero.block.common.hasher.HashingUtilities.hashInternalNodeSingleChild;
import static org.hiero.block.common.hasher.HashingUtilities.hashLeaf;
import static org.hiero.block.common.hasher.HashingUtilities.noThrowSha384HashOf;

import com.hedera.cryptography.tss.TSS;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.MerklePath;
import com.hedera.hapi.block.stream.SiblingNode;
import com.hedera.hapi.block.stream.StateProof;
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
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
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
            return new VerificationNotification(
                    false, FailureType.BAD_BLOCK_PROOF, blockNumber, null, null, blockSource);
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

        // Try direct TSS proof first (most common case)
        BlockProof tssBasedProof = getSingle(blockProofs, BlockProof::hasSignedBlockProof);
        if (tssBasedProof != null) {
            return getVerificationResult(
                    tssBasedProof, previousBlockHashToUse, rootOfAllPreviousBlockHashes, startOfBlockStateRootHash);
        }

        // Try indirect (state) proof — used when TSS signing was delayed
        BlockProof stateBasedProof = getSingle(blockProofs, BlockProof::hasBlockStateProof);
        if (stateBasedProof != null) {
            return getStateProofVerificationResult(
                    stateBasedProof, previousBlockHashToUse, rootOfAllPreviousBlockHashes, startOfBlockStateRootHash);
        }

        // No supported proof type found
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
                verified,
                verified ? null : FailureType.BAD_BLOCK_PROOF,
                blockNumber,
                blockRootHash,
                verified ? new BlockUnparsed(blockItems) : null,
                blockSource);
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
     * Verifies a block using an indirect (state) proof. A state proof proves that the target
     * block's root hash is embedded in a later directly-signed block's merkle tree, forming
     * a chain: target block hash -> merkle siblings -> signed block root -> TSS signature.
     *
     * @param blockProof the block proof containing a state proof
     * @param previousBlockHash the previous block hash
     * @param rootOfAllPreviousBlockHashes the root hash of all previous block hashes
     * @param startOfBlockStateRootHash the start of block state root hash
     * @return VerificationNotification indicating the result of the verification
     */
    protected VerificationNotification getStateProofVerificationResult(
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

        final boolean verified = verifyStateProof(blockRootHash, blockProof.blockStateProof());

        return new VerificationNotification(
                verified, blockNumber, blockRootHash, verified ? new BlockUnparsed(blockItems) : null, blockSource);
    }

    /**
     * Walks the merkle path chain in a state proof to reconstruct the directly-signed block's
     * root hash, then verifies the TSS signature on that root.
     *
     * <p>A valid block state proof contains 3 paths:
     * <ul>
     *   <li>Path 0: timestamp leaf of the signed block</li>
     *   <li>Path 1: sibling hashes from the target block through all `gap` (i.e. not directly signed) blocks to the signed block</li>
     *   <li>Path 2: terminal (empty)</li>
     * </ul>
     *
     * <p>Path 1's siblings are laid out as groups:
     * <ul>
     *   <li>Per gap block (4 siblings): prevBlockRootsHash (right), depth5Node2 (right), depth4Node2 (right), <i>already-hashed</i> timestamp (left)</li>
     *   <li>Final signed block (3 siblings): prevBlockRootsHash (right), depth5Node2 (right), depth4Node2 (right)
     * </ul>
     *
     * @param blockRootHash the computed root hash of the target block
     * @param stateProof the state proof to verify
     * @return true if the proof chain and TSS signature are valid
     */
    protected boolean verifyStateProof(@NonNull Bytes blockRootHash, @NonNull StateProof stateProof) {
        List<MerklePath> paths = stateProof.paths();
        if (paths.size() != 3) {
            LOGGER.log(WARNING, "Block {0} state proof has {1} paths, expected 3", blockNumber, paths.size());
            return false;
        }

        MerklePath timestampPath = paths.get(0);
        MerklePath siblingPath = paths.get(1);
        MerklePath terminalPath = paths.get(2);

        if (!terminalPath.siblings().isEmpty() || terminalPath.hasTimestampLeaf()) {
            LOGGER.log(WARNING, "Block {0} state proof path 2 (terminal) is unexpectedly non-empty", blockNumber);
            return false;
        }

        if (!timestampPath.hasTimestampLeaf()) {
            LOGGER.log(WARNING, "Block {0} state proof path 0 (timestamp) is missing timestamp leaf", blockNumber);
            return false;
        }
        if (!stateProof.hasSignedBlockProof()) {
            LOGGER.log(WARNING, "Block {0} state proof is missing signed block proof", blockNumber);
            return false;
        }

        List<SiblingNode> siblings = siblingPath.siblings();
        int totalSiblings = siblings.size();
        // Must have at least 3 (signed block siblings) and remainder must be groups of 4 (gap blocks)
        if (totalSiblings < 3 || (totalSiblings - 3) % 4 != 0) {
            LOGGER.log(
                    WARNING,
                    "Block {0} state proof sibling count {1} is invalid (need >= 3, remainder must be multiple of 4)",
                    blockNumber,
                    totalSiblings);
            return false;
        }
        // Starting hash must be present and non-empty to avoid propagating incorrect hashes
        if (!siblingPath.hasHash() || siblingPath.hash().length() == 0) {
            LOGGER.log(WARNING, "Block {0} state proof path 1 (sibling) has missing or empty starting hash", blockNumber);
            return false;
        }
        for (SiblingNode sibling : siblings) {
            if (sibling.hash().length() == 0) {
                LOGGER.log(WARNING, "Block {0} state proof contains a sibling node with an empty hash", blockNumber);
                return false;
            }
        }

        // siblingPath.hash() is the previous block's (T-1) root hash. Walking the siblings in groups
        // first reconstructs the target block T's root hash, then each subsequent gap block's root hash,
        // until we reach the signed block. After the first iteration, current equals T's reconstructed
        // root hash — verify it matches the independently computed blockRootHash to ensure block content integrity.
        byte[] current = siblingPath.hash().toByteArray();
        int index = 0;
        boolean firstIteration = true;

        // Process gap blocks (including the target block itself) — each contributes 4 siblings
        while (totalSiblings - index > 3) {
            SiblingNode prevBlockRootsHash = siblings.get(index);
            SiblingNode depth5Node2Sibling = siblings.get(index + 1);
            SiblingNode depth4Node2Sibling = siblings.get(index + 2);
            SiblingNode hashedTimestampSibling = siblings.get(index + 3);

            byte[] depth5Node1 = combineSibling(current, prevBlockRootsHash);
            byte[] depth4Node1 = combineSibling(depth5Node1, depth5Node2Sibling);
            byte[] depth3Node1 = combineSibling(depth4Node1, depth4Node2Sibling);
            byte[] depth2Node2 = hashInternalNodeSingleChild(depth3Node1);
            // The timestamp sibling hash is already hashLeaf(rawTimestamp) — a 48-byte node value.
            // Combine as depth2Node1 (left) with depth2Node2 (right) to reconstruct the gap block root.
            current = hashInternalNode(hashedTimestampSibling.hash().toByteArray(), depth2Node2);

            if (firstIteration) {
                // current now holds the reconstructed target block root hash; verify it matches
                // the hash computed from the actual block content we received.
                final Bytes reconstructed = Bytes.wrap(current);
                if (!blockRootHash.equals(reconstructed)) {
                    LOGGER.log(
                            WARNING,
                            "Block {0} state proof integrity check failed: hash reconstructed from path 1 siblings"
                                    + " [{1}] does not match block root hash computed from block content [{2}]",
                            blockNumber,
                            reconstructed,
                            blockRootHash);
                    return false;
                }
                firstIteration = false;
            }

            index += 4;
        }

        // Process the signed block's 3 siblings
        byte[] depth5Node1 = combineSibling(current, siblings.get(index));
        byte[] depth4Node1 = combineSibling(depth5Node1, siblings.get(index + 1));
        byte[] depth3Node1 = combineSibling(depth4Node1, siblings.get(index + 2));

        // Reconstruct the signed block's root hash using Path 0's raw timestamp bytes
        byte[] depth2Node2 = hashInternalNodeSingleChild(depth3Node1);
        byte[] hashedTimestampLeaf = hashLeaf(timestampPath.timestampLeaf().toByteArray());
        byte[] signedBlockRoot = hashInternalNode(hashedTimestampLeaf, depth2Node2);

        // Verify TSS signature on the signed block's root hash
        return verifySignature(
                Bytes.wrap(signedBlockRoot), stateProof.signedBlockProof().blockSignature());
    }

    /**
     * Combines the current hash with a sibling node, respecting the sibling's position.
     *
     * @param current the current hash being walked up the tree
     * @param sibling the sibling node with position and hash
     * @return the parent hash
     */
    private static byte[] combineSibling(byte[] current, SiblingNode sibling) {
        if (sibling.isLeft()) {
            return hashInternalNode(sibling.hash().toByteArray(), current);
        } else {
            return hashInternalNode(current, sibling.hash().toByteArray());
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

    /**
     * Returns the single element matching the predicate, or null if none match.
     * Logs a WARNING and returns null if multiple elements match.
     */
    private <T> T getSingle(List<T> list, Predicate<T> predicate) {
        List<T> filtered = list.stream().filter(predicate).toList();
        if (filtered.size() > 1) {
            LOGGER.log(
                    WARNING,
                    "Expected exactly 1 element matching predicate [{0}], but found {1}.",
                    predicate,
                    filtered.size());
            return null;
        }
        return filtered.isEmpty() ? null : filtered.get(0);
    }
}
