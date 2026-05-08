// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
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
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.block.stream.SiblingNode;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.block.stream.StateProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import org.hiero.metrics.LongCounter;

public class ExtendedMerkleTreeSession implements VerificationSession {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** Byte length of a legacy SHA384 signature (non-TSS blocks). */
    private static final int HASH_LENGTH = 48;

    /**
     * Reusable `SHA384withRSA` engine per thread — avoids per-call `Signature.getInstance()` cost.
     * The engine is stateful, so a `ThreadLocal` is required.
     */
    private static final ThreadLocal<Signature> SHA384_WITH_RSA = ThreadLocal.withInitial(() -> {
        try {
            return Signature.getInstance("SHA384withRSA");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA384withRSA not available in JVM", e);
        }
    });

    /**
     * Reusable `SHA-384` digest per thread — avoids per-call `MessageDigest.getInstance()` cost.
     * `MessageDigest` is stateful, so a `ThreadLocal` is required; each use calls `reset()` first.
     */
    private static final ThreadLocal<MessageDigest> SHA_384 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-384");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-384 not available in JVM", e);
        }
    });

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

    // RSA WRB verification state
    /**
     * Map from `node_id` to RSA `PublicKey` used for `SignedRecordFileProof` verification.
     * Built from the `NodeAddressBook` loaded by `RsaRosterBootstrapPlugin`.
     * Empty when no address book has been loaded yet.
     */
    private final Map<Long, PublicKey> rsaKeyByNodeId;
    /**
     * Raw serialized bytes of the outer `RecordFileItem` proto message, captured during block item
     * processing. Field 2 of this proto holds the `record_file_contents` bytes required to compute
     * the V6 signed payload: `SHA-384(int32(6) || record_file_contents)`.
     * Null until a `RECORD_FILE` block item is encountered.
     */
    private Bytes rawRecordFileItemProtoBytes;
    /** Metric for successful RSA WRB proof verifications; nullable — null when metrics not configured. */
    @Nullable
    private final LongCounter.Measurement rsaVerificationSuccessTotal;
    /** Metric for failed RSA WRB proof verifications; nullable — null when metrics not configured. */
    @Nullable
    private final LongCounter.Measurement rsaVerificationFailureTotal;
    /** Metric for signatures from `node_id` values absent from the loaded address book. */
    @Nullable
    private final LongCounter.Measurement rsaRosterMismatchTotal;

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
     * Constructor for `ExtendedMerkleTreeSession`.
     *
     * @param blockNumber the block number
     * @param blockSource the source of the block
     * @param previousBlockHash the previous block hash, may be null
     * @param allPreviousBlocksRootHash the all-previous-blocks root hash, may be null
     * @param ledgerId the trusted ledger ID for TSS verification, may be null for block 0
     * @param rsaKeyByNodeId map from `node_id` to RSA `PublicKey` for WRB proof verification;
     *     use `Map.of()` when no address book is available
     * @param rsaVerificationSuccessTotal metric counter for successful RSA verifications, may be null
     * @param rsaVerificationFailureTotal metric counter for failed RSA verifications, may be null
     * @param rsaRosterMismatchTotal metric counter for signatures from unknown nodes, may be null
     */
    public ExtendedMerkleTreeSession(
            final long blockNumber,
            final BlockSource blockSource,
            final Bytes previousBlockHash,
            final Bytes allPreviousBlocksRootHash,
            final Bytes ledgerId,
            final Map<Long, PublicKey> rsaKeyByNodeId,
            @Nullable final LongCounter.Measurement rsaVerificationSuccessTotal,
            @Nullable final LongCounter.Measurement rsaVerificationFailureTotal,
            @Nullable final LongCounter.Measurement rsaRosterMismatchTotal) {
        this.blockNumber = blockNumber;
        this.previousBlockHash = previousBlockHash;
        this.allPreviousBlockRootHash = allPreviousBlocksRootHash;
        this.ledgerId = ledgerId;
        this.rsaKeyByNodeId = Objects.requireNonNull(rsaKeyByNodeId, "rsaKeyByNodeId must not be null");
        this.rsaVerificationSuccessTotal = rsaVerificationSuccessTotal;
        this.rsaVerificationFailureTotal = rsaVerificationFailureTotal;
        this.rsaRosterMismatchTotal = rsaRosterMismatchTotal;
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
                // WRB record file — captured for RSA payload computation and hashed as output
                case RECORD_FILE -> {
                    this.rawRecordFileItemProtoBytes = item.recordFileOrThrow();
                    outputTreeHasher.addLeaf(getBlockItemHash(item));
                }
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

        // Route by proof type. RSA WRB proofs take priority (Phase 2a); TSS proofs follow.
        final BlockProof rsaProof = getFirst(blockProofs, BlockProof::hasSignedRecordFileProof);
        if (rsaProof != null) {
            return verifyRsaProof(
                    rsaProof, previousBlockHashToUse, rootOfAllPreviousBlockHashes, startOfBlockStateRootHash);
        }

        try {
            final BlockProof tssBasedProof = getSingle(blockProofs, BlockProof::hasSignedBlockProof);
            if (tssBasedProof != null) {
                return getVerificationResult(
                        tssBasedProof, previousBlockHashToUse, rootOfAllPreviousBlockHashes, startOfBlockStateRootHash);
            }

            final BlockProof stateBasedProof = getSingle(blockProofs, BlockProof::hasBlockStateProof);
            if (stateBasedProof != null) {
                return getStateProofVerificationResult(
                        stateBasedProof,
                        previousBlockHashToUse,
                        rootOfAllPreviousBlockHashes,
                        startOfBlockStateRootHash);
            }
        } catch (final IllegalStateException e) {
            LOGGER.log(WARNING, "Block [{0}] has malformed proof structure: %s".formatted(e.getMessage()), e);
            return new VerificationNotification(
                    false, FailureType.BAD_BLOCK_PROOF, blockNumber, null, null, blockSource);
        }

        // No recognised proof type found — reject
        LOGGER.log(WARNING, "No recognised proof type in block {0} — rejecting", blockNumber);
        return new VerificationNotification(false, FailureType.BAD_BLOCK_PROOF, blockNumber, null, null, blockSource);
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
     * Returns the first element matching `predicate`, or `null` if none match.
     *
     * @param <T> element type
     * @param list the list to search
     * @param predicate the match condition
     * @return first matching element, or `null`
     */
    private static <T> T getFirst(final List<T> list, final Predicate<T> predicate) {
        return list.stream().filter(predicate).findFirst().orElse(null);
    }

    // ---- RSA WRB verification -----------------------------------------------------------------------

    /**
     * Verifies a `SignedRecordFileProof` (WRB RSA proof) and returns a `VerificationNotification`.
     *
     * <p>**Algorithm (V6 only for Phase 2a):**
     *
     * 1. Compute the block root hash for chain continuity (identical to the TSS path).
     * 2. Extract `record_file_contents` bytes (proto field 2) from the captured `RECORD_FILE` item.
     * 3. Compute the signed payload: `SHA-384(int32(6) || rawRecordStreamFileBytes)`.
     * 4. For each `RecordFileSignature` entry:
     *    - Skip if `node_id` not in `rsaKeyByNodeId` (increment roster-mismatch counter).
     *    - Skip if signature bytes are all zeros (defensive pre-filter).
     *    - Verify with `SHA384withRSA`. If verification fails or throws, **reject the block
     *      immediately** — the CN only includes signatures from nodes that contributed to
     *      consensus, so any included signature must be cryptographically valid.
     * 5. Accept if `validCount * 2 > rosterSize` (strict majority: more than half of the
     *      address-book nodes must have signed). The CN sends exactly the signatures from
     *      nodes whose combined consensus weight exceeded 50%.
     *
     * <p>This method never throws; all error conditions return a `success=false` notification.
     *
     * @param blockProof the block proof carrying the `SignedRecordFileProof`
     * @param previousBlockHash the previous block hash for hash-chain computation
     * @param rootOfAllPreviousBlockHashes the root of all previous block hashes
     * @param startOfBlockStateRootHash the start-of-block state root hash
     * @return a `VerificationNotification` with `success=true` when the proof passes, `false` otherwise
     */
    private VerificationNotification verifyRsaProof(
            final BlockProof blockProof,
            final Bytes previousBlockHash,
            final Bytes rootOfAllPreviousBlockHashes,
            final Bytes startOfBlockStateRootHash) {

        // Compute block root hash for chain continuity — identical computation for all proof types
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

        // Guard: address book must be loaded before any WRB arrives
        if (rsaKeyByNodeId.isEmpty()) {
            LOGGER.log(
                    ERROR,
                    "Address book is empty — cannot verify RSA WRB proof for block {0}."
                            + " Ensure RsaRosterBootstrapPlugin started successfully.",
                    blockNumber);
            if (rsaVerificationFailureTotal != null) rsaVerificationFailureTotal.increment();
            return new VerificationNotification(
                    false, FailureType.BAD_BLOCK_PROOF, blockNumber, null, null, blockSource);
        }

        // Guard: RECORD_FILE item must be present in the block
        if (rawRecordFileItemProtoBytes == null) {
            LOGGER.log(
                    ERROR, "No RECORD_FILE item found in WRB block {0} — cannot compute signed payload", blockNumber);
            if (rsaVerificationFailureTotal != null) rsaVerificationFailureTotal.increment();
            return new VerificationNotification(
                    false, FailureType.BAD_BLOCK_PROOF, blockNumber, null, null, blockSource);
        }

        final SignedRecordFileProof proof = blockProof.signedRecordFileProofOrThrow();
        final int version = proof.version();

        // Phase 2a scope: only record file format version 6 is supported
        if (version != 6) {
            LOGGER.log(
                    WARNING,
                    "Unsupported SignedRecordFileProof version {0} in block {1} — only V6 is supported",
                    version,
                    blockNumber);
            if (rsaVerificationFailureTotal != null) rsaVerificationFailureTotal.increment();
            return new VerificationNotification(
                    false, FailureType.BAD_BLOCK_PROOF, blockNumber, null, null, blockSource);
        }

        // Extract the raw RecordStreamFile bytes from the RecordFileItem proto (field 2)
        final Bytes rawRecordStreamFileBytes = extractRecordStreamFileBytes(rawRecordFileItemProtoBytes);
        if (rawRecordStreamFileBytes.length() == 0) {
            LOGGER.log(ERROR, "Failed to extract record_file_contents from RECORD_FILE item in block {0}", blockNumber);
            if (rsaVerificationFailureTotal != null) rsaVerificationFailureTotal.increment();
            return new VerificationNotification(
                    false, FailureType.BAD_BLOCK_PROOF, blockNumber, null, null, blockSource);
        }

        // V6 payload: SHA-384(int32(6) || rawRecordStreamFileBytes)
        final byte[] signedPayload = computeV6SignedPayload(rawRecordStreamFileBytes);

        // Verify each signature and count valid ones.
        // Track which node_id values have already contributed a valid signature to prevent
        // a duplicate entry in the proof from inflating validCount.
        final int rosterSize = rsaKeyByNodeId.size();
        int validCount = 0;
        int mismatchCount = 0;
        final Set<Long> validatedNodes = new HashSet<>();

        for (final RecordFileSignature sig : proof.recordFileSignatures()) {
            final long nodeId = sig.nodeId();
            final PublicKey publicKey = rsaKeyByNodeId.get(nodeId);
            if (publicKey == null) {
                mismatchCount++;
                LOGGER.log(
                        DEBUG,
                        "Signature from node {0} not in address book (block {1}) — skipped",
                        nodeId,
                        blockNumber);
                continue;
            }
            if (validatedNodes.contains(nodeId)) {
                LOGGER.log(DEBUG, "Duplicate signature from node {0} in block {1} — skipped", nodeId, blockNumber);
                continue;
            }
            final byte[] sigBytes = sig.signaturesBytes().toByteArray();
            if (isAllZeros(sigBytes)) {
                LOGGER.log(DEBUG, "Zeroed signature from node {0} in block {1} — skipped", nodeId, blockNumber);
                continue;
            }
            try {
                final Signature engine = SHA384_WITH_RSA.get();
                engine.initVerify(publicKey);
                engine.update(signedPayload);
                if (engine.verify(sigBytes)) {
                    validCount++;
                    validatedNodes.add(nodeId);
                } else {
                    // CN only includes signatures from consensus-contributing nodes, so a failed
                    // cryptographic verification means the block or proof has been tampered with.
                    LOGGER.log(
                            WARNING,
                            "RSA signature from node {0} failed verification in block {1} — rejecting block",
                            nodeId,
                            blockNumber);
                    if (rsaVerificationFailureTotal != null) rsaVerificationFailureTotal.increment();
                    return new VerificationNotification(
                            false, FailureType.BAD_BLOCK_PROOF, blockNumber, null, null, blockSource);
                }
            } catch (final InvalidKeyException | SignatureException e) {
                LOGGER.log(
                        WARNING,
                        "RSA verification error for node {0} in block {1}: {2} — rejecting block",
                        nodeId,
                        blockNumber,
                        e.getMessage());
                if (rsaVerificationFailureTotal != null) rsaVerificationFailureTotal.increment();
                return new VerificationNotification(
                        false, FailureType.BAD_BLOCK_PROOF, blockNumber, null, null, blockSource);
            }
        }

        if (rsaRosterMismatchTotal != null && mismatchCount > 0) {
            rsaRosterMismatchTotal.increment(mismatchCount);
        }

        // Strict majority threshold: validCount must be strictly greater than half the roster.
        // The CN only sends signatures from nodes whose combined consensus weight exceeded 50%,
        // so we require validCount * 2 > rosterSize (equivalent to validCount > rosterSize / 2.0).
        // For example: 6-node roster requires > 3, i.e. ≥ 4 valid signatures.
        final boolean accepted = validCount * 2 > rosterSize;

        if (accepted) {
            LOGGER.log(
                    DEBUG,
                    "RSA WRB proof accepted for block {0}: {1}/{2} valid sigs (need > {3})",
                    blockNumber,
                    validCount,
                    rosterSize,
                    rosterSize / 2);
            if (rsaVerificationSuccessTotal != null) rsaVerificationSuccessTotal.increment();
        } else {
            LOGGER.log(
                    WARNING,
                    "RSA WRB proof rejected for block {0}: {1}/{2} valid sigs, need > {3}",
                    blockNumber,
                    validCount,
                    rosterSize,
                    rosterSize / 2);
            if (rsaVerificationFailureTotal != null) rsaVerificationFailureTotal.increment();
        }

        return new VerificationNotification(
                accepted,
                accepted ? null : FailureType.BAD_BLOCK_PROOF,
                blockNumber,
                accepted ? blockRootHash : null,
                accepted ? new BlockUnparsed(blockItems) : null,
                blockSource);
    }

    /**
     * Extracts the raw {@code record_file_contents} bytes from a serialized {@code RecordFileItem}
     * proto message by walking the protobuf wire format directly, without deserializing the message.
     *
     * <p>{@code record_file_contents} is proto field 2 of {@code RecordFileItem}. These bytes are
     * the verbatim content of the {@code .rcd} record stream file exactly as the consensus node
     * read it from disk when it computed the V6 signed hash. They must be returned byte-for-byte
     * identical to what the consensus node used; full deserialization via
     * {@code RecordFileItem.PROTOBUF.parse()} is deliberately avoided because re-serializing a
     * parsed object can produce subtly different bytes (e.g. omitting default-value fields, different
     * varint encoding choices), which would cause the recomputed hash to diverge from the one the
     * consensus node signed.
     *
     * <p><b>Protobuf wire format:</b> every field on the wire is encoded as a tag varint followed
     * by its value. The tag packs two things:
     * <ul>
     *   <li>{@code fieldNumber = tag >>> 3}
     *   <li>{@code wireType   = tag & 0x7}
     * </ul>
     * Wire type 2 ({@code LEN}) means the value is length-prefixed bytes, used for {@code bytes},
     * {@code string}, and embedded messages. It is encoded as:
     * {@code [tag varint] [length varint] [raw bytes...]}.
     *
     * <p><b>Algorithm:</b>
     * <ol>
     *   <li>Read the next field tag varint and decode its field number and wire type.</li>
     *   <li>If {@code fieldNumber == 2} and {@code wireType == LEN}: read the length prefix varint,
     *       read exactly that many bytes, and return them — these are the
     *       {@code record_file_contents}.</li>
     *   <li>Otherwise skip the field using the wire type to know how many bytes to consume:
     *       <ul>
     *         <li>VARINT (wire 0): read and discard one varint</li>
     *         <li>I64 (wire 1): skip 8 bytes fixed</li>
     *         <li>LEN (wire 2): read the length prefix, skip that many bytes</li>
     *         <li>I32 (wire 5): skip 4 bytes fixed</li>
     *       </ul>
     *   </li>
     *   <li>Repeat until field 2 is found or input is exhausted.</li>
     * </ol>
     *
     * @param recordFileItemBytes raw serialized bytes of a {@code RecordFileItem} proto message
     * @return verbatim bytes of the {@code record_file_contents} field (proto field 2), or
     *         {@code Bytes.EMPTY} if field 2 is not present or if any parse error occurs
     */
    private static Bytes extractRecordStreamFileBytes(final Bytes recordFileItemBytes) {
        try {
            final ReadableSequentialData input = recordFileItemBytes.toReadableSequentialData();
            while (input.hasRemaining()) {
                // Each field starts with a tag varint: high bits = field number, low 3 bits = wire type
                final int tag = input.readVarInt(false);
                final int wireType = tag & 0x7;
                final int fieldNumber = tag >>> 3;
                if (fieldNumber == 2 && wireType == 2) {
                    // Found record_file_contents (field 2, LEN wire type).
                    // Read the length-prefix varint then copy the raw payload bytes verbatim.
                    final int len = input.readVarInt(false);
                    final byte[] raw = new byte[len];
                    input.readBytes(raw);
                    return Bytes.wrap(raw);
                }
                // Not field 2 — skip this field using its wire type to advance the cursor correctly
                switch (wireType) {
                    case 0 -> input.readVarLong(false); // VARINT: read and discard the value
                    case 1 -> input.skip(8); // I64: fixed 64-bit, skip 8 bytes
                    case 2 -> { // LEN: read length prefix, skip content
                        final int l = input.readVarInt(false);
                        input.skip(l);
                    }
                    case 5 -> input.skip(4); // I32: fixed 32-bit, skip 4 bytes
                    default -> {
                        return Bytes.EMPTY; // Unknown wire type — bail out safely
                    }
                }
            }
        } catch (final RuntimeException e) {
            // Any unexpected read error (truncated input, malformed varint, etc.) is treated
            // as a missing field rather than propagated, so the caller can fail verification cleanly.
            return Bytes.EMPTY;
        }
        return Bytes.EMPTY; // field 2 not present in the message
    }

    /**
     * Computes the V6 RSA signed payload: `SHA-384(int32(6) || rawRecordStreamFileBytes)`.
     *
     * <p>This matches `SignatureDataExtractor.computeSignedHash(6, ...)` in
     * `tools-and-tests/tools`. Inlined here because that module cannot be imported from
     * `block-node/verification`.
     *
     * @param rawRecordStreamFileBytes raw bytes of the `record_file_contents` field
     * @return 48-byte SHA-384 digest
     */
    private static byte[] computeV6SignedPayload(final Bytes rawRecordStreamFileBytes) {
        final MessageDigest digest = SHA_384.get();
        digest.reset();
        digest.update(new byte[] {0, 0, 0, 6}); // int32(6) big-endian
        rawRecordStreamFileBytes.writeTo(digest);
        return digest.digest();
    }

    /**
     * Returns `true` if every byte in `bytes` is zero.
     *
     * @param bytes the byte array to inspect
     * @return `true` when the array is all zeros or empty
     */
    private static boolean isAllZeros(final byte[] bytes) {
        for (final byte b : bytes) {
            if (b != 0) return false;
        }
        return true;
    }

    // ---- StateProof (indirect proof) verification -------------------------------------------------------------------

    /**
     * Verifies a state-proof-based block proof and returns a {@link VerificationNotification}.
     *
     * @param blockProof the block proof carrying the {@code BlockStateProof}
     * @param previousBlockHash the previous block hash for hash-chain computation
     * @param rootOfAllPreviousBlockHashes the root of all previous block hashes
     * @param startOfBlockStateRootHash the start-of-block state root hash
     * @return a {@link VerificationNotification}
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
                verified,
                verified ? null : FailureType.BAD_BLOCK_PROOF,
                blockNumber,
                blockRootHash,
                verified ? new BlockUnparsed(blockItems) : null,
                blockSource);
    }

    /**
     * Walks the merkle path chain in a state proof to reconstruct the directly-signed block's
     * root hash, then verifies the TSS signature on that root.
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
        if (totalSiblings < 3 || (totalSiblings - 3) % 4 != 0) {
            LOGGER.log(
                    WARNING,
                    "Block {0} state proof sibling count {1} is invalid (need >= 3, remainder must be multiple of 4)",
                    blockNumber,
                    totalSiblings);
            return false;
        }
        if (!siblingPath.hasHash() || siblingPath.hash().length() == 0) {
            LOGGER.log(
                    WARNING, "Block {0} state proof path 1 (sibling) has missing or empty starting hash", blockNumber);
            return false;
        }
        for (SiblingNode sibling : siblings) {
            if (sibling.hash().length() == 0) {
                LOGGER.log(WARNING, "Block {0} state proof contains a sibling node with an empty hash", blockNumber);
                return false;
            }
        }

        byte[] current = siblingPath.hash().toByteArray();
        int index = 0;
        boolean firstIteration = true;

        while (totalSiblings - index > 3) {
            SiblingNode prevBlockRootsHash = siblings.get(index);
            SiblingNode depth5Node2Sibling = siblings.get(index + 1);
            SiblingNode depth4Node2Sibling = siblings.get(index + 2);
            SiblingNode hashedTimestampSibling = siblings.get(index + 3);

            byte[] depth5Node1 = combineSibling(current, prevBlockRootsHash);
            byte[] depth4Node1 = combineSibling(depth5Node1, depth5Node2Sibling);
            byte[] depth3Node1 = combineSibling(depth4Node1, depth4Node2Sibling);
            byte[] depth2Node2 = hashInternalNodeSingleChild(depth3Node1);
            current = hashInternalNode(hashedTimestampSibling.hash().toByteArray(), depth2Node2);

            if (firstIteration) {
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

        byte[] depth5Node1 = combineSibling(current, siblings.get(index));
        byte[] depth4Node1 = combineSibling(depth5Node1, siblings.get(index + 1));
        byte[] depth3Node1 = combineSibling(depth4Node1, siblings.get(index + 2));
        byte[] depth2Node2 = hashInternalNodeSingleChild(depth3Node1);
        byte[] hashedTimestampLeaf = hashLeaf(timestampPath.timestampLeaf().toByteArray());
        byte[] signedBlockRoot = hashInternalNode(hashedTimestampLeaf, depth2Node2);

        return verifySignature(
                Bytes.wrap(signedBlockRoot), stateProof.signedBlockProof().blockSignature());
    }

    private static byte[] combineSibling(byte[] current, SiblingNode sibling) {
        if (sibling.isLeft()) {
            return hashInternalNode(sibling.hash().toByteArray(), current);
        } else {
            return hashInternalNode(current, sibling.hash().toByteArray());
        }
    }

    /** Returns the single element matching the predicate, or null if none match (throws if more than one). */
    private <T> T getSingle(List<T> list, Predicate<T> predicate) {
        List<T> filtered = list.stream().filter(predicate).toList();
        if (filtered.size() > 1) {
            throw new IllegalStateException(
                    "Expected at most 1 element matching predicate, but found " + filtered.size());
        }
        return filtered.isEmpty() ? null : filtered.get(0);
    }
}
