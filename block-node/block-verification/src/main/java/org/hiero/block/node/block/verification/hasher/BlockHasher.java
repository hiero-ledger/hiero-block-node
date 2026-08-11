// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.hasher;

import static org.hiero.block.common.hasher.HashingUtilities.getBlockItemHash;
import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.UnknownField;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.hiero.block.api.TssData;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.NaiveStreamingTreeHasher;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.VerificationHelper;
import org.hiero.block.node.block.verification.metrics.HashingMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;
import org.hiero.block.node.block.verification.session.VerificationSessionFailedException;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// Block hasher.
/// This class is the first stage of a [org.hiero.block.node.block.verification.session.CompletableVerificationSession].
/// The hasher is responsible for receiving a block as items and to dynamically hash the items.
/// Eventually, a [HashingResult] is produced.
public final class BlockHasher implements Supplier<HashingResult> {
    /// The time to park between polls of the block items deque when no data is available.
    private static final long DATA_BUSY_WAIT_TIME_NANOS = TimeUnit.MICROSECONDS.toNanos(200);
    /// The first `BlockItem` field number governed by the block stream forward compatibility
    /// numbering rule. Field numbers below this value belong to the first release; an unknown
    /// field below it is reserved for item types that require specific handling.
    private static final int FIRST_FORWARD_COMPATIBLE_FIELD_NUMBER = 20;
    /// The modulus of the forward compatibility numbering rule: for a field numbered
    /// [#FIRST_FORWARD_COMPATIBLE_FIELD_NUMBER] or above, the hashing category is the field
    /// number modulo this value.
    private static final int CATEGORY_MODULUS = 20;
    /// The number of the block being hashed.
    private final long blockNumber;
    /// The source of the block, carried through to the result and failures.
    private final BlockSource blockSource;
    /// Cancellation flag shared with the owning session.
    private final AtomicBoolean isCanceled;
    /// Metrics recorded by the hashing stage.
    private final HashingMetrics hashingMetrics;
    /// The deque through which the block's item batches are supplied to the hasher.
    private final ConcurrentLinkedDeque<BlockItems> blockItemsRecordsDeque;
    /// All items of the block accumulated so far, used to build the complete block for the result.
    private final List<BlockItemUnparsed> accumulatedBlockItems;
    /// All block proofs parsed so far.
    private final List<BlockProof> blockProofs;
    /// The tree hasher for input item hashes.
    private final NaiveStreamingTreeHasher inputTreeHasher;
    /// The tree hasher for output item hashes.
    private final NaiveStreamingTreeHasher outputTreeHasher;
    /// The tree hasher for consensus header item hashes.
    private final NaiveStreamingTreeHasher consensusHeaderHasher;
    /// The tree hasher for state changes item hashes.
    private final NaiveStreamingTreeHasher stateChangesHasher;
    /// The tree hasher for trace data item hashes.
    private final NaiveStreamingTreeHasher traceDataHasher;
    /// Extension 0 subtree hasher, leaf position 9 of the fixed 16 leaf block root tree
    /// ("Merkle Mountain Top" in HIP-1424). The eight extension leaf positions (9 to 16) are
    /// permanently defined in the tree; each hasher below is bound to its position and is
    /// always instantiated. An extension hasher with no items produces {@code EMPTY_TREE_HASH}
    /// via the same fold as every other subtree, which is what its slot contributes to the
    /// Merkle Mountain Top. See issue #3377.
    private final NaiveStreamingTreeHasher extensionHasherZero;
    /// Extension 1 subtree hasher, leaf position 10. See [#extensionHasherZero].
    private final NaiveStreamingTreeHasher extensionHasherOne;
    /// Extension 2 subtree hasher, leaf position 11. See [#extensionHasherZero].
    private final NaiveStreamingTreeHasher extensionHasherTwo;
    /// Extension 3 subtree hasher, leaf position 12. See [#extensionHasherZero].
    private final NaiveStreamingTreeHasher extensionHasherThree;
    /// Extension 4 subtree hasher, leaf position 13. See [#extensionHasherZero].
    private final NaiveStreamingTreeHasher extensionHasherFour;
    /// Extension 5 subtree hasher, leaf position 14. See [#extensionHasherZero].
    private final NaiveStreamingTreeHasher extensionHasherFive;
    /// Extension 6 subtree hasher, leaf position 15. See [#extensionHasherZero].
    private final NaiveStreamingTreeHasher extensionHasherSix;
    /// Extension 7 subtree hasher, leaf position 16. See [#extensionHasherZero].
    private final NaiveStreamingTreeHasher extensionHasherSeven;
    /// Provider of the verification data, used to publish TSS data found in block 0.
    private final VerificationDataProvider verificationDataProvider;
    /// The parsed block header, set when the header item is seen.
    private BlockHeader blockHeader;
    /// The parsed block footer, set when the footer item is seen.
    private BlockFooter blockFooter;
    /// The HAPI proto version from the block header.
    private SemanticVersion hapiProtoVersion;
    /// Raw serialized bytes of the outer `RecordFileItem` proto message, captured when a
    /// `RECORD_FILE` item is seen. Field 2 of this proto holds the `record_file_contents`
    /// bytes required to compute the V6 signed payload. Null until such an item is encountered.
    private Bytes rawRecordFileItemProtoBytes;

    /// Constructor.
    ///
    /// @param isCanceled cancellation flag shared with the owning session, must not be null
    /// @param blockItemsDeque the deque through which the block's item batches are supplied,
    ///     must not be null
    /// @param hashingMetrics metrics recorded by the hashing stage, must not be null
    /// @param blockNumber the number of the block to hash, must be non-negative
    /// @param blockSource the source of the block, must not be null
    /// @param verificationDataProvider provider of the verification data, must not be null
    public BlockHasher(
            final AtomicBoolean isCanceled,
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque,
            final HashingMetrics hashingMetrics,
            final long blockNumber,
            final BlockSource blockSource,
            final VerificationDataProvider verificationDataProvider) {
        if (blockNumber < 0) {
            throw new IllegalArgumentException("Block number must be non-negative");
        }
        this.blockNumber = blockNumber;
        this.hashingMetrics = Objects.requireNonNull(hashingMetrics);
        this.blockSource = Objects.requireNonNull(blockSource);
        this.verificationDataProvider = Objects.requireNonNull(verificationDataProvider);
        this.isCanceled = Objects.requireNonNull(isCanceled);
        this.blockItemsRecordsDeque = Objects.requireNonNull(blockItemsDeque);
        this.accumulatedBlockItems = new ArrayList<>();
        this.blockProofs = new ArrayList<>();
        this.inputTreeHasher = new NaiveStreamingTreeHasher();
        this.outputTreeHasher = new NaiveStreamingTreeHasher();
        this.consensusHeaderHasher = new NaiveStreamingTreeHasher();
        this.stateChangesHasher = new NaiveStreamingTreeHasher();
        this.traceDataHasher = new NaiveStreamingTreeHasher();
        this.extensionHasherZero = new NaiveStreamingTreeHasher();
        this.extensionHasherOne = new NaiveStreamingTreeHasher();
        this.extensionHasherTwo = new NaiveStreamingTreeHasher();
        this.extensionHasherThree = new NaiveStreamingTreeHasher();
        this.extensionHasherFour = new NaiveStreamingTreeHasher();
        this.extensionHasherFive = new NaiveStreamingTreeHasher();
        this.extensionHasherSix = new NaiveStreamingTreeHasher();
        this.extensionHasherSeven = new NaiveStreamingTreeHasher();
    }

    /// This method keeps polling for block items received and dynamically hashes the items.
    /// When the block is received in full, the root hash of the block is calculated and a
    /// [HashingResult] is returned.
    /// @throws VerificationSessionFailedException in case a known failure occurs
    @Override
    public HashingResult get() {
        try {
            final long hashingStartTime = System.nanoTime();
            while (!isCanceled()) {
                final BlockItems blockItemsRecord = blockItemsRecordsDeque.poll();
                if (blockItemsRecord == null) {
                    LockSupport.parkNanos(DATA_BUSY_WAIT_TIME_NANOS);
                } else {
                    final List<BlockItemUnparsed> currentBlockItems = blockItemsRecord.blockItems();
                    if (blockItemsRecord.isStartOfNewBlock()
                            && !currentBlockItems.getFirst().hasBlockHeader()) {
                        throw new VerificationSessionFailedException(
                                blockNumber, SessionFailureType.MISSING_MANDATORY_ITEM, blockSource);
                    } else {
                        this.accumulatedBlockItems.addAll(currentBlockItems);
                        for (final BlockItemUnparsed item : currentBlockItems) {
                            final SessionFailureType failure = processItem(item, blockItemsRecord.blockNumber());
                            if (failure != null) {
                                throw new VerificationSessionFailedException(blockNumber, failure, blockSource);
                            }
                        }
                    }
                    if (blockItemsRecord.isEndOfBlock()) {
                        final HashingResult hashingResult = finalHashingResult();
                        final long hashingTimeElapsed = System.nanoTime() - hashingStartTime;
                        hashingMetrics.hashingBlockTimeNs().increment(hashingTimeElapsed);
                        return hashingResult;
                    }
                }
            }
            throw new VerificationSessionFailedException(blockNumber, SessionFailureType.CANCELLED, blockSource);
        } catch (final ParseException e) {
            throw new VerificationSessionFailedException(
                    blockNumber, SessionFailureType.UNABLE_TO_PARSE, blockSource, e);
        } catch (final NoSuchAlgorithmException e) {
            throw new VerificationSessionFailedException(
                    blockNumber, SessionFailureType.MISSING_VERIFICATION_DATA, blockSource, e);
        }
    }

    /// Returns true if the session has been canceled.
    private boolean isCanceled() {
        return isCanceled.get() || Thread.currentThread().isInterrupted();
    }

    /// Processes a single block item: hashes it into the correct subtree and captures block
    /// level data (header, footer, proofs) along the way.
    ///
    /// The switch below is deliberately an exhaustive switch expression with no default branch.
    /// When a new item type is added to the `BlockItem` schema, compilation fails here until the
    /// new type is given an explicit handling decision. An item type that is unknown to the
    /// compiled schema altogether surfaces as `UNSET` with the data preserved as an unknown
    /// field, and is handled by the forward compatibility numbering rule in
    /// [#processFutureItem].
    /// @param item the block item to process
    /// @param itemsBlockNumber the block number carried by the current block items record
    /// @return null on success, or the failure type when the block must be refused
    /// @throws ParseException if a known item fails to parse
    private SessionFailureType processItem(final BlockItemUnparsed item, final long itemsBlockNumber)
            throws ParseException {
        final BlockItemUnparsed.ItemOneOfType kind = item.item().kind();
        final List<UnknownField> unknownFields = item.getUnknownFields();
        final SessionFailureType failure;
        if (kind != BlockItemUnparsed.ItemOneOfType.UNSET && !unknownFields.isEmpty()) {
            // A BlockItem is a protobuf oneof, so a valid item carries exactly one field. A known
            // item type alongside unknown fields parses fine, but is not a processable stream.
            failure = SessionFailureType.UNSUPPORTED_STREAM_FORMAT;
        } else {
            failure = switch (kind) {
                case BLOCK_HEADER -> {
                    if (this.blockHeader != null) {
                        // a mandatory once per block item appearing more than once is a valid
                        // encoding, but not a processable stream
                        yield SessionFailureType.UNSUPPORTED_STREAM_FORMAT;
                    } else {
                        this.blockHeader = standardParse(BlockHeader.PROTOBUF, item.blockHeader());
                        this.hapiProtoVersion = this.blockHeader.hapiProtoVersion();
                        if (this.hapiProtoVersion == null) {
                            yield SessionFailureType.MISSING_MANDATORY_FIELD;
                        } else {
                            outputTreeHasher.addLeaf(getBlockItemHash(item));
                            yield null;
                        }
                    }
                }
                case ROUND_HEADER, EVENT_HEADER -> {
                    consensusHeaderHasher.addLeaf(getBlockItemHash(item));
                    yield null;
                }
                case SIGNED_TRANSACTION -> {
                    inputTreeHasher.addLeaf(getBlockItemHash(item));
                    if (itemsBlockNumber == 0 && !verificationDataProvider.hasTssData()) {
                        final LedgerIdPublicationTransactionBody publication =
                                findLedgerIdPublication(item.signedTransaction());
                        if (publication != null) {
                            // publish TSS Data
                            final TssData tssData = VerificationHelper.extractTssData(publication, blockNumber);
                            verificationDataProvider.safeUpdateTssData(tssData, true);
                        }
                    }
                    yield null;
                }
                case TRANSACTION_RESULT, TRANSACTION_OUTPUT -> {
                    outputTreeHasher.addLeaf(getBlockItemHash(item));
                    yield null;
                }
                case STATE_CHANGES -> {
                    stateChangesHasher.addLeaf(getBlockItemHash(item));
                    yield null;
                }
                case TRACE_DATA -> {
                    traceDataHasher.addLeaf(getBlockItemHash(item));
                    yield null;
                }
                case RECORD_FILE -> {
                    if (this.rawRecordFileItemProtoBytes != null) {
                        // a mandatory once per block item appearing more than once is a valid
                        // encoding, but not a processable stream
                        yield SessionFailureType.UNSUPPORTED_STREAM_FORMAT;
                    } else {
                        this.rawRecordFileItemProtoBytes = item.recordFileOrThrow();
                        outputTreeHasher.addLeaf(getBlockItemHash(item));
                        yield null;
                    }
                }
                case BLOCK_FOOTER -> {
                    if (this.blockFooter != null) {
                        // a mandatory once per block item appearing more than once is a valid
                        // encoding, but not a processable stream
                        yield SessionFailureType.UNSUPPORTED_STREAM_FORMAT;
                    } else {
                        this.blockFooter = standardParse(BlockFooter.PROTOBUF, item.blockFooter());
                        yield null;
                    }
                }
                case BLOCK_PROOF -> {
                    blockProofs.add(standardParse(BlockProof.PROTOBUF, item.blockProof()));
                    yield null;
                }
                // item types this version of the node cannot process currently
                case REDACTED_ITEM, FILTERED_SINGLE_ITEM -> SessionFailureType.UNSUPPORTED_ITEM_TYPE;
                case UNSET -> processFutureItem(item, unknownFields);
            };
        }
        return failure;
    }

    /// Handles an item whose type is unknown to the compiled schema, applying the block stream
    /// forward compatibility numbering rule: for a field numbered 20 or above, the hashing
    /// category is the field number modulo 20. Categories that map to a defined subtree are
    /// hashed like any other item, not-hashed categories are read and ignored, and everything
    /// else refuses the block, because guessing could produce a hash that disagrees with an
    /// upgraded node.
    /// @param item the block item carrying the unknown field
    /// @param unknownFields the unknown fields of the item
    /// @return null on success, or the failure type when the block must be refused
    private SessionFailureType processFutureItem(final BlockItemUnparsed item, final List<UnknownField> unknownFields) {
        final SessionFailureType failure;
        if (unknownFields.isEmpty()) {
            // an item with no field at all, nothing valid to process
            failure = SessionFailureType.UNKNOWN_ERROR;
        } else if (unknownFields.size() > 1) {
            // a BlockItem is a oneof, so a valid item carries exactly one field; more than one
            // unknown field parses fine, but is not a processable stream
            failure = SessionFailureType.UNSUPPORTED_STREAM_FORMAT;
        } else {
            final int fieldNumber = unknownFields.getFirst().field();
            if (fieldNumber < FIRST_FORWARD_COMPATIBLE_FIELD_NUMBER) {
                // an unknown field below 20 is a first release field reserved for item types that
                // require specific handling this version does not know
                hashingMetrics.futureItemsRefused().increment();
                failure = SessionFailureType.UNSUPPORTED_ITEM_TYPE;
            } else {
                final int category = fieldNumber % CATEGORY_MODULUS;
                failure = switch (category) {
                    case 0, 19 -> {
                        // not part of the block proof merkle tree, read and ignore
                        hashingMetrics.futureItemsNotHashed().increment();
                        yield null;
                    }
                    case 1, 2 -> {
                        // requires specific handling
                        hashingMetrics.futureItemsRefused().increment();
                        yield SessionFailureType.UNSUPPORTED_ITEM_TYPE;
                    }
                    case 3 -> hashFutureItem(item, consensusHeaderHasher);
                    case 4 -> hashFutureItem(item, inputTreeHasher);
                    case 5 -> hashFutureItem(item, outputTreeHasher);
                    case 6 -> hashFutureItem(item, stateChangesHasher);
                    case 7 -> hashFutureItem(item, traceDataHasher);
                    case 8 -> hashFutureItem(item, extensionHasherZero);
                    case 9 -> hashFutureItem(item, extensionHasherOne);
                    case 10 -> hashFutureItem(item, extensionHasherTwo);
                    case 11 -> hashFutureItem(item, extensionHasherThree);
                    case 12 -> hashFutureItem(item, extensionHasherFour);
                    case 13 -> hashFutureItem(item, extensionHasherFive);
                    case 14 -> hashFutureItem(item, extensionHasherSix);
                    case 15 -> hashFutureItem(item, extensionHasherSeven);
                    // categories 16 to 18 are reserved with no subtree in the block root tree
                    case 16, 17, 18 -> {
                        // categories 16 to 18 are reserved with no subtree in the block root tree
                        hashingMetrics.futureItemsRefused().increment();
                        yield SessionFailureType.UNSUPPORTED_ITEM_TYPE;
                    }
                    default -> SessionFailureType.UNKNOWN_ERROR;
                };
            }
        }
        return failure;
    }

    /// Hashes a future item into the given subtree hasher.
    /// @param item the block item to hash
    /// @param hasher the subtree hasher the item's category maps to
    /// @return always null, the item was hashed successfully
    private SessionFailureType hashFutureItem(final BlockItemUnparsed item, final NaiveStreamingTreeHasher hasher) {
        hasher.addLeaf(getBlockItemHash(item));
        hashingMetrics.futureItemsHashed().increment();
        return null;
    }

    /// Returns the given extension subtree hasher, or a new one when it does not exist yet.
    /// @param hasher the current extension subtree hasher, or null when not yet created
    /// Finish the hashing operation.
    /// This method will finalize the hashing process. Root hash will be calculated and
    /// a [HashingResult] will be returned.
    /// @throws VerificationSessionFailedException in case a known failure occurs
    private HashingResult finalHashingResult() throws NoSuchAlgorithmException {
        final HashingResult hashingResult;
        if (blockHeader == null || blockFooter == null || blockProofs.isEmpty()) {
            throw new VerificationSessionFailedException(
                    blockNumber, SessionFailureType.MISSING_MANDATORY_ITEM, blockSource);
        } else {
            final Timestamp timestamp = blockHeader.blockTimestamp();
            final Bytes rootOfAllPreviousBlockHashes = blockFooter.rootHashOfAllBlockHashesTree();
            final Bytes previousBlockHash = blockFooter.previousBlockRootHash();
            final Bytes startOfBlockStateRootHash = blockFooter.startOfBlockStateRootHash();
            if (validFields(timestamp, rootOfAllPreviousBlockHashes, previousBlockHash, startOfBlockStateRootHash)) {
                final Bytes blockRootHash = HashingUtilities.computeFinalBlockHash(
                        timestamp,
                        previousBlockHash,
                        rootOfAllPreviousBlockHashes,
                        startOfBlockStateRootHash,
                        inputTreeHasher,
                        outputTreeHasher,
                        consensusHeaderHasher,
                        stateChangesHasher,
                        traceDataHasher,
                        extensionHasherZero,
                        extensionHasherOne,
                        extensionHasherTwo,
                        extensionHasherThree,
                        extensionHasherFour,
                        extensionHasherFive,
                        extensionHasherSix,
                        extensionHasherSeven);
                final BlockUnparsed block = BlockUnparsed.newBuilder()
                        .blockItems(accumulatedBlockItems)
                        .build();
                final List<BlockProof> proofs = Collections.unmodifiableList(blockProofs);
                final byte[] signedWRBPayload = rawRecordFileItemProtoBytes == null
                        ? null
                        : computeWRBSignedPayload(rawRecordFileItemProtoBytes);
                hashingResult = new HashingResult(
                        blockNumber,
                        blockSource,
                        block,
                        blockRootHash,
                        blockHeader,
                        blockFooter,
                        proofs,
                        hapiProtoVersion,
                        signedWRBPayload);
            } else {
                throw new VerificationSessionFailedException(
                        blockNumber, SessionFailureType.MISSING_MANDATORY_FIELD, blockSource);
            }
        }
        return hashingResult;
    }

    ///  Find and parse ledger id publication.
    private LedgerIdPublicationTransactionBody findLedgerIdPublication(final Bytes signedTxBytes)
            throws ParseException {
        if (signedTxBytes == null || signedTxBytes.length() == 0) {
            return null;
        } else {
            final SignedTransaction signedTx =
                    standardParse(SignedTransaction.PROTOBUF, signedTxBytes, Integer.MAX_VALUE);
            final TransactionBody body =
                    standardParse(TransactionBody.PROTOBUF, signedTx.bodyBytes(), Integer.MAX_VALUE);
            return body.ledgerIdPublication();
        }
    }

    /// Validate that required fields are present.
    /// @return `true` iff all fields are valid and present
    private boolean validFields(
            final Timestamp timestamp,
            final Bytes rootOfAllPreviousBlockHashes,
            final Bytes previousBlockHash,
            final Bytes startOfBlockStateRootHash) {
        return timestamp != null
                && rootOfAllPreviousBlockHashes != null
                && rootOfAllPreviousBlockHashes != Bytes.EMPTY
                && previousBlockHash != null
                && previousBlockHash != Bytes.EMPTY
                && startOfBlockStateRootHash != null
                && startOfBlockStateRootHash != Bytes.EMPTY;
    }

    /// Compute the WRB signed payload
    /// @return a `byte[]` containing the WRB signed payload
    private byte[] computeWRBSignedPayload(final Bytes rawRecordFileBytes) throws NoSuchAlgorithmException {
        final Bytes extracted = extractRecordStreamFileBytes(rawRecordFileBytes);
        if (extracted.length() == 0) {
            throw new VerificationSessionFailedException(
                    blockNumber, SessionFailureType.MISSING_MANDATORY_FIELD, blockSource);
        } else {
            return computeV6SignedPayload(extracted);
        }
    }

    /// Extracts the raw `record_file_contents` bytes from a serialized `RecordFileItem`
    /// proto message by walking the protobuf wire format directly, without deserializing the message.
    ///
    /// `record_file_contents` is proto field 2 of `RecordFileItem`. These bytes are
    /// the verbatim content of the `.rcd` record stream file exactly as the consensus node
    /// read it from disk when it computed the V6 signed hash. They must be returned byte-for-byte
    /// identical to what the consensus node used; full deserialization via
    /// `RecordFileItem.PROTOBUF.parse()` is deliberately avoided because re-serializing a
    /// parsed object can produce subtly different bytes (e.g. omitting default-value fields, different
    /// varint encoding choices), which would cause the recomputed hash to diverge from the one the
    /// consensus node signed.
    ///
    /// **Protobuf wire format:** every field on the wire is encoded as a tag varint followed
    /// by its value. The tag packs two things:
    /// - `fieldNumber = tag >>> 3`
    /// - `wireType = tag & 0x7`
    ///
    /// Wire type 2 (`LEN`) means the value is length-prefixed bytes, used for `bytes`,
    /// `string`, and embedded messages. It is encoded as:
    /// `[tag varint] [length varint] [raw bytes...]`.
    ///
    /// **Algorithm:**
    /// 1. Read the next field tag varint and decode its field number and wire type.
    /// 2. If `fieldNumber == 2` and `wireType == LEN`: read the length prefix varint,
    ///    read exactly that many bytes, and return them - these are the
    ///    `record_file_contents`.
    /// 3. Otherwise skip the field using the wire type to know how many bytes to consume:
    ///    - VARINT (wire 0): read and discard one varint
    ///    - I64 (wire 1): skip 8 bytes fixed
    ///    - LEN (wire 2): read the length prefix, skip that many bytes
    ///    - I32 (wire 5): skip 4 bytes fixed
    /// 4. Repeat until field 2 is found or input is exhausted.
    ///
    /// @param recordFileItemBytes raw serialized bytes of a `RecordFileItem` proto message
    /// @return verbatim bytes of the `record_file_contents` field (proto field 2), or
    ///         `Bytes.EMPTY` if field 2 is not present or if any parse error occurs
    private Bytes extractRecordStreamFileBytes(final Bytes recordFileItemBytes) {
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
                // Not field 2 - skip this field using its wire type to advance the cursor correctly
                switch (wireType) {
                    case 0 -> input.readVarLong(false); // VARINT: read and discard the value
                    case 1 -> input.skip(8); // I64: fixed 64-bit, skip 8 bytes
                    case 2 -> { // LEN: read length prefix, skip content
                        final int l = input.readVarInt(false);
                        input.skip(l);
                    }
                    case 5 -> input.skip(4); // I32: fixed 32-bit, skip 4 bytes
                    default -> {
                        return Bytes.EMPTY; // Unknown wire type - bail out safely
                    }
                }
            }
        } catch (final RuntimeException e) {
            throw new VerificationSessionFailedException(
                    blockNumber, SessionFailureType.UNABLE_TO_PARSE, blockSource, e);
        }
        return Bytes.EMPTY; // field 2 not present in the message
    }

    /// Computes the V6 RSA signed payload: `SHA-384(int32(6) || rawRecordStreamFileBytes)`.
    ///
    /// Delegates to [HashingUtilities#computeV6SignedPayload] so the verifier and the block-signing
    /// library share one definition of the payload.
    ///
    /// @param rawRecordStreamFileBytes raw bytes of the `record_file_contents` field
    /// @return 48-byte SHA-384 digest
    private byte[] computeV6SignedPayload(final Bytes rawRecordStreamFileBytes) {
        return HashingUtilities.computeV6SignedPayload(rawRecordStreamFileBytes);
    }
}
