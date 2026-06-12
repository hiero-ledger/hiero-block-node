// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.hasher;

import static org.hiero.block.common.hasher.HashingUtilities.getBlockItemHash;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
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
    /// Max protobuf parse depth: each level of message nesting needs >= ~8 bytes on the wire,
    /// so size/8 bounds the deepest a non-degenerate message can nest.
    private static final int MAX_BLOCK_MESSAGE_DEPTH = Integer.MAX_VALUE / 8;
    private static final long DATA_BUSY_WAIT_TIME_NANOS = TimeUnit.MICROSECONDS.toNanos(200);
    private final long blockNumber;
    private final BlockSource blockSource;
    private final AtomicBoolean isCanceled;
    private final HashingMetrics hashingMetrics;
    private final ConcurrentLinkedDeque<BlockItems> blockItemsRecordsDeque;
    private final List<BlockItemUnparsed> accumulatedBlockItems;
    private final List<BlockProof> blockProofs;
    private final NaiveStreamingTreeHasher inputTreeHasher;
    private final NaiveStreamingTreeHasher outputTreeHasher;
    private final NaiveStreamingTreeHasher consensusHeaderHasher;
    private final NaiveStreamingTreeHasher stateChangesHasher;
    private final NaiveStreamingTreeHasher traceDataHasher;
    private final VerificationDataProvider verificationDataProvider;
    private BlockHeader blockHeader;
    private BlockFooter blockFooter;
    private SemanticVersion hapiProtoVersion;
    private Bytes rawRecordFileItemProtoBytes;

    /// Constructor.
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
                            final BlockItemUnparsed.ItemOneOfType kind =
                                    item.item().kind();
                            switch (kind) {
                                case BLOCK_HEADER -> {
                                    this.blockHeader = BlockHeader.PROTOBUF.parse(item.blockHeader());
                                    this.hapiProtoVersion = this.blockHeader.hapiProtoVersion();
                                    if (this.hapiProtoVersion == null) {
                                        throw new VerificationSessionFailedException(
                                                blockNumber, SessionFailureType.MISSING_MANDATORY_FIELD, blockSource);
                                    } else {
                                        outputTreeHasher.addLeaf(getBlockItemHash(item));
                                    }
                                }
                                case ROUND_HEADER, EVENT_HEADER ->
                                    consensusHeaderHasher.addLeaf(getBlockItemHash(item));
                                case SIGNED_TRANSACTION -> {
                                    inputTreeHasher.addLeaf(getBlockItemHash(item));
                                    if (blockItemsRecord.blockNumber() == 0 && !verificationDataProvider.hasTssData()) {
                                        final LedgerIdPublicationTransactionBody publication =
                                                findLedgerIdPublication(item.signedTransaction());
                                        if (publication != null) {
                                            // publish TSS Data
                                            final TssData tssData = VerificationHelper.extractTssData(publication);
                                            verificationDataProvider.safeUpdateTssData(tssData, true);
                                        }
                                    }
                                }
                                case TRANSACTION_RESULT, TRANSACTION_OUTPUT ->
                                    outputTreeHasher.addLeaf(getBlockItemHash(item));
                                case STATE_CHANGES -> stateChangesHasher.addLeaf(getBlockItemHash(item));
                                case TRACE_DATA -> traceDataHasher.addLeaf(getBlockItemHash(item));
                                case RECORD_FILE -> {
                                    this.rawRecordFileItemProtoBytes = item.recordFileOrThrow();
                                    outputTreeHasher.addLeaf(getBlockItemHash(item));
                                }
                                case BLOCK_FOOTER -> this.blockFooter = BlockFooter.PROTOBUF.parse(item.blockFooter());
                                case BLOCK_PROOF -> {
                                    final BlockProof blockProof = BlockProof.PROTOBUF.parse(item.blockProof());
                                    blockProofs.add(blockProof);
                                }
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

    /// Finish the hashing operation.
    /// This method will finalize the hashing process. Root hash will be calculated and
    /// a [HashingResult] will be returned.
    /// @throws VerificationSessionFailedException in case a known failure occurs
    private HashingResult finalHashingResult() throws NoSuchAlgorithmException {
        final HashingResult hashingResult;
        if (blockHeader == null || blockFooter == null || blockProofs.isEmpty()) {
            // todo(2528) validate that when we see an WRB we have:
            //    1x Block Header, 1x RECORD_ITEM, 1x Block Footer, N number (at least 1) Block Proof
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
                        traceDataHasher);
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
            final SignedTransaction signedTx = SignedTransaction.PROTOBUF.parse(
                    signedTxBytes.toReadableSequentialData(), false, true, MAX_BLOCK_MESSAGE_DEPTH, Integer.MAX_VALUE);
            final TransactionBody body = TransactionBody.PROTOBUF.parse(
                    signedTx.bodyBytes().toReadableSequentialData(),
                    false,
                    true,
                    MAX_BLOCK_MESSAGE_DEPTH,
                    Integer.MAX_VALUE);
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
    ///
    ///     - `fieldNumber = tag >>> 3`
    ///   - `wireType= tag & 0x7`
    ///
    /// Wire type 2 (`LEN`) means the value is length-prefixed bytes, used for `bytes`,
    /// `string`, and embedded messages. It is encoded as:
    /// `[tag varint] [length varint] [raw bytes...]`.
    ///
    /// **Algorithm:**
    /// <ol>
    ///     - Read the next field tag varint and decode its field number and wire type.
    ///     - If `fieldNumber == 2` and `wireType == LEN`: read the length prefix varint,
    ///     read exactly that many bytes, and return them — these are the
    ///     `record_file_contents`.
    ///     - Otherwise skip the field using the wire type to know how many bytes to consume:
    ///
    ///   - VARINT (wire 0): read and discard one varint
    ///       - I64 (wire 1): skip 8 bytes fixed
    ///       - LEN (wire 2): read the length prefix, skip that many bytes
    ///       - I32 (wire 5): skip 4 bytes fixed
    ///
    ///
    ///     - Repeat until field 2 is found or input is exhausted.
    /// </ol>
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
            throw new VerificationSessionFailedException(
                    blockNumber, SessionFailureType.UNABLE_TO_PARSE, blockSource, e);
        }
        return Bytes.EMPTY; // field 2 not present in the message
    }

    /// Computes the V6 RSA signed payload: `SHA-384(int32(6) || rawRecordStreamFileBytes)`.
    ///
    /// This matches `SignatureDataExtractor.computeSignedHash(6, ...)` in
    /// `tools-and-tests/tools`. Inlined here because that module cannot be imported from
    /// `block-node/verification`.
    ///
    /// @param rawRecordStreamFileBytes raw bytes of the `record_file_contents` field
    /// @return 48-byte SHA-384 digest
    private byte[] computeV6SignedPayload(final Bytes rawRecordStreamFileBytes) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-384");
        digest.reset();
        digest.update(new byte[] {0, 0, 0, 6}); // int32(6) big-endian
        rawRecordStreamFileBytes.writeTo(digest);
        return digest.digest();
    }
}
