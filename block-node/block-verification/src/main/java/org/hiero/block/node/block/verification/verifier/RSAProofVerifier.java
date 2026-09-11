// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.InvalidKeyException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.block.verification.metrics.ProofVerificationMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;

/// RSA proof verifier.
public final class RSAProofVerifier implements ProofVerifier {
    /// Logger for the verifier.
    private static final System.Logger LOGGER = System.getLogger(RSAProofVerifier.class.getName());
    /// Cancellation flag shared with the owning session.
    private final AtomicBoolean isCanceled;
    /// Metrics for proof verification results.
    private final ProofVerificationMetrics proofVerificationMetrics;
    /// The number of the block being verified, used for logging.
    private final long blockNumber;
    /// Map from `node_id` to RSA [PublicKey], resolved from the address book era covering the block.
    private final Map<Long, PublicKey> rsaKeyByNodeId;
    /// The signed record file proof to verify.
    private final SignedRecordFileProof proof;
    /// The record file format version declared by the proof.
    private final int version;
    /// The whole block being verified, carrying the `RECORD_FILE` item whose contents the
    /// signed payload is computed from.
    private final BlockUnparsed block;
    /// The HAPI protocol version from the block header, needed for the legacy v2/v5 payload
    /// reconstructions (see [RecordFileSignedPayload]).
    private final SemanticVersion hapiProtoVersion;
    /// The `SHA384withRSA` signature engine used to verify each signature.
    private final Signature sha384WithRSA;

    /// The outcome of the signed payload computation: exactly one of the two components is
    /// non-null. Carries either the computed payload or the failure the block must be refused
    /// with.
    private record SignedPayloadResult(byte[] payload, SessionFailureType failure) {}

    /// Constructor.
    ///
    /// @param isCanceled cancellation flag shared with the owning session, must not be null
    /// @param proofVerificationMetrics metrics for proof verification results, must not be null
    /// @param blockNumber the number of the block being verified
    /// @param rsaKeyByNodeId map from `node_id` to RSA [PublicKey] for the era covering the
    ///     block; an empty map fails verification, must not be null
    /// @param proof the signed record file proof to verify
    /// @param block the whole block being verified, carrying the `RECORD_FILE` item, must not
    ///     be null
    /// @param hapiProtoVersion the HAPI protocol version from the block header, must not be null
    /// @param sha384WithRSA the `SHA384withRSA` signature engine, must not be null
    public RSAProofVerifier(
            final AtomicBoolean isCanceled,
            final ProofVerificationMetrics proofVerificationMetrics,
            final long blockNumber,
            final Map<Long, PublicKey> rsaKeyByNodeId,
            final SignedRecordFileProof proof,
            final BlockUnparsed block,
            final SemanticVersion hapiProtoVersion,
            final Signature sha384WithRSA) {
        this.isCanceled = Objects.requireNonNull(isCanceled);
        this.proofVerificationMetrics = Objects.requireNonNull(proofVerificationMetrics);
        this.blockNumber = blockNumber;
        this.rsaKeyByNodeId = Objects.requireNonNull(rsaKeyByNodeId);
        this.proof = proof;
        this.version = proof.version();
        this.block = Objects.requireNonNull(block);
        this.hapiProtoVersion = Objects.requireNonNull(hapiProtoVersion);
        this.sha384WithRSA = Objects.requireNonNull(sha384WithRSA);
    }

    /// {@inheritDoc}
    /// ---
    /// Verifies a `SignedRecordFileProof` (WRB RSA proof) of record file format version 2, 5
    /// or 6.
    ///
    /// **Algorithm:**
    /// 1. Compute the block root hash for chain continuity (identical to the TSS path, done in
    ///    the hashing stage upstream).
    /// 2. Locate the `RECORD_FILE` item in the block and extract its `record_file_contents`
    ///    bytes (proto field 2).
    /// 3. Compute the signed payload for the version declared by the proof, see
    ///    [RecordFileSignedPayload] for the per-version constructions; the wrap CLI preserves
    ///    the source `recordFormatVersion` on `SignedRecordFileProof.version` because V2/V5
    ///    signatures were computed over the legacy binary serializations, not the normalized
    ///    V6 protobuf.
    /// 4. For each `RecordFileSignature` entry:
    ///    - Skip if `node_id` not in `rsaKeyByNodeId` (increment roster-mismatch counter).
    ///    - Skip if signature bytes are all zeros (defensive pre-filter).
    ///    - Verify with `SHA384withRSA` over the signed payload; the verification mechanics
    ///      are identical for every version, only the payload construction differs. If
    ///      verification fails or throws, **reject the block immediately** - both the CN (V6)
    ///      and the wrap CLI (V2/V5) only include signatures already validated against the
    ///      signed hash, so any included signature must be cryptographically valid.
    /// 5. Accept if at least one signature verified. Signatures from nodes not in
    ///      the local roster are skipped (see @todo(2808)) and tallied into a single
    ///      batched `rsa_roster_mismatch_total` increment; step 4 already rejects the
    ///      block on any failed signature from a known node, so reaching this step
    ///      means every verifiable signature passed.
    @Override
    public SessionFailureType verify() {
        final SessionFailureType result;
        // Guard: no era in the address book history covers this block number
        if (rsaKeyByNodeId.isEmpty()) {
            LOGGER.log(
                    WARNING,
                    "No address book era covers block {0} - cannot verify RSA WRB proof."
                            + " Ensure rsa-address-book-history.json is loaded and covers this block number.",
                    blockNumber);
            result = SessionFailureType.MISSING_VERIFICATION_DATA;
        } else {
            final SignedPayloadResult payloadResult = computeSignedWRBPayload();
            final byte[] signedWRBPayload = payloadResult.payload();
            if (signedWRBPayload == null) {
                result = payloadResult.failure();
            } else {
                // Verify each signature over the version-appropriate payload and count valid ones.
                // Track which node_id values have already contributed a valid signature to prevent
                // a duplicate entry in the proof from inflating validCount.
                final int rosterSize = rsaKeyByNodeId.size();
                int validCount = 0;
                int mismatchCount = 0;
                final Set<Long> validatedNodes = new HashSet<>();
                for (final RecordFileSignature sig : proof.recordFileSignatures()) {
                    if (isCanceled()) {
                        proofVerificationMetrics.rsaFailure().increment();
                        return SessionFailureType.CANCELLED;
                    } else {
                        final long nodeId = sig.nodeId();
                        // uses a historical roster keyed by block number so signatures
                        // from nodes that were valid at the time the block was produced are verified correctly
                        // across address-book transitions, skipping unknown node IDs can still occur.
                        final PublicKey publicKey = rsaKeyByNodeId.get(nodeId);
                        if (publicKey == null) {
                            mismatchCount++;
                            LOGGER.log(
                                    DEBUG,
                                    "Signature from node {0} not in era address book for block {1} - skipped",
                                    nodeId,
                                    blockNumber);
                            continue;
                        }
                        if (validatedNodes.contains(nodeId)) {
                            LOGGER.log(
                                    DEBUG,
                                    "Duplicate signature from node {0} in block {1} - skipped",
                                    nodeId,
                                    blockNumber);
                            continue;
                        }
                        final byte[] sigBytes = sig.signaturesBytes().toByteArray();
                        if (isAllZeros(sigBytes)) {
                            LOGGER.log(
                                    DEBUG,
                                    "Zeroed signature from node {0} in block {1} - skipped",
                                    nodeId,
                                    blockNumber);
                            continue;
                        }
                        try {
                            final Signature engine = sha384WithRSA;
                            engine.initVerify(publicKey);
                            engine.update(signedWRBPayload);
                            if (engine.verify(sigBytes)) {
                                validCount++;
                                validatedNodes.add(nodeId);
                            } else {
                                // CN only includes signatures from consensus-contributing nodes, so a failed
                                // cryptographic verification means the block or proof has been tampered with.
                                LOGGER.log(
                                        DEBUG,
                                        "RSA signature from node {0} failed verification in block {1} - rejecting block",
                                        nodeId,
                                        blockNumber);
                                proofVerificationMetrics.rsaFailure().increment();
                                return SessionFailureType.BAD_BLOCK_PROOF;
                            }
                        } catch (final InvalidKeyException | SignatureException e) {
                            LOGGER.log(
                                    WARNING,
                                    "RSA verification error for node {0} in block {1}: {2} - rejecting block",
                                    nodeId,
                                    blockNumber,
                                    e.getMessage());
                            proofVerificationMetrics.rsaFailure().increment();
                            return SessionFailureType.BAD_BLOCK_PROOF;
                        }
                    }
                }
                if (mismatchCount > 0) {
                    proofVerificationMetrics.rsaRosterMismatch().increment(mismatchCount);
                }
                // Acceptance threshold: every signature present in the proof passes validation,
                // and at least one such signature exists. Signatures from nodes not in the local
                // roster are skipped (see @todo(2808)).
                // Because we fail fast on any failed verification, reaching this
                // point means all verifiable signatures passed.
                final boolean accepted = validCount > 0;
                if (accepted) {
                    LOGGER.log(
                            DEBUG,
                            "RSA WRB proof accepted for block {0}: {1} valid signatures (roster size {2})",
                            blockNumber,
                            validCount,
                            rosterSize);
                    result = null;
                } else {
                    LOGGER.log(
                            WARNING,
                            "RSA WRB proof rejected for block {0}: {1} valid signatures (roster size {2})",
                            blockNumber,
                            validCount,
                            rosterSize);
                    result = SessionFailureType.BAD_BLOCK_PROOF;
                }
            }
        }
        if (result != null) {
            proofVerificationMetrics.rsaFailure().increment();
        } else {
            proofVerificationMetrics.rsaSuccess().increment();
        }
        return result;
    }

    /// Computes the signed payload for the proof's declared record file format version from
    /// the block's `RECORD_FILE` item, or the failure the block must be refused with:
    /// - no `RECORD_FILE` item in the block: `MISSING_VERIFICATION_DATA`
    /// - unsupported version (outside 2, 5 and 6): `MISSING_MANDATORY_FIELD`
    /// - `record_file_contents` absent or missing components mandatory for the version
    ///   (the running hashes of the legacy reconstructions): `MISSING_MANDATORY_FIELD`
    /// - malformed protobuf wire data: `UNABLE_TO_PARSE`
    ///
    /// @return the payload or the failure, exactly one of the two, never both
    private SignedPayloadResult computeSignedWRBPayload() {
        final SignedPayloadResult result;
        final Bytes rawRecordFileItemBytes = findRecordFileItemBytes();
        if (rawRecordFileItemBytes == null) {
            LOGGER.log(WARNING, "WRB block {0} carries no RECORD_FILE item to verify the proof against", blockNumber);
            result = new SignedPayloadResult(null, SessionFailureType.MISSING_VERIFICATION_DATA);
        } else if (!RecordFileSignedPayload.isSupportedVersion(version)) {
            LOGGER.log(
                    WARNING,
                    "Unsupported SignedRecordFileProof version {0} in block {1}"
                            + " - only versions 2, 5 and 6 are supported",
                    version,
                    blockNumber);
            result = new SignedPayloadResult(null, SessionFailureType.MISSING_MANDATORY_FIELD);
        } else {
            SignedPayloadResult computed;
            try {
                final Bytes recordFileContents = extractRecordStreamFileBytes(rawRecordFileItemBytes);
                if (recordFileContents.length() == 0) {
                    LOGGER.log(WARNING, "WRB block {0} carries no record_file_contents", blockNumber);
                    computed = new SignedPayloadResult(null, SessionFailureType.MISSING_MANDATORY_FIELD);
                } else {
                    final byte[] payload =
                            RecordFileSignedPayload.computeSignedPayload(version, hapiProtoVersion, recordFileContents);
                    if (payload == null) {
                        // the contents are missing components mandatory for this version
                        LOGGER.log(
                                WARNING,
                                "WRB block {0} record_file_contents miss components mandatory for version {1}",
                                blockNumber,
                                version);
                        computed = new SignedPayloadResult(null, SessionFailureType.MISSING_MANDATORY_FIELD);
                    } else {
                        computed = new SignedPayloadResult(payload, null);
                    }
                }
            } catch (final ParseException e) {
                LOGGER.log(
                        WARNING,
                        "WRB block %d record_file_contents are malformed - rejecting block".formatted(blockNumber),
                        e);
                computed = new SignedPayloadResult(null, SessionFailureType.UNABLE_TO_PARSE);
            }
            result = computed;
        }
        return result;
    }

    /// Finds the raw serialized bytes of the block's `RecordFileItem` proto message.
    ///
    /// @return the raw `RECORD_FILE` item bytes, or `null` when the block carries no such item
    private Bytes findRecordFileItemBytes() {
        Bytes result = null;
        for (final BlockItemUnparsed item : block.blockItems()) {
            if (item.hasRecordFile()) {
                result = item.recordFile();
                break;
            }
        }
        return result;
    }

    /// Extracts the raw `record_file_contents` bytes from a serialized `RecordFileItem`
    /// proto message by walking the protobuf wire format directly, without deserializing the message.
    ///
    /// `record_file_contents` is proto field 2 of `RecordFileItem`. These bytes are
    /// the normalized `RecordStreamFile` content exactly as the wrap CLI serialized it. They
    /// must be returned byte-for-byte identical to what the signed payload was computed over;
    /// full deserialization via `RecordFileItem.PROTOBUF.parse()` is deliberately avoided
    /// because re-serializing a parsed object can produce subtly different bytes (e.g. omitting
    /// default-value fields, different varint encoding choices), which would cause the
    /// recomputed payload to diverge from the one the consensus nodes signed.
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
    ///         [Bytes#EMPTY] if field 2 is not present or an unknown wire type is encountered
    /// @throws ParseException if the wire data is malformed (e.g. truncated)
    private static Bytes extractRecordStreamFileBytes(final Bytes recordFileItemBytes) throws ParseException {
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
            throw new ParseException(e);
        }
        return Bytes.EMPTY; // field 2 not present in the message
    }

    /// Returns `true` if every byte in `bytes` is zero.
    ///
    /// @param bytes the byte array to inspect
    /// @return `true` when the array is all zeros or empty
    private static boolean isAllZeros(final byte[] bytes) {
        for (final byte b : bytes) {
            if (b != 0) return false;
        }
        return true;
    }

    /// Returns `true` if the owning session has been cancelled or the current thread interrupted.
    private boolean isCanceled() {
        return isCanceled.get() || Thread.currentThread().isInterrupted();
    }
}
