// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.VerificationProofMetrics;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for the RSA `SignedRecordFileProof` verification path in
 * `ExtendedMerkleTreeSession`.
 *
 * <p>Tests use synthetic blocks and in-process RSA key pairs — no real testnet blocks are required.
 * The acceptance rule is: at least one signature in the proof must verify, AND every signature
 * present must succeed — any single failed cryptographic verification
 * causes immediate block rejection regardless of how many other sigs are valid.
 *
 * <p>The raw `RecordFileItem` proto is assembled manually using protobuf wire format so that
 * `extractRecordStreamFileBytes` is exercised by the field-2 extraction path.
 */
class RsaWrbVerificationTest {

    /** Number of nodes in the fixed roster used by most tests. */
    private static final int ROSTER_SIZE = 6;

    /**
     * Extended key pool size for the parametrised roster-size test.
     * Covers all roster sizes exercised in `oneValidSignatureSuffices_zeroRejected`.
     */
    private static final int MAX_KEY_POOL = 9;

    /** HAPI version >= 0.72.0 routes to `ExtendedMerkleTreeSession`. */
    private static final SemanticVersion HAPI_VERSION = new SemanticVersion(1, 0, 0, "", "");

    private static final SemanticVersion SW_VERSION = new SemanticVersion(1, 0, 0, "", "");
    private static final Timestamp BLOCK_TIMESTAMP = new Timestamp(1_700_000_000L, 0);
    private static final long BLOCK_NUMBER = 42L;

    /**
     * Raw bytes used as the `record_file_contents` (field 2) of the test `RecordFileItem`.
     * These represent a minimal placeholder — only the bytes matter for the hash computation.
     */
    private static final byte[] RECORD_STREAM_FILE_BYTES = "test-record-stream-file-v6-content".getBytes();

    /** RSA public keys indexed by node_id (0 .. ROSTER_SIZE-1). Used by the existing 6-node tests. */
    private static final Map<Long, PublicKey> KEY_MAP = new HashMap<>();

    /** RSA private keys indexed by node_id (0 .. ROSTER_SIZE-1). Used by the existing 6-node tests. */
    private static final Map<Long, PrivateKey> PRIVATE_KEY_MAP = new HashMap<>();

    /**
     * Extended public key pool covering node_ids 0 .. MAX_KEY_POOL-1.
     * Used exclusively by the parametrised roster-size test so it can build rosters of varying sizes
     * without affecting the existing 6-node test fixtures.
     */
    private static final Map<Long, PublicKey> EXTENDED_KEY_MAP = new HashMap<>();

    /** Extended private key pool paired with `EXTENDED_KEY_MAP`. */
    private static final Map<Long, PrivateKey> EXTENDED_PRIVATE_KEY_MAP = new HashMap<>();

    /** Serialized `RecordFileItem` proto with field 2 = `RECORD_STREAM_FILE_BYTES`. */
    private static Bytes recordFileItemBytes;

    /** V6 signed payload for the above record file: SHA-384(int32(6) || RECORD_STREAM_FILE_BYTES). */
    private static byte[] signedPayload;

    @BeforeAll
    static void generateKeysAndPayload() throws Exception {
        // 2048-bit RSA keys are used for test speed only — production network uses 4096-bit keys.
        // CodeQL flags anything below 2048 as a weak-key warning even in test code, so we stay at 2048.
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        // Generate the 6-node pool used by the fixed-roster tests.
        for (long nodeId = 0; nodeId < ROSTER_SIZE; nodeId++) {
            final KeyPair kp = kpg.generateKeyPair();
            KEY_MAP.put(nodeId, kp.getPublic());
            PRIVATE_KEY_MAP.put(nodeId, kp.getPrivate());
        }
        // Generate the extended pool (MAX_KEY_POOL nodes) for the parametrised roster-size tests.
        for (long nodeId = 0; nodeId < MAX_KEY_POOL; nodeId++) {
            final KeyPair kp = kpg.generateKeyPair();
            EXTENDED_KEY_MAP.put(nodeId, kp.getPublic());
            EXTENDED_PRIVATE_KEY_MAP.put(nodeId, kp.getPrivate());
        }

        // Build a minimal RecordFileItem proto: field 2 (tag=18, wire=LEN) = RECORD_STREAM_FILE_BYTES
        // Proto wire format: tag_varint | length_varint | bytes
        // tag = (2 << 3) | 2 = 18 = 0x12
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(0x12); // tag: field 2, wire type 2 (LEN)
        bos.write(RECORD_STREAM_FILE_BYTES.length); // length fits in one byte (< 128)
        bos.write(RECORD_STREAM_FILE_BYTES);
        recordFileItemBytes = Bytes.wrap(bos.toByteArray());

        // V6 signed payload: SHA-384(int32(6) || RECORD_STREAM_FILE_BYTES)
        final MessageDigest digest = MessageDigest.getInstance("SHA-384");
        digest.update(new byte[] {0, 0, 0, 6});
        digest.update(RECORD_STREAM_FILE_BYTES);
        signedPayload = digest.digest();
    }

    // ---- Helper methods -----------------------------------------------------------------------

    /**
     * Signs `signedPayload` with the RSA private key of the given node and returns the
     * signature bytes.
     */
    private static byte[] sign(final long nodeId) throws Exception {
        final Signature engine = Signature.getInstance("SHA384withRSA");
        engine.initSign(PRIVATE_KEY_MAP.get(nodeId));
        engine.update(signedPayload);
        return engine.sign();
    }

    /**
     * Builds a `BlockItemUnparsed` list representing a minimal WRB block:
     * `BLOCK_HEADER | RECORD_FILE | BLOCK_FOOTER | BLOCK_PROOF`.
     *
     * @param signatures the list of `RecordFileSignature` entries to include in the proof
     * @return list of unparsed block items ready to pass to `processBlockItems`
     */
    private static List<BlockItemUnparsed> buildWrbBlock(final List<RecordFileSignature> signatures) {
        final BlockHeader header =
                new BlockHeader(HAPI_VERSION, SW_VERSION, BLOCK_NUMBER, BLOCK_TIMESTAMP, BlockHashAlgorithm.SHA2_384);
        final BlockFooter footer = new BlockFooter(Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY);
        final BlockProof proof = BlockProof.newBuilder()
                .block(BLOCK_NUMBER)
                .signedRecordFileProof(new SignedRecordFileProof(6, signatures))
                .build();

        final BlockItemUnparsed headerItem = BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(header))
                .build();
        final BlockItemUnparsed recordFileItem =
                BlockItemUnparsed.newBuilder().recordFile(recordFileItemBytes).build();
        final BlockItemUnparsed footerItem = BlockItemUnparsed.newBuilder()
                .blockFooter(BlockFooter.PROTOBUF.toBytes(footer))
                .build();
        final BlockItemUnparsed proofItem = BlockItemUnparsed.newBuilder()
                .blockProof(BlockProof.PROTOBUF.toBytes(proof))
                .build();

        return List.of(headerItem, recordFileItem, footerItem, proofItem);
    }

    /**
     * Builds a `BlockItemUnparsed` list representing a WRB block with multiple `BLOCK_PROOF`
     * items, one per `SignedRecordFileProof`. Used to test the multi-proof verification path.
     *
     * @param signaturesPerProof one signature list per `BLOCK_PROOF` item to append
     * @return list of unparsed block items ready to pass to `processBlockItems`
     */
    private static List<BlockItemUnparsed> buildWrbBlockWithMultipleProofs(
            final List<List<RecordFileSignature>> signaturesPerProof) {
        final BlockHeader header =
                new BlockHeader(HAPI_VERSION, SW_VERSION, BLOCK_NUMBER, BLOCK_TIMESTAMP, BlockHashAlgorithm.SHA2_384);
        final BlockFooter footer = new BlockFooter(Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY);

        final List<BlockItemUnparsed> items = new ArrayList<>();
        items.add(BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(header))
                .build());
        items.add(BlockItemUnparsed.newBuilder().recordFile(recordFileItemBytes).build());
        items.add(BlockItemUnparsed.newBuilder()
                .blockFooter(BlockFooter.PROTOBUF.toBytes(footer))
                .build());
        for (final List<RecordFileSignature> signatures : signaturesPerProof) {
            final BlockProof proof = BlockProof.newBuilder()
                    .block(BLOCK_NUMBER)
                    .signedRecordFileProof(new SignedRecordFileProof(6, signatures))
                    .build();
            items.add(BlockItemUnparsed.newBuilder()
                    .blockProof(BlockProof.PROTOBUF.toBytes(proof))
                    .build());
        }
        return items;
    }

    /**
     * Runs a single-call verification through `ExtendedMerkleTreeSession` using the given
     * key map and signatures.
     */
    private static VerificationNotification runVerification(
            final Map<Long, PublicKey> keyMap, final List<RecordFileSignature> signatures) throws Exception {
        final List<BlockItemUnparsed> items = buildWrbBlock(signatures);
        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, keyMap, VerificationProofMetrics.NONE);
        return session.processBlockItems(new BlockItems(items, BLOCK_NUMBER, true, true));
    }

    /** Signs with the given node IDs (from the 6-node pool) and returns a `RecordFileSignature` list. */
    private static List<RecordFileSignature> signaturesFor(final long... nodeIds) throws Exception {
        final List<RecordFileSignature> sigs = new ArrayList<>();
        for (final long nodeId : nodeIds) {
            sigs.add(new RecordFileSignature(Bytes.wrap(sign(nodeId)), nodeId));
        }
        return sigs;
    }

    /**
     * Signs `signedPayload` with the extended pool private key for `nodeId` and returns a
     * `RecordFileSignature`. Used only by the parametrised roster-size test.
     */
    private static RecordFileSignature extendedSignature(final long nodeId) throws Exception {
        final Signature engine = Signature.getInstance("SHA384withRSA");
        engine.initSign(EXTENDED_PRIVATE_KEY_MAP.get(nodeId));
        engine.update(signedPayload);
        return new RecordFileSignature(Bytes.wrap(engine.sign()), nodeId);
    }

    // ---- Tests -------------------------------------------------------------------------------

    @Test
    @DisplayName("valid proof with a single signature is accepted")
    void validProofSingleSignature_accepted() throws Exception {
        // Under the at-least-one-valid rule, a single valid signature accepts the block.
        final List<RecordFileSignature> sigs = signaturesFor(0L);
        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result, "Session must produce a notification");
        assertTrue(result.success(), "Proof with one valid signature must be accepted");
        assertNotNull(result.blockHash(), "Block hash must be set on success");
        assertNotNull(result.block(), "Block must be present on success");
    }

    @Test
    @DisplayName("valid proof with multiple signatures is accepted")
    void validProofMultipleSignatures_accepted() throws Exception {
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L);
        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result, "Session must produce a notification");
        assertTrue(result.success(), "Proof with multiple valid signatures must be accepted");
        assertNotNull(result.blockHash(), "Block hash must be set on success");
        assertNotNull(result.block(), "Block must be present on success");
    }

    @Test
    @DisplayName("valid proof with all nodes signing is accepted")
    void validProofAllNodesSigning_accepted() throws Exception {
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L, 5L);
        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result);
        assertTrue(result.success(), "Proof signed by all nodes must be accepted");
        assertNotNull(result.blockHash(), "Block hash must be set on success");
        assertNotNull(result.block(), "Block must be present on success");
    }

    @Test
    @DisplayName("valid proof with three signatures in a six-node roster is accepted")
    void validProofMinoritySignatures_accepted() throws Exception {
        // Under the at-least-one-valid rule a small number of valid signatures (well below half
        // the roster) still accepts the block, provided every signature present verifies.
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L);
        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result);
        assertTrue(result.success(), "3 valid sigs of 6 must be accepted under the at-least-one rule");
        assertNotNull(result.blockHash(), "Block hash must be set on success");
        assertNotNull(result.block(), "Block must be present on success");
    }

    @Test
    @DisplayName("all-zero signature bytes are skipped, leaving zero valid sigs → rejected")
    void allZeroSignatures_rejected() throws Exception {
        // Every entry is all-zero — the defensive pre-filter skips each, validCount stays at 0,
        // and the at-least-one-valid rule rejects the block.
        final List<RecordFileSignature> sigs = new ArrayList<>();
        for (long id = 0; id < ROSTER_SIZE; id++) {
            sigs.add(new RecordFileSignature(Bytes.wrap(new byte[256]), id));
        }
        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result);
        assertFalse(result.success(), "All-zero signatures yield zero valid sigs and must be rejected");
    }

    @Test
    @DisplayName("empty signature list in proof is rejected")
    void emptySignatureList_rejected() throws Exception {
        final VerificationNotification result = runVerification(KEY_MAP, List.of());

        assertNotNull(result);
        assertFalse(result.success(), "Empty signature list must be rejected");
    }

    // TODO(2808): both tests below cover the current skip-on-missing-key behaviour; once issue 2808
    //   (historical roster lookup by block number) is resolved, update them to verify against the
    //   roster that was active when the block was produced rather than the current local roster.

    @Test
    @DisplayName("signature from unknown node_id is skipped; rest still counted")
    void unknownNodeId_sigSkipped_othersStillCounted() throws Exception {
        // Nodes 0..4 produce valid sigs; node 99 is not in the key map and is skipped.
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);
        // Add a sig from an unknown node
        sigs.add(
                new RecordFileSignature(Bytes.wrap(sign(0L)), 99L)); // content irrelevant — node 99 absent from key map

        final Map<Long, PublicKey> keyMap = new HashMap<>(KEY_MAP); // node 99 absent
        final VerificationNotification result = runVerification(keyMap, sigs);

        assertNotNull(result);
        assertTrue(
                result.success(),
                "Sig from unknown node is skipped (no key to verify); valid sigs from known nodes still accept");
    }

    @Test
    @DisplayName("roster transition: sig from new node not yet in local AB is skipped; block still accepted")
    void rosterTransition_newNodeSigSkipped_blockAccepted() throws Exception {
        // AB1 has nodes 0..4 (5 nodes). AB2 adds node 5. Local roster is still AB1.
        // The proof arrives with 6 signatures (all AB2 nodes). Node 5 is unknown → skipped.
        // The 5 known-node signatures all verify → at-least-one-valid rule accepts the block.
        final Map<Long, PublicKey> ab1KeyMap = new HashMap<>();
        for (long id = 0; id < 5; id++) {
            ab1KeyMap.put(id, KEY_MAP.get(id));
        }
        // Build proof with all 6 signatures (nodes 0..5)
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L, 5L);
        final VerificationNotification result = runVerification(ab1KeyMap, sigs);

        assertNotNull(result);
        assertTrue(
                result.success(),
                "Sig from roster-transition node (not in local AB) must be skipped, not fail; "
                        + "5 known valid sigs still accept the block");
    }

    @Test
    @DisplayName("wrong payload signature (signed wrong data) causes immediate block rejection")
    void wrongPayloadSignature_immediatelyRejectsBlock() throws Exception {
        // CN only sends signatures from consensus-contributing nodes, so a failed RSA verify
        // means the proof or block is tampered — the block is rejected at the first failing sig.
        final List<RecordFileSignature> sigs = new ArrayList<>();
        final byte[] wrongData = "totally-wrong-payload".getBytes();
        for (long id = 0; id < ROSTER_SIZE; id++) {
            final Signature engine = Signature.getInstance("SHA384withRSA");
            engine.initSign(PRIVATE_KEY_MAP.get(id));
            engine.update(wrongData);
            sigs.add(new RecordFileSignature(Bytes.wrap(engine.sign()), id));
        }
        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result);
        assertFalse(result.success(), "Any signature failing RSA verification must cause immediate block rejection");
    }

    @Test
    @DisplayName("one bad sig among otherwise valid sigs causes immediate block rejection")
    void oneBadSigAmongValids_immediatelyRejectsBlock() throws Exception {
        // 5 valid sigs from nodes 0..4, but node 5 signs the wrong payload — the block must be
        // rejected immediately by the "any failed verify rejects" rule, regardless of how many
        // other sigs are valid.
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);
        final byte[] wrongData = "tampered-payload".getBytes();
        final Signature engine = Signature.getInstance("SHA384withRSA");
        engine.initSign(PRIVATE_KEY_MAP.get(5L));
        engine.update(wrongData);
        sigs.add(new RecordFileSignature(Bytes.wrap(engine.sign()), 5L));

        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result);
        assertFalse(
                result.success(),
                "Block must be rejected immediately when any signature from a known node fails — "
                        + "regardless of how many other sigs are valid");
    }

    @Test
    @DisplayName("empty address book (keyByNodeId empty) is rejected with error")
    void emptyAddressBook_rejected() throws Exception {
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L, 5L);
        final VerificationNotification result = runVerification(Map.of(), sigs);

        assertNotNull(result);
        assertFalse(result.success(), "An empty address book must cause rejection");
    }

    @Test
    @DisplayName("malformed DER key in address book is excluded; remaining keys still counted")
    void malformedKeyExcludedFromMap_restStillCounted() throws Exception {
        // Build a key map where node 5 has a malformed (random) key — RsaKeyDecoder would skip it.
        // Here we simulate by simply omitting node 5 from the map (as if buildKeyMap had skipped it).
        final Map<Long, PublicKey> reducedMap = new HashMap<>(KEY_MAP);
        reducedMap.remove(5L);

        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);
        final VerificationNotification result = runVerification(reducedMap, sigs);

        assertNotNull(result);
        assertTrue(result.success(), "5 valid sigs against a 5-key map must be accepted");
    }

    @Test
    @DisplayName("duplicate node_id in proof is skipped and block is accepted")
    void duplicateNodeId_skippedAndBlockAccepted() throws Exception {
        // 2 distinct valid sigs + 1 duplicate of node 0. The dedup branch must skip the
        // duplicate (defence-in-depth against an inflated validCount) without affecting the
        // accept/reject outcome under the at-least-one-valid rule.
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L);
        sigs.add(new RecordFileSignature(Bytes.wrap(sign(0L)), 0L)); // duplicate of node 0
        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result);
        assertTrue(result.success(), "Block must still be accepted when a duplicate signature is present");
    }

    @Test
    @DisplayName("multiple RSA proofs all valid are accepted")
    void multipleValidProofs_accepted() throws Exception {
        // Two RSA proofs, each with all-valid signatures — both must verify for the block to accept.
        final List<List<RecordFileSignature>> proofs =
                List.of(signaturesFor(0L, 1L, 2L, 3L), signaturesFor(0L, 1L, 2L, 3L, 4L, 5L));
        final List<BlockItemUnparsed> items = buildWrbBlockWithMultipleProofs(proofs);
        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, VerificationProofMetrics.NONE);
        final VerificationNotification result =
                session.processBlockItems(new BlockItems(items, BLOCK_NUMBER, true, true));

        assertNotNull(result);
        assertTrue(result.success(), "Block with multiple valid RSA proofs must be accepted");
        assertNotNull(result.blockHash(), "Block hash must be set on success");
        assertNotNull(result.block(), "Block must be present on success");
    }

    @Test
    @DisplayName("multiple RSA proofs with one containing a failed signature rejects the block")
    void multipleProofs_oneFailing_rejected() throws Exception {
        // First proof is fully valid; second proof contains one signature against a wrong
        // payload — that single failed verify must reject the whole block.
        final List<RecordFileSignature> secondProofSigs = signaturesFor(0L, 1L);
        final byte[] wrongData = "tampered-payload".getBytes();
        final Signature wrongEngine = Signature.getInstance("SHA384withRSA");
        wrongEngine.initSign(PRIVATE_KEY_MAP.get(2L));
        wrongEngine.update(wrongData);
        secondProofSigs.add(new RecordFileSignature(Bytes.wrap(wrongEngine.sign()), 2L));

        final List<List<RecordFileSignature>> proofs = List.of(signaturesFor(0L, 1L, 2L, 3L, 4L, 5L), secondProofSigs);
        final List<BlockItemUnparsed> items = buildWrbBlockWithMultipleProofs(proofs);
        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, VerificationProofMetrics.NONE);
        final VerificationNotification result =
                session.processBlockItems(new BlockItems(items, BLOCK_NUMBER, true, true));

        assertNotNull(result);
        assertFalse(result.success(), "Block must be rejected when any RSA proof present fails verification");
        assertNull(result.blockHash(), "Block hash must not be set on failure");
        assertNull(result.block(), "Block must not be present on failure");
    }

    @Test
    @DisplayName("unsupported proof version (V5) is rejected")
    void unsupportedVersion_rejected() throws Exception {
        final List<BlockItemUnparsed> items;
        {
            final BlockHeader header = new BlockHeader(
                    HAPI_VERSION, SW_VERSION, BLOCK_NUMBER, BLOCK_TIMESTAMP, BlockHashAlgorithm.SHA2_384);
            final BlockFooter footer = new BlockFooter(Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY);
            // Version 5 — only V6 is supported in Phase 2a
            final BlockProof proof = BlockProof.newBuilder()
                    .block(BLOCK_NUMBER)
                    .signedRecordFileProof(new SignedRecordFileProof(5, signaturesFor(0L, 1L, 2L, 3L, 4L)))
                    .build();
            items = List.of(
                    BlockItemUnparsed.newBuilder()
                            .blockHeader(BlockHeader.PROTOBUF.toBytes(header))
                            .build(),
                    BlockItemUnparsed.newBuilder()
                            .recordFile(recordFileItemBytes)
                            .build(),
                    BlockItemUnparsed.newBuilder()
                            .blockFooter(BlockFooter.PROTOBUF.toBytes(footer))
                            .build(),
                    BlockItemUnparsed.newBuilder()
                            .blockProof(BlockProof.PROTOBUF.toBytes(proof))
                            .build());
        }
        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, VerificationProofMetrics.NONE);
        final VerificationNotification result =
                session.processBlockItems(new BlockItems(items, BLOCK_NUMBER, true, true));

        assertNotNull(result);
        assertFalse(result.success(), "SignedRecordFileProof version 5 must be rejected — only V6 is supported");
    }

    @Test
    @DisplayName("RSA V6: extractRecordStreamFileBytes correctly isolates field-2 bytes")
    void recordStreamFileBytesExtractedCorrectly_signatureVerifies() throws Exception {
        // Add a field-1 entry before field-2 in the proto bytes to confirm the parser skips correctly.
        // Field 1 (tag=10, wire=LEN): creation_time — we put some dummy bytes there.
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final byte[] dummyField1 = new byte[] {8, 0, 16, 0}; // minimal Timestamp proto (seconds=0, nanos=0)
        bos.write(0x0A); // tag: field 1 (LEN)
        bos.write(dummyField1.length);
        bos.write(dummyField1);
        bos.write(0x12); // tag: field 2 (LEN)
        bos.write(RECORD_STREAM_FILE_BYTES.length);
        bos.write(RECORD_STREAM_FILE_BYTES);
        final Bytes protoWithBothFields = Bytes.wrap(bos.toByteArray());

        // Build a session using these proto bytes as the RECORD_FILE item
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);
        final List<BlockItemUnparsed> items;
        {
            final BlockHeader header = new BlockHeader(
                    HAPI_VERSION, SW_VERSION, BLOCK_NUMBER, BLOCK_TIMESTAMP, BlockHashAlgorithm.SHA2_384);
            final BlockFooter footer = new BlockFooter(Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY);
            final BlockProof proof = BlockProof.newBuilder()
                    .block(BLOCK_NUMBER)
                    .signedRecordFileProof(new SignedRecordFileProof(6, sigs))
                    .build();
            items = List.of(
                    BlockItemUnparsed.newBuilder()
                            .blockHeader(BlockHeader.PROTOBUF.toBytes(header))
                            .build(),
                    BlockItemUnparsed.newBuilder()
                            .recordFile(protoWithBothFields)
                            .build(),
                    BlockItemUnparsed.newBuilder()
                            .blockFooter(BlockFooter.PROTOBUF.toBytes(footer))
                            .build(),
                    BlockItemUnparsed.newBuilder()
                            .blockProof(BlockProof.PROTOBUF.toBytes(proof))
                            .build());
        }
        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, VerificationProofMetrics.NONE);
        final VerificationNotification result =
                session.processBlockItems(new BlockItems(items, BLOCK_NUMBER, true, true));

        assertNotNull(result);
        assertTrue(result.success(), "Field-2 bytes must be correctly extracted even when field-1 precedes them");
    }

    /**
     * Builds a WRB block using custom {@code recordFileItemBytes} instead of the global fixture.
     * Used by the edge-case tests for {@code extractRecordStreamFileBytes}.
     */
    private static List<BlockItemUnparsed> buildWrbBlockWithCustomRecordFile(
            final Bytes customRecordFileBytes, final List<RecordFileSignature> signatures) {
        final BlockHeader header =
                new BlockHeader(HAPI_VERSION, SW_VERSION, BLOCK_NUMBER, BLOCK_TIMESTAMP, BlockHashAlgorithm.SHA2_384);
        final BlockFooter footer = new BlockFooter(Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY);
        final BlockProof proof = BlockProof.newBuilder()
                .block(BLOCK_NUMBER)
                .signedRecordFileProof(new SignedRecordFileProof(6, signatures))
                .build();
        return List.of(
                BlockItemUnparsed.newBuilder()
                        .blockHeader(BlockHeader.PROTOBUF.toBytes(header))
                        .build(),
                BlockItemUnparsed.newBuilder().recordFile(customRecordFileBytes).build(),
                BlockItemUnparsed.newBuilder()
                        .blockFooter(BlockFooter.PROTOBUF.toBytes(footer))
                        .build(),
                BlockItemUnparsed.newBuilder()
                        .blockProof(BlockProof.PROTOBUF.toBytes(proof))
                        .build());
    }

    @Test
    @DisplayName("extractRecordStreamFileBytes: empty, field-2-absent, unknown-wire-type all return EMPTY → failure")
    void extractRecordStreamFileBytes_defensiveCases() throws Exception {
        // All cases use valid signatures so any failure is from extraction, not signature validation.
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);

        // Case 1: empty RECORD_FILE bytes — nothing to extract → field not found → Bytes.EMPTY → fail
        final ExtendedMerkleTreeSession session1 = new ExtendedMerkleTreeSession(
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, VerificationProofMetrics.NONE);
        final VerificationNotification result1 = session1.processBlockItems(
                new BlockItems(buildWrbBlockWithCustomRecordFile(Bytes.EMPTY, sigs), BLOCK_NUMBER, true, true));
        assertNotNull(result1);
        assertFalse(result1.success(), "Empty RECORD_FILE bytes must cause extraction failure → rejected");

        // Case 2: only field 1 present, no field 2 — iterator exhausts without finding field 2 → Bytes.EMPTY → fail
        // Tag=0x0A (field 1, LEN), length=1, one payload byte
        final Bytes field1Only = Bytes.wrap(new byte[] {0x0A, 0x01, 0x42});
        final ExtendedMerkleTreeSession session2 = new ExtendedMerkleTreeSession(
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, VerificationProofMetrics.NONE);
        final VerificationNotification result2 = session2.processBlockItems(
                new BlockItems(buildWrbBlockWithCustomRecordFile(field1Only, sigs), BLOCK_NUMBER, true, true));
        assertNotNull(result2);
        assertFalse(result2.success(), "RECORD_FILE with no field-2 must cause extraction failure → rejected");

        // Case 3: unknown wire type (wire type 3) as first tag — bail-out path returns Bytes.EMPTY → fail
        // Tag = (1 << 3) | 3 = 0x0B (field 1, wire 3 = SGROUP — unused in proto3 but valid tag encoding)
        final Bytes unknownWireType = Bytes.wrap(new byte[] {0x0B});
        final ExtendedMerkleTreeSession session3 = new ExtendedMerkleTreeSession(
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, VerificationProofMetrics.NONE);
        final VerificationNotification result3 = session3.processBlockItems(
                new BlockItems(buildWrbBlockWithCustomRecordFile(unknownWireType, sigs), BLOCK_NUMBER, true, true));
        assertNotNull(result3);
        assertFalse(result3.success(), "Unknown wire type in RECORD_FILE must bail out → rejected");
    }

    /**
     * Verifies the at-least-one-valid acceptance rule across a range of roster sizes:
     * a single valid signature accepts, zero valid signatures rejects.
     */
    @ParameterizedTest(name = "rosterSize={0}")
    @MethodSource("rosterSizes")
    @DisplayName("one valid signature accepts and zero valid signatures rejects across roster sizes")
    void oneValidSignatureSuffices_zeroRejected(final int rosterSize) throws Exception {
        // Build a key map from the extended pool so existing 6-node KEY_MAP is unaffected.
        final Map<Long, PublicKey> keyMap = new HashMap<>();
        for (long id = 0; id < rosterSize; id++) {
            keyMap.put(id, EXTENDED_KEY_MAP.get(id));
        }

        // 1 valid sig → must be accepted
        final List<RecordFileSignature> one = List.of(extendedSignature(0L));
        assertTrue(
                runVerification(keyMap, one).success(), "rosterSize=" + rosterSize + ": 1 valid sig must be accepted");

        // 0 valid sigs (empty list) → must be rejected
        assertFalse(
                runVerification(keyMap, List.of()).success(),
                "rosterSize=" + rosterSize + ": 0 valid sigs must be rejected");
    }

    static Stream<Arguments> rosterSizes() {
        return Stream.of(
                Arguments.of(3),
                Arguments.of(4),
                Arguments.of(5),
                Arguments.of(6),
                Arguments.of(7),
                Arguments.of(8),
                Arguments.of(9));
    }
}
