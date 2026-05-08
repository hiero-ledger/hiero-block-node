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
 * Six nodes are used so the supermajority threshold (`floor(2*6/3) + 1 = 5`) is testable with
 * both passing (5 or 6 valid sigs) and failing (4 valid sigs) scenarios.
 *
 * <p>The raw `RecordFileItem` proto is assembled manually using protobuf wire format so that
 * `extractRecordStreamFileBytes` is exercised by the field-2 extraction path.
 */
class RsaWrbVerificationTest {

    /** Number of test nodes — threshold is `floor(2*6/3) + 1 = 5`. */
    private static final int ROSTER_SIZE = 6;

    /**
     * Extended key pool size for the parametrised threshold tests.
     * Covers all roster sizes tested in `supermajorityThreshold_exactlyMet_acceptedOneLess_rejected`.
     */
    private static final int MAX_KEY_POOL = 9;

    /** Supermajority threshold for 6 nodes. */
    private static final int THRESHOLD = 5;

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
     * Used exclusively by the parametrised threshold test so it can build rosters of varying sizes
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
        // 1024-bit RSA keys are used for test speed only — production network uses 4096-bit keys.
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(1024);
        // Generate the 6-node pool used by the fixed-roster tests.
        for (long nodeId = 0; nodeId < ROSTER_SIZE; nodeId++) {
            final KeyPair kp = kpg.generateKeyPair();
            KEY_MAP.put(nodeId, kp.getPublic());
            PRIVATE_KEY_MAP.put(nodeId, kp.getPrivate());
        }
        // Generate the extended pool (MAX_KEY_POOL nodes) for the parametrised threshold tests.
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
     * Runs a single-call verification through `ExtendedMerkleTreeSession` using the given
     * key map and signatures.
     */
    private static VerificationNotification runVerification(
            final Map<Long, PublicKey> keyMap, final List<RecordFileSignature> signatures) throws Exception {
        final List<BlockItemUnparsed> items = buildWrbBlock(signatures);
        final ExtendedMerkleTreeSession session = new ExtendedMerkleTreeSession(
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, keyMap, null, null, null);
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
     * `RecordFileSignature`. Used only by the parametrised threshold test.
     */
    private static RecordFileSignature extendedSignature(final long nodeId) throws Exception {
        final Signature engine = Signature.getInstance("SHA384withRSA");
        engine.initSign(EXTENDED_PRIVATE_KEY_MAP.get(nodeId));
        engine.update(signedPayload);
        return new RecordFileSignature(Bytes.wrap(engine.sign()), nodeId);
    }

    // ---- Tests -------------------------------------------------------------------------------

    @Test
    @DisplayName("valid proof at exactly the threshold is accepted")
    void validProofAtExactThreshold_accepted() throws Exception {
        // threshold = 5; sign with exactly nodes 0..4
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);
        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result, "Session must produce a notification");
        assertTrue(result.success(), "Proof with exactly threshold signatures must be accepted");
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
    }

    @Test
    @DisplayName("valid proof one below threshold is rejected")
    void validProofOneBelowThreshold_rejected() throws Exception {
        // threshold = 5; sign with only nodes 0..3 (4 valid sigs)
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L);
        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result);
        assertFalse(result.success(), "Proof with fewer than threshold signatures must be rejected");
        assertNull(result.blockHash(), "Block hash must not be set on failure");
        assertNull(result.block(), "Block must not be present on failure");
    }

    @Test
    @DisplayName("all-zero signature bytes are skipped and count as invalid")
    void allZeroSignatures_rejected() throws Exception {
        // Build signatures with all-zero bytes for every node
        final List<RecordFileSignature> sigs = new ArrayList<>();
        for (long id = 0; id < ROSTER_SIZE; id++) {
            sigs.add(new RecordFileSignature(Bytes.wrap(new byte[256]), id));
        }
        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result);
        assertFalse(result.success(), "All-zero signatures must not count toward threshold");
    }

    @Test
    @DisplayName("empty signature list in proof is rejected")
    void emptySignatureList_rejected() throws Exception {
        final VerificationNotification result = runVerification(KEY_MAP, List.of());

        assertNotNull(result);
        assertFalse(result.success(), "Empty signature list must be rejected");
    }

    @Test
    @DisplayName("signature from unknown node_id is skipped; rest still counted")
    void unknownNodeId_sigSkipped_othersStillCounted() throws Exception {
        // Nodes 0..4 produce valid sigs (meets threshold); node 99 is not in the key map
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);
        // Add a sig from an unknown node
        sigs.add(new RecordFileSignature(Bytes.wrap(sign(0L) /* use node 0's key bytes */), 99L));

        final Map<Long, PublicKey> keyMap = new HashMap<>(KEY_MAP); // node 99 absent
        final VerificationNotification result = runVerification(keyMap, sigs);

        assertNotNull(result);
        assertTrue(
                result.success(),
                "Sig from unknown node should be skipped; valid sigs from known nodes still meet threshold");
    }

    @Test
    @DisplayName("wrong payload signature (signed wrong data) counts as invalid")
    void wrongPayloadSignature_countedInvalid() throws Exception {
        // Sign wrong data with all 6 node keys — only 0 valid sigs
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
        assertFalse(result.success(), "Signatures on wrong payload must not verify");
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
        // 5 valid sigs from nodes 0..4 still meet the threshold of 5 for a 5-node roster.
        final Map<Long, PublicKey> reducedMap = new HashMap<>(KEY_MAP);
        reducedMap.remove(5L); // 5-node roster now — threshold = floor(2*5/3)+1 = 3+1 = 4

        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);
        final VerificationNotification result = runVerification(reducedMap, sigs);

        assertNotNull(result);
        // threshold for 5 nodes = 4; 5 valid sigs ≥ 4 → accepted
        assertTrue(result.success(), "5 valid sigs out of 5-node roster meets the threshold of 4");
    }

    @Test
    @DisplayName("duplicate node_id in proof is counted only once toward the threshold")
    void duplicateNodeId_countedOnlyOnce() throws Exception {
        // threshold = 5; send 4 distinct valid signatures + 1 duplicate of node 0 = still only 4 valid
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L);
        sigs.add(new RecordFileSignature(Bytes.wrap(sign(0L)), 0L)); // duplicate node 0
        final VerificationNotification result = runVerification(KEY_MAP, sigs);

        assertNotNull(result);
        assertFalse(
                result.success(),
                "Duplicate signature from node 0 must not count twice — validCount stays at 4, below threshold 5");
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
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, null, null, null);
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
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, null, null, null);
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
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, null, null, null);
        final VerificationNotification result1 = session1.processBlockItems(
                new BlockItems(buildWrbBlockWithCustomRecordFile(Bytes.EMPTY, sigs), BLOCK_NUMBER, true, true));
        assertNotNull(result1);
        assertFalse(result1.success(), "Empty RECORD_FILE bytes must cause extraction failure → rejected");

        // Case 2: only field 1 present, no field 2 — iterator exhausts without finding field 2 → Bytes.EMPTY → fail
        // Tag=0x0A (field 1, LEN), length=1, one payload byte
        final Bytes field1Only = Bytes.wrap(new byte[] {0x0A, 0x01, 0x42});
        final ExtendedMerkleTreeSession session2 = new ExtendedMerkleTreeSession(
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, null, null, null);
        final VerificationNotification result2 = session2.processBlockItems(
                new BlockItems(buildWrbBlockWithCustomRecordFile(field1Only, sigs), BLOCK_NUMBER, true, true));
        assertNotNull(result2);
        assertFalse(result2.success(), "RECORD_FILE with no field-2 must cause extraction failure → rejected");

        // Case 3: unknown wire type (wire type 3) as first tag — bail-out path returns Bytes.EMPTY → fail
        // Tag = (1 << 3) | 3 = 0x0B (field 1, wire 3 = SGROUP — unused in proto3 but valid tag encoding)
        final Bytes unknownWireType = Bytes.wrap(new byte[] {0x0B});
        final ExtendedMerkleTreeSession session3 = new ExtendedMerkleTreeSession(
                BLOCK_NUMBER, BlockSource.PUBLISHER, null, null, null, KEY_MAP, null, null, null);
        final VerificationNotification result3 = session3.processBlockItems(
                new BlockItems(buildWrbBlockWithCustomRecordFile(unknownWireType, sigs), BLOCK_NUMBER, true, true));
        assertNotNull(result3);
        assertFalse(result3.success(), "Unknown wire type in RECORD_FILE must bail out → rejected");
    }

    /**
     * Verifies that the supermajority threshold `floor(2 * rosterSize / 3) + 1` is correctly
     * implemented for roster sizes that are not multiples of 3.
     *
     * <p>The former formula `2 * ceil(rosterSize / 3) + 1` only agreed with `floor(2n/3) + 1`
     * when `rosterSize` was a multiple of 3. For example, with 7 nodes it produced threshold=7
     * (unanimous, impossible with any offline node) instead of the correct threshold=5.
     */
    @ParameterizedTest(name = "rosterSize={0} → threshold={1}")
    @MethodSource("thresholdCases")
    @DisplayName("supermajority threshold floor(2n/3)+1 is correct across roster sizes")
    void supermajorityThreshold_exactlyMet_acceptedOneLess_rejected(final int rosterSize, final int expectedThreshold)
            throws Exception {
        // Build a key map from the extended pool so existing 6-node KEY_MAP is unaffected.
        final Map<Long, PublicKey> keyMap = new HashMap<>();
        for (long id = 0; id < rosterSize; id++) {
            keyMap.put(id, EXTENDED_KEY_MAP.get(id));
        }

        // Exactly threshold valid sigs → must be accepted
        final List<RecordFileSignature> atThreshold = new ArrayList<>();
        for (long id = 0; id < expectedThreshold; id++) {
            atThreshold.add(extendedSignature(id));
        }
        assertTrue(
                runVerification(keyMap, atThreshold).success(),
                "rosterSize=" + rosterSize + ": " + expectedThreshold + " sigs (= threshold) must be accepted");

        // One below threshold → must be rejected
        final List<RecordFileSignature> oneLess = new ArrayList<>();
        for (long id = 0; id < expectedThreshold - 1; id++) {
            oneLess.add(extendedSignature(id));
        }
        assertFalse(
                runVerification(keyMap, oneLess).success(),
                "rosterSize=" + rosterSize + ": " + (expectedThreshold - 1) + " sigs (threshold-1) must be rejected");
    }

    static Stream<Arguments> thresholdCases() {
        // floor(2*n/3) + 1 for each roster size
        return Stream.of(
                Arguments.of(3, 3), // floor(6/3)  + 1 = 3
                Arguments.of(4, 3), // floor(8/3)  + 1 = 3
                Arguments.of(5, 4), // floor(10/3) + 1 = 4
                Arguments.of(6, 5), // floor(12/3) + 1 = 5
                Arguments.of(7, 5), // floor(14/3) + 1 = 5
                Arguments.of(8, 6), // floor(16/3) + 1 = 6
                Arguments.of(9, 7) //  floor(18/3) + 1 = 7
                );
    }
}
