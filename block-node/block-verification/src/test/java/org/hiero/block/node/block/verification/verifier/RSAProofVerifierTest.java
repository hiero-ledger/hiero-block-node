// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.hasher.BlockHasher;
import org.hiero.block.node.block.verification.hasher.HashingResult;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.SessionFailureType;
import org.hiero.block.node.block.verification.session.VerificationSessionFailedException;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/// Tests for [RSAProofVerifier].
class RSAProofVerifierTest {
    /// Number of nodes in the fixed roster used by most tests.
    private static final int ROSTER_SIZE = 6;
    /// Extended key pool size for the parametrised roster-size test.
    /// Covers all roster sizes exercised in `oneValidSignatureSuffices_zeroRejected`.
    private static final int MAX_KEY_POOL = 9;
    /// HAPI version >= 0.72.0 routes to `ExtendedMerkleTreeSession`.
    private static final SemanticVersion HAPI_VERSION = new SemanticVersion(1, 0, 0, "", "");
    private static final SemanticVersion SW_VERSION = new SemanticVersion(1, 0, 0, "", "");
    private static final Timestamp BLOCK_TIMESTAMP = new Timestamp(1_700_000_000L, 0);
    private static final long BLOCK_NUMBER = 42L;
    /// Raw bytes used as the `record_file_contents` (field 2) of the test `RecordFileItem`.
    /// These represent a minimal placeholder - only the bytes matter for the hash computation.
    private static final byte[] RECORD_STREAM_FILE_BYTES = "test-record-stream-file-v6-content".getBytes();
    /// RSA public keys indexed by node_id (0 .. ROSTER_SIZE-1). Used by the existing 6-node tests.
    private static final Map<Long, PublicKey> KEY_MAP = new HashMap<>();
    /// RSA private keys indexed by node_id (0 .. ROSTER_SIZE-1). Used by the existing 6-node tests.
    private static final Map<Long, PrivateKey> PRIVATE_KEY_MAP = new HashMap<>();
    /// Extended public key pool covering node_ids 0 .. MAX_KEY_POOL-1.
    /// Used exclusively by the parametrised roster-size test so it can build rosters of varying sizes
    /// without affecting the existing 6-node test fixtures.
    private static final Map<Long, PublicKey> EXTENDED_KEY_MAP = new HashMap<>();
    /// Extended private key pool paired with `EXTENDED_KEY_MAP`.
    private static final Map<Long, PrivateKey> EXTENDED_PRIVATE_KEY_MAP = new HashMap<>();
    /// Serialized `RecordFileItem` proto with field 2 = `RECORD_STREAM_FILE_BYTES`.
    private static Bytes recordFileItemBytes;
    /// V6 signed payload for the above record file: SHA-384(int32(6) || RECORD_STREAM_FILE_BYTES).
    private static byte[] signedPayload;

    @BeforeAll
    static void generateKeysAndPayload() throws Exception {
        // 2048-bit RSA keys are used for test speed only - production network uses 4096-bit keys.
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

    /// Signs `signedPayload` with the RSA private key of the given node and returns the
    /// signature bytes.
    private static byte[] sign(final long nodeId) throws Exception {
        final Signature engine = Signature.getInstance("SHA384withRSA");
        engine.initSign(PRIVATE_KEY_MAP.get(nodeId));
        engine.update(signedPayload);
        return engine.sign();
    }

    /// Builds a `BlockItemUnparsed` list representing a minimal WRB block:
    /// `BLOCK_HEADER | RECORD_FILE | BLOCK_FOOTER | BLOCK_PROOF`.
    ///
    /// @param signatures the list of `RecordFileSignature` entries to include in the proof
    /// @return list of unparsed block items ready to pass to `processBlockItems`
    private static List<BlockItemUnparsed> buildWrbBlock(final List<RecordFileSignature> signatures) {
        final BlockHeader header =
                new BlockHeader(HAPI_VERSION, SW_VERSION, BLOCK_NUMBER, BLOCK_TIMESTAMP, BlockHashAlgorithm.SHA2_384);
        final BlockFooter footer = new BlockFooter(Bytes.wrap("hash1"), Bytes.wrap("hash2"), Bytes.wrap("hash3"));
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

    /// Builds a WRB block using custom {@code recordFileItemBytes} instead of the global fixture.
    /// Used by the edge-case tests for {@code extractRecordStreamFileBytes}.
    private static List<BlockItemUnparsed> buildWrbBlockWithCustomRecordFile(
            final Bytes customRecordFileBytes, final List<RecordFileSignature> signatures) {
        final BlockHeader header =
                new BlockHeader(HAPI_VERSION, SW_VERSION, BLOCK_NUMBER, BLOCK_TIMESTAMP, BlockHashAlgorithm.SHA2_384);
        final BlockFooter footer = new BlockFooter(Bytes.wrap("hash1"), Bytes.wrap("hash2"), Bytes.wrap("hash3"));
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

    /// Runs a single-call verification through `ExtendedMerkleTreeSession` using the given
    /// key map and signatures.
    private static SessionFailureType runVerification(
            final Map<Long, PublicKey> keyMap, final List<RecordFileSignature> signatures) throws Exception {
        return runVerification(buildWrbBlock(signatures), keyMap);
    }

    /// Runs a single-call verification through `ExtendedMerkleTreeSession` using the given
    /// key map and signatures.
    private static SessionFailureType runVerification(
            final List<BlockItemUnparsed> items, final Map<Long, PublicKey> keyMap) throws Exception {
        final BlockNodeContext context = TestUtils.testContext();
        final MetricsHolder metricsHolder = MetricsHolder.create(context.metricRegistry());
        final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
        final BlockHasher hasher = createHasher(blockItemsDeque, BLOCK_NUMBER, context, metricsHolder);
        blockItemsDeque.offer(new BlockItems(items, BLOCK_NUMBER, true, true));
        final HashingResult hashingResult = hasher.get();
        final SignedRecordFileProof proof =
                hashingResult.blockProofs().getFirst().signedRecordFileProofOrThrow();
        final Signature sha384WithRSASig = Signature.getInstance("SHA384withRSA");
        final RSAProofVerifier proofVerifier = new RSAProofVerifier(
                new AtomicBoolean(false),
                metricsHolder.proofVerificationMetrics(),
                hashingResult.blockNumber(),
                keyMap,
                proof,
                signedPayload,
                sha384WithRSASig);
        return proofVerifier.verify();
    }

    private static BlockHasher createHasher(
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque,
            final long blockNumber,
            final BlockNodeContext context,
            final MetricsHolder metricsHolder) {
        final VerificationDataProvider verificationDataProvider = new VerificationDataProvider(context);
        final AtomicBoolean isCanceled = new AtomicBoolean(false);
        return new BlockHasher(
                isCanceled,
                blockItemsDeque,
                metricsHolder.hashingMetrics(),
                blockNumber,
                BlockSource.PUBLISHER,
                verificationDataProvider);
    }

    /// Signs with the given node IDs (from the 6-node pool) and returns a `RecordFileSignature` list.
    private static List<RecordFileSignature> signaturesFor(final long... nodeIds) throws Exception {
        final List<RecordFileSignature> sigs = new ArrayList<>();
        for (final long nodeId : nodeIds) {
            sigs.add(new RecordFileSignature(Bytes.wrap(sign(nodeId)), nodeId));
        }
        return sigs;
    }

    /// Signs `signedPayload` with the extended pool private key for `nodeId` and returns a
    /// `RecordFileSignature`. Used only by the parametrised roster-size test.
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
        final SessionFailureType result = runVerification(KEY_MAP, sigs);
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("valid proof with multiple signatures is accepted")
    void validProofMultipleSignatures_accepted() throws Exception {
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L);
        final SessionFailureType result = runVerification(KEY_MAP, sigs);
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("valid proof with all nodes signing is accepted")
    void validProofAllNodesSigning_accepted() throws Exception {
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L, 5L);
        final SessionFailureType result = runVerification(KEY_MAP, sigs);
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("valid proof with three signatures in a six-node roster is accepted")
    void validProofMinoritySignatures_accepted() throws Exception {
        // Under the at-least-one-valid rule a small number of valid signatures (well below half
        // the roster) still accepts the block, provided every signature present verifies.
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L);
        final SessionFailureType result = runVerification(KEY_MAP, sigs);
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("all-zero signature bytes are skipped, leaving zero valid sigs → rejected")
    void allZeroSignatures_rejected() throws Exception {
        // Every entry is all-zero - the defensive pre-filter skips each, validCount stays at 0,
        // and the at-least-one-valid rule rejects the block.
        final List<RecordFileSignature> sigs = new ArrayList<>();
        for (long id = 0; id < ROSTER_SIZE; id++) {
            sigs.add(new RecordFileSignature(Bytes.wrap(new byte[256]), id));
        }
        final SessionFailureType result = runVerification(KEY_MAP, sigs);
        assertThat(result).isNotNull().isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    @Test
    @DisplayName("empty signature list in proof is rejected")
    void emptySignatureList_rejected() throws Exception {
        final SessionFailureType result = runVerification(KEY_MAP, List.of());
        assertThat(result).isNotNull().isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    @Test
    @DisplayName("signature from unknown node_id is skipped as a roster mismatch; block still accepted")
    void unknownNodeId_skippedAsMismatch_blockAccepted() throws Exception {
        // Node 99 is not in the era key map - its signature is skipped and tallied as a roster
        // mismatch (see @todo(2808)) rather than rejecting the block outright; the five valid
        // signatures from nodes 0..4 are sufficient to accept.
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);
        sigs.add(new RecordFileSignature(Bytes.wrap(sign(0L)), 99L)); // node 99 absent from era key map
        final Map<Long, PublicKey> keyMap = new HashMap<>(KEY_MAP); // node 99 absent
        final SessionFailureType result = runVerification(keyMap, sigs);
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("signature from node not in era address book is skipped as a roster mismatch; block still accepted")
    void nodeNotInEraAddressBook_skippedAsMismatch_blockAccepted() throws Exception {
        // The era key map only has nodes 0..4; the proof has a sig from node 5, which is skipped
        // and tallied as a roster mismatch. The five valid era signatures accept the block.
        final Map<Long, PublicKey> eraKeyMap = new HashMap<>();
        for (long id = 0; id < 5; id++) {
            eraKeyMap.put(id, KEY_MAP.get(id));
        }
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L, 5L);
        final SessionFailureType result = runVerification(eraKeyMap, sigs);
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("all signatures from unknown node_ids are skipped as mismatches, leaving zero valid sigs - rejected")
    void allSignaturesUnknownNodeIds_skippedAsMismatches_rejected() throws Exception {
        // Era roster contains nodes 10..12 only - none of the signing nodes (0, 1, 2) are present,
        // so every signature is skipped as a roster mismatch and validCount stays at 0.
        final Map<Long, PublicKey> eraKeyMap = new HashMap<>();
        eraKeyMap.put(10L, KEY_MAP.get(0L));
        eraKeyMap.put(11L, KEY_MAP.get(1L));
        eraKeyMap.put(12L, KEY_MAP.get(2L));
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L);
        final SessionFailureType result = runVerification(eraKeyMap, sigs);
        assertThat(result).isNotNull().isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    @Test
    @DisplayName("wrong payload signature (signed wrong data) causes immediate block rejection")
    void wrongPayloadSignature_immediatelyRejectsBlock() throws Exception {
        // CN only sends signatures from consensus-contributing nodes, so a failed RSA verify
        // means the proof or block is tampered - the block is rejected at the first failing sig.
        final List<RecordFileSignature> sigs = new ArrayList<>();
        final byte[] wrongData = "totally-wrong-payload".getBytes();
        for (long id = 0; id < ROSTER_SIZE; id++) {
            final Signature engine = Signature.getInstance("SHA384withRSA");
            engine.initSign(PRIVATE_KEY_MAP.get(id));
            engine.update(wrongData);
            sigs.add(new RecordFileSignature(Bytes.wrap(engine.sign()), id));
        }
        final SessionFailureType result = runVerification(KEY_MAP, sigs);
        assertThat(result).isNotNull().isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    @Test
    @DisplayName("one bad sig among otherwise valid sigs causes immediate block rejection")
    void oneBadSigAmongValids_immediatelyRejectsBlock() throws Exception {
        // 5 valid sigs from nodes 0..4, but node 5 signs the wrong payload - the block must be
        // rejected immediately by the "any failed verify rejects" rule, regardless of how many
        // other sigs are valid.
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);
        final byte[] wrongData = "tampered-payload".getBytes();
        final Signature engine = Signature.getInstance("SHA384withRSA");
        engine.initSign(PRIVATE_KEY_MAP.get(5L));
        engine.update(wrongData);
        sigs.add(new RecordFileSignature(Bytes.wrap(engine.sign()), 5L));
        final SessionFailureType result = runVerification(KEY_MAP, sigs);
        assertThat(result).isNotNull().isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    @Test
    @DisplayName("empty address book (keyByNodeId empty) is rejected with error")
    void emptyAddressBook_rejected() throws Exception {
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L, 5L);
        final SessionFailureType result = runVerification(Map.of(), sigs);
        assertThat(result).isNotNull().isEqualTo(SessionFailureType.MISSING_VERIFICATION_DATA);
    }

    @Test
    @DisplayName("malformed DER key in address book is excluded; remaining keys still counted")
    void malformedKeyExcludedFromMap_restStillCounted() throws Exception {
        // Build a key map where node 5 has a malformed (random) key - RsaKeyDecoder would skip it.
        // Here we simulate by simply omitting node 5 from the map (as if buildKeyMap had skipped it).
        final Map<Long, PublicKey> reducedMap = new HashMap<>(KEY_MAP);
        reducedMap.remove(5L);
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);
        final SessionFailureType result = runVerification(reducedMap, sigs);
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("duplicate node_id in proof is skipped and block is accepted")
    void duplicateNodeId_skippedAndBlockAccepted() throws Exception {
        // 2 distinct valid sigs + 1 duplicate of node 0. The dedup branch must skip the
        // duplicate (defence-in-depth against an inflated validCount) without affecting the
        // accept/reject outcome under the at-least-one-valid rule.
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L);
        sigs.add(new RecordFileSignature(Bytes.wrap(sign(0L)), 0L)); // duplicate of node 0
        final SessionFailureType result = runVerification(KEY_MAP, sigs);
        assertThat(result).isNull();
    }

    /// Verifies the at-least-one-valid acceptance rule across a range of roster sizes:
    /// a single valid signature accepted, zero valid signatures rejected.
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
        assertThat(runVerification(keyMap, one))
                .withFailMessage("rosterSize=" + rosterSize + ": 1 valid sig must be accepted")
                .isNull();
        // 0 valid sigs (empty list) → must be rejected
        assertThat(runVerification(keyMap, List.of()))
                .withFailMessage("rosterSize=" + rosterSize + ": 0 valid sigs must be rejected")
                .isNotNull()
                .isEqualTo(SessionFailureType.BAD_BLOCK_PROOF);
    }

    // @todo(3007) port below tests as a follow up
    //    add test in order and out of order based on high water mark and all sources ordering config
    //    add test passing in any order if lower than high water mark

    @Test
    @DisplayName("Null signed payload rejected")
    void testNullSignedPayloadRejected() throws Exception {
        final BlockNodeContext context = TestUtils.testContext();
        final MetricsHolder metricsHolder = MetricsHolder.create(context.metricRegistry());
        final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
        final BlockHasher hasher = createHasher(blockItemsDeque, BLOCK_NUMBER, context, metricsHolder);
        blockItemsDeque.offer(new BlockItems(buildWrbBlock(signaturesFor(0L)), BLOCK_NUMBER, true, true));
        final HashingResult hashingResult = hasher.get();
        final SignedRecordFileProof proof =
                hashingResult.blockProofs().getFirst().signedRecordFileProofOrThrow();
        final Signature sha384WithRSASig = Signature.getInstance("SHA384withRSA");
        final RSAProofVerifier proofVerifier = new RSAProofVerifier(
                new AtomicBoolean(false),
                metricsHolder.proofVerificationMetrics(),
                hashingResult.blockNumber(),
                KEY_MAP,
                proof,
                null,
                sha384WithRSASig);
        final SessionFailureType actual = proofVerifier.verify();
        assertThat(actual).isNotNull().isEqualTo(SessionFailureType.MISSING_VERIFICATION_DATA);
    }

    @Test
    @DisplayName("unsupported proof version is rejected")
    void unsupportedVersion_rejected() throws Exception {
        final List<BlockItemUnparsed> items;
        {
            final BlockHeader header = new BlockHeader(
                    HAPI_VERSION, SW_VERSION, BLOCK_NUMBER, BLOCK_TIMESTAMP, BlockHashAlgorithm.SHA2_384);
            final BlockFooter footer = new BlockFooter(Bytes.wrap("hash1"), Bytes.wrap("hash2"), Bytes.wrap("hash3"));
            // Version 5 - only V6 is supported in Phase 2a
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
        final SessionFailureType sessionFailureType = runVerification(items, KEY_MAP);
        assertThat(sessionFailureType).isNotNull().isEqualTo(SessionFailureType.MISSING_MANDATORY_FIELD);
    }

    @Test
    @DisplayName("RSA V6: extractRecordStreamFileBytes correctly isolates field-2 bytes")
    void recordStreamFileBytesExtractedCorrectly_signatureVerifies() throws Exception {
        // Add a field-1 entry before field-2 in the proto bytes to confirm the parser skips correctly.
        // Field 1 (tag=10, wire=LEN): creation_time - we put some dummy bytes there.
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
            final BlockFooter footer = new BlockFooter(Bytes.wrap("hash1"), Bytes.wrap("hash2"), Bytes.wrap("hash3"));
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
        final SessionFailureType sessionFailureType = runVerification(items, KEY_MAP);
        assertThat(sessionFailureType).isNull();
    }

    @Test
    @DisplayName("extractRecordStreamFileBytes: empty, field-2-absent, unknown-wire-type all return EMPTY → failure")
    void extractRecordStreamFileBytes_defensiveCases() throws Exception {
        // All cases use valid signatures so any failure is from extraction, not signature validation.
        final List<RecordFileSignature> sigs = signaturesFor(0L, 1L, 2L, 3L, 4L);
        // Case 1: empty RECORD_FILE bytes - nothing to extract → field not found → Bytes.EMPTY → fail
        // We expect hasher to fail
        assertThatExceptionOfType(VerificationSessionFailedException.class)
                .isThrownBy(() -> runVerification(buildWrbBlockWithCustomRecordFile(Bytes.EMPTY, sigs), KEY_MAP))
                .asInstanceOf(type(VerificationSessionFailedException.class))
                .returns(
                        SessionFailureType.MISSING_MANDATORY_FIELD, VerificationSessionFailedException::getFailureType);
        // Case 2: only field 1 present, no field 2 - iterator exhausts without finding field 2 → Bytes.EMPTY → fail
        // Tag=0x0A (field 1, LEN), length=1, one payload byte
        // We expect hasher to fail
        final Bytes field1Only = Bytes.wrap(new byte[] {0x0A, 0x01, 0x42});
        assertThatExceptionOfType(VerificationSessionFailedException.class)
                .isThrownBy(() -> runVerification(buildWrbBlockWithCustomRecordFile(field1Only, sigs), KEY_MAP))
                .asInstanceOf(type(VerificationSessionFailedException.class))
                .returns(
                        SessionFailureType.MISSING_MANDATORY_FIELD, VerificationSessionFailedException::getFailureType);
        // Case 3: unknown wire type (wire type 3) as first tag - bail-out path returns Bytes.EMPTY → fail
        // Tag = (1 << 3) | 3 = 0x0B (field 1, wire 3 = SGROUP - unused in proto3 but valid tag encoding)
        final Bytes unknownWireType = Bytes.wrap(new byte[] {0x0B});
        // We expect hasher to fail
        assertThatExceptionOfType(VerificationSessionFailedException.class)
                .isThrownBy(() -> runVerification(buildWrbBlockWithCustomRecordFile(unknownWireType, sigs), KEY_MAP))
                .asInstanceOf(type(VerificationSessionFailedException.class))
                .returns(
                        SessionFailureType.MISSING_MANDATORY_FIELD, VerificationSessionFailedException::getFailureType);
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
