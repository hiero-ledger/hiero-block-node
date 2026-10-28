// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.CodedOutputStream;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapDeleteChange;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.blockrecords.BlockInfo;
import com.hedera.hapi.node.state.blockrecords.RunningHashes;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.hapi.streams.TransactionSidecarRecord;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.hiero.block.tools.blocks.TestBlockFactory;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link ProtobufDuplicateFieldValidator}. */
class ProtobufDuplicateFieldValidatorTest {

    // Field numbers used to hand-wrap sub-messages, from the block stream protos.
    private static final int BLOCK_ITEMS = 1;
    private static final int ITEM_STATE_CHANGES = 7;
    private static final int ITEM_BLOCK_PROOF = 9;
    private static final int ITEM_RECORD_FILE = 10;
    private static final int RECORD_FILE_CONTENTS = 2;
    private static final int RECORD_STREAM_ITEMS = 3;
    private static final int STATE_CHANGES_STATE_CHANGES = 2;
    private static final int STATE_CHANGE_SINGLETON_UPDATE = 4;
    private static final int STATE_CHANGE_MAP_UPDATE = 5;

    private static final MapChangeKey KEY_A = MapChangeKey.newBuilder()
            .accountIdKey(AccountID.newBuilder().accountNum(2).build())
            .build();
    private static final MapChangeKey KEY_B = MapChangeKey.newBuilder()
            .accountIdKey(AccountID.newBuilder().accountNum(3).build())
            .build();
    private static final RecordStreamItem RSI_A = RecordStreamItem.newBuilder()
            .record(TransactionRecord.newBuilder().memo("a").build())
            .build();
    private static final RecordStreamItem RSI_B = RecordStreamItem.newBuilder()
            .record(TransactionRecord.newBuilder().memo("b").build())
            .build();

    // ── Must pass ──

    @ParameterizedTest
    @ValueSource(
            strings = {
                "/record-files/wrb/v2-block.blk.gz",
                "/record-files/wrb/v5-block.blk.gz",
                "/record-files/wrb/v6-block.blk.gz"
            })
    void realBlocks_pass(final String resource) throws IOException {
        final byte[] bytes;
        try (InputStream in = new GZIPInputStream(getClass().getResourceAsStream(resource))) {
            bytes = in.readAllBytes();
        }
        assertDoesNotThrow(() -> ProtobufDuplicateFieldValidator.validate(bytes, 0));
    }

    @Test
    void testFactoryBlock_passes() {
        final Block block = TestBlockFactory.createValidChain(1).getFirst();
        assertPasses(Block.PROTOBUF.toBytes(block).toByteArray());
    }

    @Test
    void repeatedItems_pass() {
        // items[0] = {block_header {number: 5}}, items[1] = {block_footer {previous_block_root_hash: 0x01}}
        assertPasses(hex("0A04 0A021805 0A05 6203 0A0101"));
    }

    @Test
    void repeatedMessageFieldsDeeper_pass() {
        final Timestamp ts = Timestamp.newBuilder().seconds(1).build();
        final SidecarFile sidecar = SidecarFile.newBuilder()
                .sidecarRecords(List.of(TransactionSidecarRecord.newBuilder()
                        .consensusTimestamp(ts)
                        .build()))
                .build();
        final RecordFileItem recordFile = RecordFileItem.newBuilder()
                .recordFileContents(RecordStreamFile.newBuilder()
                        .recordStreamItems(List.of(RSI_A, RSI_B))
                        .build())
                .sidecarFileContents(List.of(sidecar, sidecar))
                .amendments(List.of(RSI_A, RSI_B))
                .build();
        final StateChange change =
                StateChange.newBuilder().mapUpdate(mapUpdate(KEY_A)).build();
        final StateChanges stateChanges = StateChanges.newBuilder()
                .stateChanges(List.of(change, change, change))
                .build();
        final Block block = new Block(List.of(
                BlockItem.newBuilder().recordFile(recordFile).build(),
                BlockItem.newBuilder().stateChanges(stateChanges).build()));
        assertPasses(Block.PROTOBUF.toBytes(block).toByteArray());
    }

    @Test
    void duplicatePlainScalar_notReported() {
        // BlockHeader.number twice, PBJ keeps 6
        assertPasses(hex("0A06 0A04 18051806"));
    }

    @Test
    void unknownField_skipped() {
        // BlockHeader field 999 (LEN, one byte 0xFF), then number: 5
        assertPasses(hex("0A08 0A06 BA3E01FF 1805"));
    }

    // ── Must fail: non-repeated message field set twice ──

    @Test
    void hapiProtoVersionTwice_fails() {
        // protoc merges to {major: 1, minor: 80}, PBJ keeps {major: 0, minor: 80}
        assertFails(hex("0A0A 0A08 0A020801 0A021050"), "Block.items[0].block_header.hapi_proto_version");
    }

    @Test
    void recordFileContentsTwice_fails() {
        final byte[] recordFile = concat(
                RecordFileItem.PROTOBUF
                        .toBytes(RecordFileItem.newBuilder()
                                .recordFileContents(RecordStreamFile.newBuilder()
                                        .blockNumber(1)
                                        .build())
                                .build())
                        .toByteArray(),
                RecordFileItem.PROTOBUF
                        .toBytes(RecordFileItem.newBuilder()
                                .recordFileContents(RecordStreamFile.newBuilder()
                                        .blockNumber(2)
                                        .build())
                                .build())
                        .toByteArray());
        final String message = assertFails(block(field(ITEM_RECORD_FILE, recordFile)));
        assertTrue(message.endsWith("record_file.record_file_contents"), message);
    }

    @Test
    void mapUpdateKeyTwice_fails() {
        final byte[] mapUpdate = concat(
                MapUpdateChange.PROTOBUF.toBytes(mapUpdate(KEY_A)).toByteArray(),
                MapUpdateChange.PROTOBUF.toBytes(mapUpdate(KEY_B)).toByteArray());
        final byte[] stateChange = field(STATE_CHANGE_MAP_UPDATE, mapUpdate);
        final byte[] item = field(ITEM_STATE_CHANGES, field(STATE_CHANGES_STATE_CHANGES, stateChange));
        final String message = assertFails(block(item));
        assertTrue(message.endsWith("state_changes[0].map_update.key"), message);
    }

    @Test
    void recordStreamItemRecordTwice_fails() {
        final byte[] duplicateRecord = concat(
                RecordStreamItem.PROTOBUF.toBytes(RSI_A).toByteArray(),
                RecordStreamItem.PROTOBUF.toBytes(RSI_B).toByteArray());
        final byte[] recordStreamFile = concat(
                RecordStreamFile.PROTOBUF
                        .toBytes(RecordStreamFile.newBuilder()
                                .recordStreamItems(List.of(RSI_A))
                                .build())
                        .toByteArray(),
                field(RECORD_STREAM_ITEMS, duplicateRecord));
        final byte[] item = field(ITEM_RECORD_FILE, field(RECORD_FILE_CONTENTS, recordStreamFile));
        final String message = assertFails(block(item));
        assertTrue(message.endsWith("record_file.record_file_contents.record_stream_items[1].record"), message);
    }

    // ── Must fail: oneof with more than one member ──

    @Test
    void sameOneofMessageMemberTwice_fails() {
        assertFails(hex("0A08 0A021805 0A021805"), "oneof 'item'", "block_header");
    }

    @Test
    void twoDifferentOneofMessageMembers_fails() {
        assertFails(hex("0A09 0A021805 62030A0101"), "oneof 'item'", "block_header, block_footer");
    }

    @Test
    void bytesThenMessageOneofMembers_fails() {
        assertFails(hex("0A07 2201AA 0A021805"), "oneof 'item'", "signed_transaction, block_header");
    }

    @Test
    void sameOneofBytesMemberTwice_fails() {
        assertFails(hex("0A06 2201AA 2201BB"), "oneof 'item'", "signed_transaction");
    }

    @Test
    void blockProofOneofTwoMembers_fails() {
        final byte[] proof = concat(
                BlockProof.PROTOBUF
                        .toBytes(BlockProof.newBuilder()
                                .signedBlockProof(TssSignedBlockProof.newBuilder()
                                        .blockSignature(Bytes.wrap(new byte[] {1}))
                                        .build())
                                .build())
                        .toByteArray(),
                BlockProof.PROTOBUF
                        .toBytes(BlockProof.newBuilder()
                                .signedRecordFileProof(SignedRecordFileProof.newBuilder()
                                        .version(6)
                                        .build())
                                .build())
                        .toByteArray());
        assertFails(
                block(field(ITEM_BLOCK_PROOF, proof)),
                "oneof 'proof'",
                "signed_block_proof, signed_record_file_proof",
                "Block.items[0].block_proof");
    }

    @Test
    void stateChangeOneofTwoMembers_fails() {
        final byte[] stateChange = concat(
                StateChange.PROTOBUF
                        .toBytes(StateChange.newBuilder()
                                .mapUpdate(mapUpdate(KEY_A))
                                .build())
                        .toByteArray(),
                StateChange.PROTOBUF
                        .toBytes(StateChange.newBuilder()
                                .mapDelete(
                                        MapDeleteChange.newBuilder().key(KEY_A).build())
                                .build())
                        .toByteArray());
        final byte[] item = field(ITEM_STATE_CHANGES, field(STATE_CHANGES_STATE_CHANGES, stateChange));
        assertFails(block(item), "oneof 'change_operation'", "map_update, map_delete", "state_changes[0]");
    }

    @Test
    void singletonUpdateOneofTwoMembers_fails() {
        final byte[] singletonUpdate = concat(
                SingletonUpdateChange.PROTOBUF
                        .toBytes(SingletonUpdateChange.newBuilder()
                                .blockInfoValue(BlockInfo.newBuilder()
                                        .lastBlockNumber(1)
                                        .build())
                                .build())
                        .toByteArray(),
                SingletonUpdateChange.PROTOBUF
                        .toBytes(SingletonUpdateChange.newBuilder()
                                .runningHashesValue(RunningHashes.newBuilder()
                                        .runningHash(Bytes.wrap(new byte[] {1}))
                                        .build())
                                .build())
                        .toByteArray());
        final byte[] stateChange = field(STATE_CHANGE_SINGLETON_UPDATE, singletonUpdate);
        final byte[] item = field(ITEM_STATE_CHANGES, field(STATE_CHANGES_STATE_CHANGES, stateChange));
        assertFails(
                block(item),
                "oneof 'new_value'",
                "block_info_value, running_hashes_value",
                "state_changes[0].singleton_update");
    }

    // ── Must fail: malformed ──

    @Test
    void truncatedInput_fails() {
        final byte[] blockBytes = hex("0A05 0A02");
        assertThrows(IOException.class, () -> ProtobufDuplicateFieldValidator.validate(blockBytes, 0));
    }

    @Test
    void nestingPastMaxDepth_fails() {
        // Key.keyList (6) -> KeyList.keys (1) -> Key ... is self-recursive, two levels per round.
        byte[] key = new byte[0];
        for (int i = 0; i <= ProtobufParsingConstants.MAX_DEPTH / 2; i++) {
            key = field(6, field(1, key));
        }
        // StateChanges.state_changes -> StateChange.map_update -> MapUpdateChange.value (2)
        // -> MapChangeValue.account_value (1) -> Account.key (3)
        final byte[] mapUpdate = field(2, field(1, field(3, key)));
        final byte[] stateChange = field(STATE_CHANGE_MAP_UPDATE, mapUpdate);
        final byte[] item = field(ITEM_STATE_CHANGES, field(STATE_CHANGES_STATE_CHANGES, stateChange));
        final byte[] blockBytes = block(item);
        final IOException e =
                assertThrows(IOException.class, () -> ProtobufDuplicateFieldValidator.validate(blockBytes, 0));
        final String expected = "Nesting deeper than " + ProtobufParsingConstants.MAX_DEPTH;
        assertTrue(e.getMessage().contains(expected), "Expected '" + expected + "' in: " + e.getMessage());
    }

    // ── Helpers ──

    private static void assertPasses(final byte[] blockBytes) {
        assertDoesNotThrow(() -> ProtobufDuplicateFieldValidator.validate(blockBytes, 0));
    }

    /** Asserts the validator fails and the message contains every expected fragment; returns the message. */
    private static String assertFails(final byte[] blockBytes, final String... expected) {
        final ValidationException e =
                assertThrows(ValidationException.class, () -> ProtobufDuplicateFieldValidator.validate(blockBytes, 0));
        final String message = e.getMessage();
        for (final String fragment : expected) {
            assertTrue(message.contains(fragment), "Expected '" + fragment + "' in: " + message);
        }
        return message;
    }

    private static MapUpdateChange mapUpdate(final MapChangeKey key) {
        return MapUpdateChange.newBuilder().key(key).build();
    }

    private static byte[] hex(final String spaced) {
        return HexFormat.of().parseHex(spaced.replace(" ", ""));
    }

    /** Wraps each item as a {@code Block.items} entry. */
    private static byte[] block(final byte[]... items) {
        final byte[][] wrapped = new byte[items.length][];
        for (int i = 0; i < items.length; i++) {
            wrapped[i] = field(BLOCK_ITEMS, items[i]);
        }
        return concat(wrapped);
    }

    /** Encodes {@code payload} as a length-delimited field. */
    private static byte[] field(final int number, final byte[] payload) {
        try {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final CodedOutputStream coded = CodedOutputStream.newInstance(out);
            coded.writeByteArray(number, payload);
            coded.flush();
            return out.toByteArray();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] concat(final byte[]... parts) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (final byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }
}
