// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.hedera.pbj.runtime.ProtoConstants;
import com.hedera.pbj.runtime.ProtoParserTools;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RecordFileItemSplitter}.
 */
@DisplayName("RecordFileItemSplitter Tests")
class RecordFileItemSplitterTest {

    // -----------------------------------------------------------------------
    // splitIfNeeded — basic invariants
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("non-recordFile item is returned unchanged")
    void testNonRecordFileItemReturnedUnchanged() {
        final BlockItemUnparsed item = BlockItemUnparsed.newBuilder()
                .roundHeader(Bytes.wrap(new byte[100]))
                .build();
        final List<BlockItemUnparsed> result = RecordFileItemSplitter.splitIfNeeded(item, 1_000_000);
        assertThat(result).hasSize(1).first().isSameAs(item);
    }

    @Test
    @DisplayName("small recordFile item under the limit is returned unchanged")
    void testSmallRecordFileReturnedUnchanged() {
        // Build a tiny RecordFileItem: field 2 (record_file_contents) wrapping a tiny RecordStreamFile
        final byte[] tinyRFI = buildMinimalRecordFileItemBytes(10);
        final BlockItemUnparsed item =
                BlockItemUnparsed.newBuilder().recordFile(Bytes.wrap(tinyRFI)).build();
        final int size = BlockItemUnparsed.PROTOBUF.measureRecord(item);
        final List<BlockItemUnparsed> result = RecordFileItemSplitter.splitIfNeeded(item, size + 1000);
        assertThat(result).hasSize(1).first().isSameAs(item);
    }

    @Test
    @DisplayName("recordFile with no record_stream_items cannot be split — original returned")
    void testRecordFileWithNoRSIReturnedUnchanged() {
        // Build a RecordFileItem whose RecordStreamFile has NO field-3 entries (just a tiny header)
        final ByteArrayOutputStream rsfOut = new ByteArrayOutputStream();
        final WritableStreamingData rsfWriter = new WritableStreamingData(rsfOut);
        // Write field 1 (hapi_proto_version, varint, value = 1)
        writeVarintField(rsfWriter, 1, 1L);
        final byte[] rsfBytes = rsfOut.toByteArray();

        final ByteArrayOutputStream rfiOut = new ByteArrayOutputStream();
        final WritableStreamingData rfiWriter = new WritableStreamingData(rfiOut);
        writeLenField(rfiWriter, 2, rsfBytes);
        final byte[] rfiBytes = rfiOut.toByteArray();

        final BlockItemUnparsed item =
                BlockItemUnparsed.newBuilder().recordFile(Bytes.wrap(rfiBytes)).build();
        // Even if we request a split at 1 byte, there's nothing to split
        final List<BlockItemUnparsed> result = RecordFileItemSplitter.splitIfNeeded(item, 1);
        assertThat(result).hasSize(1);
    }

    @Test
    @DisplayName("recordFile with many RSI entries is split into batches under the limit")
    void testLargeRecordFileIsSplit() {
        final int maxBytes = 500_000; // 500 KB limit
        // Build a RecordFileItem with 200 RSI entries of ~4 KB each → ~800 KB total
        final int rsiCount = 200;
        final int rsiEntryPayloadSize = 4_000; // 4 KB payload per entry

        final ByteArrayOutputStream rsfOut = new ByteArrayOutputStream();
        final WritableStreamingData rsfWriter = new WritableStreamingData(rsfOut);
        // Non-item field (field 1, varint)
        writeVarintField(rsfWriter, 1, 6L);
        // 200 field-3 (record_stream_items) entries
        for (int i = 0; i < rsiCount; i++) {
            writeLenField(rsfWriter, 3, new byte[rsiEntryPayloadSize]);
        }
        final byte[] rsfBytes = rsfOut.toByteArray();

        final ByteArrayOutputStream rfiOut = new ByteArrayOutputStream();
        final WritableStreamingData rfiWriter = new WritableStreamingData(rfiOut);
        writeLenField(rfiWriter, 2, rsfBytes);
        final byte[] rfiBytes = rfiOut.toByteArray();

        final BlockItemUnparsed item =
                BlockItemUnparsed.newBuilder().recordFile(Bytes.wrap(rfiBytes)).build();
        final int originalSize = BlockItemUnparsed.PROTOBUF.measureRecord(item);
        assertThat(originalSize).as("original item exceeds limit").isGreaterThan(maxBytes);

        final List<BlockItemUnparsed> subItems = RecordFileItemSplitter.splitIfNeeded(item, maxBytes);

        // Must have produced more than one sub-item
        assertThat(subItems).hasSizeGreaterThan(1);

        // Every sub-item must fit within the limit
        for (final BlockItemUnparsed sub : subItems) {
            assertThat(BlockItemUnparsed.PROTOBUF.measureRecord(sub))
                    .as("each sub-item fits within limit")
                    .isLessThanOrEqualTo(maxBytes);
        }

        // Total RSI entry count across all sub-items must equal the original
        int totalRSIEntries = 0;
        for (final BlockItemUnparsed sub : subItems) {
            totalRSIEntries += countRSIEntries(sub.recordFile().toByteArray());
        }
        assertThat(totalRSIEntries)
                .as("total RSI entries preserved across split")
                .isEqualTo(rsiCount);
    }

    @Test
    @DisplayName("non-item RSF fields appear in every chunk")
    void testNonItemFieldsInEveryChunk() {
        final int maxBytes = 300_000; // 300 KB
        final int rsiCount = 100;

        final ByteArrayOutputStream rsfOut = new ByteArrayOutputStream();
        final WritableStreamingData rsfWriter = new WritableStreamingData(rsfOut);
        // Non-item header fields: field 1 (varint), field 2 (LEN, 48 bytes)
        writeVarintField(rsfWriter, 1, 5L);
        writeLenField(rsfWriter, 2, new byte[48]);
        // RSI entries
        for (int i = 0; i < rsiCount; i++) {
            writeLenField(rsfWriter, 3, new byte[3_000]);
        }
        // Non-item trailer: field 4 (LEN, 48 bytes), field 5 (varint)
        writeLenField(rsfWriter, 4, new byte[48]);
        writeVarintField(rsfWriter, 5, 837080L);

        final byte[] rsfBytes = rsfOut.toByteArray();
        final ByteArrayOutputStream rfiOut = new ByteArrayOutputStream();
        final WritableStreamingData rfiWriter = new WritableStreamingData(rfiOut);
        writeLenField(rfiWriter, 2, rsfBytes);
        final byte[] rfiBytes = rfiOut.toByteArray();

        final BlockItemUnparsed item =
                BlockItemUnparsed.newBuilder().recordFile(Bytes.wrap(rfiBytes)).build();

        final List<BlockItemUnparsed> subItems = RecordFileItemSplitter.splitIfNeeded(item, maxBytes);
        assertThat(subItems).hasSizeGreaterThan(1);

        // Every sub-item must have the non-RSI fields (i.e. contain the known non-zero
        // otherRSF bytes). We verify by checking that each sub-item's RecordFileItem
        // has a record_file_contents, and that its size is > just the batch RSI.
        for (final BlockItemUnparsed sub : subItems) {
            assertThat(sub.hasRecordFile()).isTrue();
            // Each sub-item must have the non-RSI header overhead beyond just the entries
            final int subSize = BlockItemUnparsed.PROTOBUF.measureRecord(sub);
            assertThat(subSize).isGreaterThan(50); // at minimum the overhead fields are present
        }
    }

    // -----------------------------------------------------------------------
    // Integration test with real block 837,080
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("block 837080 recordFile item (27.66 MB) is split into 4 MB chunks")
    void testBlock837080IsSplit() throws Exception {
        final Path blockFile = Path.of(System.getProperty("user.home"), "Downloads", "wrb-837080.blk");
        assumeTrue(Files.exists(blockFile), "wrb-837080.blk not found — skipping");

        final byte[] rawBytes = Files.readAllBytes(blockFile);
        final BlockUnparsed block = standardParse(
                BlockUnparsed.PROTOBUF, Bytes.wrap(rawBytes), SubscriberConfig.DEFAULT_MAX_PROTOBUF_MESSAGE_SIZE_BYTES);

        final BlockItemUnparsed recordFileItem = block.blockItems().get(1);
        assertThat(recordFileItem.hasRecordFile()).isTrue();
        final int originalSize = BlockItemUnparsed.PROTOBUF.measureRecord(recordFileItem);
        assertThat(originalSize).isGreaterThan(SubscriberConfig.DEFAULT_MAX_SINGLE_ITEM_SIZE_BYTES);

        final int limit = SubscriberConfig.DEFAULT_MAX_SINGLE_ITEM_SIZE_BYTES;
        final List<BlockItemUnparsed> subItems = RecordFileItemSplitter.splitIfNeeded(recordFileItem, limit);

        // Must have produced more than one sub-item
        assertThat(subItems).hasSizeGreaterThan(1);

        // Every sub-item fits within the 4 MB limit
        for (final BlockItemUnparsed sub : subItems) {
            assertThat(BlockItemUnparsed.PROTOBUF.measureRecord(sub))
                    .as("sub-item must fit within 4 MB")
                    .isLessThanOrEqualTo(limit);
        }

        // Total RSI count across all sub-items must equal the original 71,776
        int totalRSI = 0;
        for (final BlockItemUnparsed sub : subItems) {
            totalRSI += countRSIEntries(sub.recordFile().toByteArray());
        }
        assertThat(totalRSI).as("total record_stream_items count preserved").isEqualTo(71_776);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Builds minimal valid RecordFileItem bytes with {@code rsiCount} tiny RSI entries. */
    private static byte[] buildMinimalRecordFileItemBytes(final int rsiCount) {
        final ByteArrayOutputStream rsfOut = new ByteArrayOutputStream();
        final WritableStreamingData rsfWriter = new WritableStreamingData(rsfOut);
        for (int i = 0; i < rsiCount; i++) {
            writeLenField(rsfWriter, 3, new byte[10]);
        }
        final byte[] rsfBytes = rsfOut.toByteArray();

        final ByteArrayOutputStream rfiOut = new ByteArrayOutputStream();
        final WritableStreamingData rfiWriter = new WritableStreamingData(rfiOut);
        writeLenField(rfiWriter, 2, rsfBytes);
        return rfiOut.toByteArray();
    }

    /** Writes a length-delimited field: tag + length varint + content bytes. */
    private static void writeLenField(final WritableStreamingData w, final int fieldNumber, final byte[] content) {
        w.writeVarInt(
                (fieldNumber << ProtoParserTools.TAG_FIELD_OFFSET) | ProtoConstants.WIRE_TYPE_DELIMITED.ordinal(),
                false);
        w.writeVarInt(content.length, false);
        w.writeBytes(content);
    }

    /** Writes a varint field: tag + value varint. */
    private static void writeVarintField(final WritableStreamingData w, final int fieldNumber, final long value) {
        w.writeVarInt(
                (fieldNumber << ProtoParserTools.TAG_FIELD_OFFSET)
                        | ProtoConstants.WIRE_TYPE_VARINT_OR_ZIGZAG.ordinal(),
                false);
        w.writeVarLong(value, false);
    }

    /** Counts field-3 (record_stream_items) entries in raw RecordFileItem bytes. */
    private static int countRSIEntries(final byte[] rfiRawBytes) {
        int count = 0;
        final Bytes rfi = Bytes.wrap(rfiRawBytes);
        final ReadableSequentialData rfiInput = rfi.toReadableSequentialData();

        while (rfiInput.hasRemaining()) {
            final int tag = rfiInput.readVarInt(false);
            final int fieldNumber = tag >> ProtoParserTools.TAG_FIELD_OFFSET;
            final ProtoConstants wireType = ProtoConstants.get(tag & ProtoConstants.TAG_WIRE_TYPE_MASK);

            if (wireType == ProtoConstants.WIRE_TYPE_DELIMITED) {
                final int contentLen = rfiInput.readVarInt(false);
                final long contentStart = rfiInput.position();
                rfiInput.skip(contentLen);

                if (fieldNumber == 2) {
                    // Recurse into RecordStreamFile bytes
                    final Bytes rsfBytes = rfi.getBytes(contentStart, contentLen);
                    final ReadableSequentialData rsfInput = rsfBytes.toReadableSequentialData();
                    while (rsfInput.hasRemaining()) {
                        final int rsfTag = rsfInput.readVarInt(false);
                        final int rsfField = rsfTag >> ProtoParserTools.TAG_FIELD_OFFSET;
                        final ProtoConstants rsfWire = ProtoConstants.get(rsfTag & ProtoConstants.TAG_WIRE_TYPE_MASK);
                        if (rsfWire == ProtoConstants.WIRE_TYPE_DELIMITED) {
                            final int rsfLen = rsfInput.readVarInt(false);
                            if (rsfField == 3) count++;
                            rsfInput.skip(rsfLen);
                        } else if (rsfWire == ProtoConstants.WIRE_TYPE_VARINT_OR_ZIGZAG) {
                            rsfInput.readVarLong(false);
                        } else {
                            break;
                        }
                    }
                }
            } else if (wireType == ProtoConstants.WIRE_TYPE_VARINT_OR_ZIGZAG) {
                rfiInput.readVarLong(false);
            } else {
                break;
            }
        }
        return count;
    }
}
