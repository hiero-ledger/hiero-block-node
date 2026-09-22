// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import com.hedera.pbj.runtime.ProtoConstants;
import com.hedera.pbj.runtime.ProtoParserTools;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;

/**
 * Splits an oversized {@code RECORD_FILE} {@link BlockItemUnparsed} into smaller chunks
 * by partitioning the {@code RecordStreamFile.record_stream_items} repeated field at the
 * wire-format level, so that no resulting item exceeds a configured byte limit.
 *
 * <p>Proto field numbers used for wire-level parsing:
 * <ul>
 *   <li>{@code RecordFileItem}: creation_time=1, record_file_contents=2,
 *       sidecar_file_contents=3, amendments=4</li>
 *   <li>{@code RecordStreamFile}: hapi_proto_version=1, start_object_running_hash=2,
 *       record_stream_items=3, end_object_running_hash=4, block_number=5, sidecars=6</li>
 * </ul>
 *
 * <p>Non-{@code record_stream_items} fields (running hashes, version, block number) are
 * copied verbatim into every chunk so that each chunk is a structurally valid, if
 * partial, {@code RecordFileItem} message.
 */
final class RecordFileItemSplitter {

    // RecordFileItem field that carries the RecordStreamFile (LEN, wire type 2)
    private static final int RFI_RECORD_FILE_CONTENTS_FIELD = 2;
    // RecordStreamFile field that carries repeated RecordStreamItem entries (LEN, wire type 2)
    private static final int RSF_RECORD_STREAM_ITEMS_FIELD = 3;

    private RecordFileItemSplitter() {}

    /**
     * Splits the item if it is a {@code RECORD_FILE} type whose serialised size exceeds
     * {@code maxBytesPerItem}.  Returns a single-element list containing the original item
     * when no split is needed or possible.
     *
     * @param item            the block item to examine
     * @param maxBytesPerItem the per-item byte limit (typically
     *                        {@link SubscriberConfig#maxSingleItemSizeBytes()})
     * @return split sub-items, or a single-element list with the original item unchanged
     */
    static List<BlockItemUnparsed> splitIfNeeded(final BlockItemUnparsed item, final int maxBytesPerItem) {
        if (!item.hasRecordFile()) {
            return List.of(item);
        }
        final int size = BlockItemUnparsed.PROTOBUF.measureRecord(item);
        if (size <= maxBytesPerItem) {
            return List.of(item);
        }
        return split(item.recordFile(), maxBytesPerItem);
    }

    // -----------------------------------------------------------------------
    // Private implementation
    // -----------------------------------------------------------------------

    private static List<BlockItemUnparsed> split(final Bytes rfiBytes, final int maxBytesPerItem) {

        // --- Step 1: parse RecordFileItem to isolate record_file_contents (field 2) ---
        Bytes rsfBytes = null;
        Bytes otherRFI = Bytes.EMPTY;

        final ReadableSequentialData rfiInput = rfiBytes.toReadableSequentialData();
        while (rfiInput.hasRemaining()) {
            final long fieldStart = rfiInput.position();
            final int tag = rfiInput.readVarInt(false);
            final int fieldNumber = tag >> ProtoParserTools.TAG_FIELD_OFFSET;
            final ProtoConstants wireType = ProtoConstants.get(tag & ProtoConstants.TAG_WIRE_TYPE_MASK);

            if (wireType == ProtoConstants.WIRE_TYPE_DELIMITED) {
                final int contentLen = rfiInput.readVarInt(false);
                final long contentStart = rfiInput.position();
                rfiInput.skip(contentLen);
                if (fieldNumber == RFI_RECORD_FILE_CONTENTS_FIELD) {
                    rsfBytes = rfiBytes.getBytes(contentStart, contentLen);
                } else {
                    otherRFI = Bytes.merge(otherRFI, rfiBytes.getBytes(fieldStart, rfiInput.position() - fieldStart));
                }
            } else if (wireType == ProtoConstants.WIRE_TYPE_VARINT_OR_ZIGZAG) {
                rfiInput.readVarLong(false);
                otherRFI = Bytes.merge(otherRFI, rfiBytes.getBytes(fieldStart, rfiInput.position() - fieldStart));
            } else {
                // Unexpected wire type — abort split, return original
                return List.of(
                        BlockItemUnparsed.newBuilder().recordFile(rfiBytes).build());
            }
        }

        if (rsfBytes == null) {
            return List.of(BlockItemUnparsed.newBuilder().recordFile(rfiBytes).build());
        }

        // --- Step 2: parse RecordStreamFile to isolate record_stream_items (field 3) ---
        Bytes otherRSF = Bytes.EMPTY;
        final List<Bytes> rsiEntries = new ArrayList<>();

        final ReadableSequentialData rsfInput = rsfBytes.toReadableSequentialData();
        while (rsfInput.hasRemaining()) {
            final long fieldStart = rsfInput.position();
            final int tag = rsfInput.readVarInt(false);
            final int fieldNumber = tag >> ProtoParserTools.TAG_FIELD_OFFSET;
            final ProtoConstants wireType = ProtoConstants.get(tag & ProtoConstants.TAG_WIRE_TYPE_MASK);

            if (wireType == ProtoConstants.WIRE_TYPE_DELIMITED) {
                final int contentLen = rsfInput.readVarInt(false);
                rsfInput.skip(contentLen);
                final Bytes fieldBytes = rsfBytes.getBytes(fieldStart, rsfInput.position() - fieldStart);
                if (fieldNumber == RSF_RECORD_STREAM_ITEMS_FIELD) {
                    rsiEntries.add(fieldBytes);
                } else {
                    otherRSF = Bytes.merge(otherRSF, fieldBytes);
                }
            } else if (wireType == ProtoConstants.WIRE_TYPE_VARINT_OR_ZIGZAG) {
                rsfInput.readVarLong(false);
                otherRSF = Bytes.merge(otherRSF, rsfBytes.getBytes(fieldStart, rsfInput.position() - fieldStart));
            } else {
                // Unexpected wire type — abort
                return List.of(
                        BlockItemUnparsed.newBuilder().recordFile(rfiBytes).build());
            }
        }

        if (rsiEntries.isEmpty()) {
            return List.of(BlockItemUnparsed.newBuilder().recordFile(rfiBytes).build());
        }

        // --- Step 3: compute how many RSI bytes fit per chunk ---
        // measureRecord(BlockItemUnparsed{recordFile=X}) ≈
        //   1 byte (field-10 tag) + 4 bytes (len varint for ~4 MB) +   ← outer wrapper
        //   1 byte (field-2 tag) + 4 bytes (len varint)                ← RFI field-2 wrapper
        //   + otherRFI.length + otherRSF.length + batchSize
        // Using 16 bytes as a safe upper bound for all tag/length varints.
        final int fixedOverhead = 16 + (int) otherRFI.length() + (int) otherRSF.length();
        final int availableForRSI = maxBytesPerItem - fixedOverhead;

        if (availableForRSI <= 0) {
            return List.of(BlockItemUnparsed.newBuilder().recordFile(rfiBytes).build());
        }

        // --- Step 4: bucket RSI entries into batches ---
        final List<List<Bytes>> batches = new ArrayList<>();
        List<Bytes> currentBatch = new ArrayList<>();
        int currentBatchSize = 0;

        for (final Bytes entry : rsiEntries) {
            final int entrySize = (int) entry.length();
            if (!currentBatch.isEmpty() && currentBatchSize + entrySize > availableForRSI) {
                batches.add(currentBatch);
                currentBatch = new ArrayList<>();
                currentBatchSize = 0;
            }
            currentBatch.add(entry);
            currentBatchSize += entrySize;
        }
        if (!currentBatch.isEmpty()) {
            batches.add(currentBatch);
        }

        if (batches.size() <= 1) {
            // All items already fit in one chunk — return original
            return List.of(BlockItemUnparsed.newBuilder().recordFile(rfiBytes).build());
        }

        // --- Step 5: reconstruct a BlockItemUnparsed per batch ---
        final int rfiField2Tag = (RFI_RECORD_FILE_CONTENTS_FIELD << ProtoParserTools.TAG_FIELD_OFFSET)
                | ProtoConstants.WIRE_TYPE_DELIMITED.ordinal();
        final List<BlockItemUnparsed> result = new ArrayList<>(batches.size());
        for (final List<Bytes> batch : batches) {
            // Build RecordStreamFile bytes: non-RSI fields + this batch's RSI entries
            final ByteArrayOutputStream rsfOut = new ByteArrayOutputStream();
            final WritableStreamingData rsfWriter = new WritableStreamingData(rsfOut);
            rsfWriter.writeBytes(otherRSF);
            for (final Bytes entry : batch) {
                rsfWriter.writeBytes(entry);
            }
            final byte[] newRSFBytes = rsfOut.toByteArray();

            // Build RecordFileItem bytes: field 2 (RecordStreamFile) + other RFI fields
            final ByteArrayOutputStream rfiOut = new ByteArrayOutputStream();
            final WritableStreamingData rfiWriter = new WritableStreamingData(rfiOut);
            rfiWriter.writeVarInt(rfiField2Tag, false);
            rfiWriter.writeVarInt(newRSFBytes.length, false);
            rfiWriter.writeBytes(newRSFBytes);
            rfiWriter.writeBytes(otherRFI);

            result.add(BlockItemUnparsed.newBuilder()
                    .recordFile(Bytes.wrap(rfiOut.toByteArray()))
                    .build());
        }
        return result;
    }
}
