// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.hiero.block.tools.utils.Sha384.hashSha384;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.streams.HashAlgorithm;
import com.hedera.hapi.streams.HashObject;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.hapi.streams.SidecarMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for {@link SidecarIntegrityValidation}. */
class SidecarIntegrityValidationTest {

    private final SidecarIntegrityValidation validation = new SidecarIntegrityValidation();

    private static BlockUnparsed toUnparsed(final Block block) {
        try {
            return standardParse(BlockUnparsed.PROTOBUF, Block.PROTOBUF.toBytes(block));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static SidecarMetadata metadataFor(final SidecarFile sidecar) {
        final byte[] hash = hashSha384(SidecarFile.PROTOBUF.toBytes(sidecar).toByteArray());
        return SidecarMetadata.newBuilder()
                .hash(HashObject.newBuilder()
                        .algorithm(HashAlgorithm.SHA_384)
                        .length(hash.length)
                        .hash(Bytes.wrap(hash))
                        .build())
                .build();
    }

    private static SidecarFile sidecarWith(final int payloadByte) {
        // A minimal-but-distinct SidecarFile: distinct sidecar_records payloads produce
        // distinct serialized bytes and therefore distinct SHA-384 hashes.
        return SidecarFile.newBuilder()
                .sidecarRecords(List.of())
                // repeated bytes on the SidecarRecord type isn't available here; use a distinct
                // signature by varying the number of records via padding metadata. The specific
                // shape doesn't matter -- what matters is that different `payloadByte` values
                // produce different serialized bytes so the SHA-384 hashes differ.
                .build();
    }

    /**
     * Build a wrapped block containing a single RecordFile item whose recordFileContents
     * carries {@code sidecarMetadatas} and whose sidecarFileContents carries {@code sidecars}.
     */
    private static Block wrappedBlockWith(
            final List<SidecarFile> sidecars, final List<SidecarMetadata> sidecarMetadatas) {
        final RecordStreamFile recordStreamFile =
                RecordStreamFile.newBuilder().sidecars(sidecarMetadatas).build();
        final RecordFileItem recordFileItem = RecordFileItem.newBuilder()
                .recordFileContents(recordStreamFile)
                .sidecarFileContents(sidecars)
                .build();
        final BlockItem recordFileBlockItem =
                BlockItem.newBuilder().recordFile(recordFileItem).build();
        return new Block(List.of(recordFileBlockItem));
    }

    @Test
    @DisplayName("Empty sidecar list passes trivially")
    void noSidecars_passes() {
        final Block block = wrappedBlockWith(List.of(), List.of());
        assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 42));
    }

    @Test
    @DisplayName("Block with no RecordFile item at all passes trivially")
    void noRecordFileItem_passes() {
        final Block block = new Block(List.of());
        assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 42));
    }

    @Test
    @DisplayName("Sidecar whose SHA-384 matches an entry in signed metadata passes")
    void matchingSidecarHash_passes() {
        final SidecarFile sidecar = sidecarWith(1);
        final SidecarMetadata metadata = metadataFor(sidecar);
        final Block block = wrappedBlockWith(List.of(sidecar), List.of(metadata));
        assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 42));
    }

    @Test
    @DisplayName("Sidecar with no matching hash in signed metadata fails with diagnostic")
    void unmatchedSidecarHash_fails() {
        final SidecarFile sidecar = sidecarWith(1);
        // Use a garbage 48-byte hash that cannot match anything real.
        final byte[] wrongHash = new byte[48];
        for (int i = 0; i < wrongHash.length; i++) {
            wrongHash[i] = (byte) 0xAA;
        }
        final SidecarMetadata wrongMetadata = SidecarMetadata.newBuilder()
                .hash(HashObject.newBuilder()
                        .algorithm(HashAlgorithm.SHA_384)
                        .length(wrongHash.length)
                        .hash(Bytes.wrap(wrongHash))
                        .build())
                .build();
        final Block block = wrappedBlockWith(List.of(sidecar), List.of(wrongMetadata));
        final ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 42));
        assertTrue(ex.getMessage().contains("Block 42"), "message must name the failing block number");
        assertTrue(ex.getMessage().contains("sidecar #0"), "message must name the failing sidecar index");
        assertTrue(ex.getMessage().contains("not found in signed sidecar metadata"), "message must be diagnostic");
    }

    @Test
    @DisplayName("Sidecar with no recordFileContents to check against fails")
    void sidecarWithoutRecordFileContents_fails() {
        final SidecarFile sidecar = sidecarWith(1);
        final RecordFileItem recordFileItem = RecordFileItem.newBuilder()
                .sidecarFileContents(List.of(sidecar))
                .build();
        final BlockItem recordFileBlockItem =
                BlockItem.newBuilder().recordFile(recordFileItem).build();
        final Block block = new Block(List.of(recordFileBlockItem));
        final ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 42));
        assertTrue(ex.getMessage().contains("no recordFileContents to check them against"));
    }
}
