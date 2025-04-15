// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.luben.zstd.Zstd;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.hiero.hapi.block.node.BlockUnparsed;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit Test for BlockAccessor interface and its default methods.
 */
public class BlockAccessorTest {
    private static final Block SAMPLE_BLOCK = new Block(List.of(
            new BlockItem(new OneOf<>(
                    ItemOneOfType.BLOCK_HEADER,
                    new BlockHeader(
                            new SemanticVersion(1, 2, 3, "a", "b"),
                            new SemanticVersion(4, 5, 6, "c", "d"),
                            123,
                            new Timestamp(123L, 456),
                            BlockHashAlgorithm.SHA2_384))),
            new BlockItem(new OneOf<>(ItemOneOfType.ROUND_HEADER, new RoundHeader(827))),
            new BlockItem(new OneOf<>(
                    ItemOneOfType.BLOCK_PROOF,
                    new BlockProof(
                            890,
                            Bytes.wrap("previousBlockRootHash".getBytes()),
                            Bytes.wrap("startOfBlockStateRootHash".getBytes()),
                            Bytes.wrap("signature".getBytes()),
                            Collections.emptyList())))));
    private static final Bytes SAMPLE_BLOCK_PROTOBUF_BYTES;
    private static final Bytes SAMPLE_BLOCK_ZSTD_PROTOBUF_BYTES;
    private static final Bytes SAMPLE_BLOCK_JSON_BYTES;

    static {
        SAMPLE_BLOCK_PROTOBUF_BYTES = Block.PROTOBUF.toBytes(SAMPLE_BLOCK);
        SAMPLE_BLOCK_ZSTD_PROTOBUF_BYTES = Bytes.wrap(Zstd.compress(SAMPLE_BLOCK_PROTOBUF_BYTES.toByteArray()));
        SAMPLE_BLOCK_JSON_BYTES = Block.JSON.toBytes(SAMPLE_BLOCK);
    }

    private static class TestBlockAccessor implements BlockAccessor {
        @Override
        public Block block() {
            return SAMPLE_BLOCK;
        }
    }

    @Test
    @DisplayName("Test availableFormats method")
    void testAvailableFormats() {
        BlockAccessor accessor = new TestBlockAccessor();
        List<Format> formats = accessor.availableFormats();
        assertEquals(BlockAccessor.ALL_FORMATS, formats);
    }

    @Test
    @DisplayName("Test blockUnparsed method")
    void testBlockUnparsed() throws ParseException {
        BlockAccessor accessor = new TestBlockAccessor();
        BlockUnparsed blockUnparsed = BlockUnparsed.PROTOBUF.parse(SAMPLE_BLOCK_PROTOBUF_BYTES);
        assertEquals(blockUnparsed, accessor.blockUnparsed());
        // create a parsing failure
        BlockAccessor emptyAccessor = new BlockAccessor() {
            @Override
            public Block block() {
                return null;
            }

            @Override
            public Bytes blockBytes(Format format) throws IllegalArgumentException {
                return Bytes.wrap(new byte[] {1, 2, 3, 4, 5});
            }
        };
        assertThrows(RuntimeException.class, emptyAccessor::blockUnparsed);
    }

    @Test
    @DisplayName("Test blockBytes method with unsupported format")
    void testBlockBytesUnsupportedFormat() {
        BlockAccessor accessor = new TestBlockAccessor();
        assertThrows(IllegalArgumentException.class, () -> accessor.blockBytes(null));
    }

    @Test
    @DisplayName("Test blockBytes method with supported format")
    void testBlockBytesSupportedFormats() {
        BlockAccessor accessor = new TestBlockAccessor();
        assertEquals(SAMPLE_BLOCK_PROTOBUF_BYTES, accessor.blockBytes(Format.PROTOBUF));
        assertEquals(SAMPLE_BLOCK_ZSTD_PROTOBUF_BYTES, accessor.blockBytes(Format.ZSTD_PROTOBUF));
        assertEquals(SAMPLE_BLOCK_JSON_BYTES, accessor.blockBytes(Format.JSON));
    }

    @Test
    @DisplayName("Test writeBytesTo WritableSequentialData method")
    void testWriteBytesToWritableSequentialData() {
        BlockAccessor accessor = new TestBlockAccessor();
        // Format.PROTOBUF
        BufferedData protobufOut = BufferedData.allocate((int) SAMPLE_BLOCK_PROTOBUF_BYTES.length());
        accessor.writeBytesTo(Format.PROTOBUF, protobufOut);
        assertEquals(SAMPLE_BLOCK_PROTOBUF_BYTES, protobufOut.getBytes(0, protobufOut.length()));
        // Format.ZSTD_PROTOBUF
        BufferedData zstdProtobufOut = BufferedData.allocate((int) SAMPLE_BLOCK_ZSTD_PROTOBUF_BYTES.length());
        accessor.writeBytesTo(Format.ZSTD_PROTOBUF, zstdProtobufOut);
        assertEquals(SAMPLE_BLOCK_ZSTD_PROTOBUF_BYTES, zstdProtobufOut.getBytes(0, zstdProtobufOut.length()));
        // Format.JSON
        BufferedData jsonOut = BufferedData.allocate((int) SAMPLE_BLOCK_JSON_BYTES.length());
        accessor.writeBytesTo(Format.JSON, jsonOut);
        assertEquals(SAMPLE_BLOCK_JSON_BYTES, jsonOut.getBytes(0, jsonOut.length()));
    }

    @Test
    @DisplayName("Test writeBytesTo OutputStream method")
    void testWriteBytesToOutputStream() {
        BlockAccessor accessor = new TestBlockAccessor();
        // Format.PROTOBUF
        ByteArrayOutputStream protobufOut = new ByteArrayOutputStream();
        accessor.writeBytesTo(Format.PROTOBUF, protobufOut);
        assertEquals(SAMPLE_BLOCK_PROTOBUF_BYTES, Bytes.wrap(protobufOut.toByteArray()));
        // Format.ZSTD_PROTOBUF
        ByteArrayOutputStream zstdProtobufOut = new ByteArrayOutputStream();
        accessor.writeBytesTo(Format.ZSTD_PROTOBUF, zstdProtobufOut);
        assertEquals(SAMPLE_BLOCK_ZSTD_PROTOBUF_BYTES, Bytes.wrap(zstdProtobufOut.toByteArray()));
        // Format.JSON
        ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
        accessor.writeBytesTo(Format.JSON, jsonOut);
        assertEquals(SAMPLE_BLOCK_JSON_BYTES, Bytes.wrap(jsonOut.toByteArray()));
    }

    @Test
    @DisplayName("Test writeTo Path method")
    void testWriteToPath() throws IOException {
        BlockAccessor accessor = new TestBlockAccessor();
        // Format.PROTOBUF
        Path tempFileProtobuf = Files.createTempFile("protobuf", ".tmp");
        accessor.writeTo(Format.PROTOBUF, tempFileProtobuf);
        assertEquals(SAMPLE_BLOCK_PROTOBUF_BYTES, Bytes.wrap(Files.readAllBytes(tempFileProtobuf)));
        Files.delete(tempFileProtobuf);
        // Format.ZSTD_PROTOBUF
        Path tempFileZstdProtobuf = Files.createTempFile("zstd_protobuf", ".tmp");
        accessor.writeTo(Format.ZSTD_PROTOBUF, tempFileZstdProtobuf);
        assertEquals(SAMPLE_BLOCK_ZSTD_PROTOBUF_BYTES, Bytes.wrap(Files.readAllBytes(tempFileZstdProtobuf)));
        Files.delete(tempFileZstdProtobuf);
        // Format.JSON
        Path tempFileJson = Files.createTempFile("json", ".tmp");
        accessor.writeTo(Format.JSON, tempFileJson);
        assertEquals(SAMPLE_BLOCK_JSON_BYTES, Bytes.wrap(Files.readAllBytes(tempFileJson)));
        Files.delete(tempFileJson);
    }
}
