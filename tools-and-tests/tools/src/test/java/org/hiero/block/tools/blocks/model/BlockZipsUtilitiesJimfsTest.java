// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities.BlockSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BlockZipsUtilities} using jimfs in-memory file system.
 */
class BlockZipsUtilitiesJimfsTest {

    private FileSystem fs;
    private Path root;

    @BeforeEach
    void setUp() throws IOException {
        fs = Jimfs.newFileSystem(Configuration.unix());
        root = fs.getPath("/blocks");
        Files.createDirectories(root);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }

    @Test
    void findBlockSources_discoversBlkFiles() throws IOException {
        // Create some .blk files
        writeMinimalBlock(root.resolve("0.blk"), 0);
        writeMinimalBlock(root.resolve("1.blk"), 1);
        writeMinimalBlock(root.resolve("2.blk"), 2);

        List<BlockSource> sources = BlockZipsUtilities.findBlockSources(new Path[] {root}, null);

        assertEquals(3, sources.size());
        assertEquals(0, sources.get(0).blockNumber());
        assertEquals(1, sources.get(1).blockNumber());
        assertEquals(2, sources.get(2).blockNumber());
        assertFalse(sources.get(0).isZipEntry());
    }

    @Test
    void findBlockSources_discoversZstdFiles() throws IOException {
        // Create .blk.zstd files (just empty stubs for source discovery)
        Files.write(root.resolve("10.blk.zstd"), new byte[] {0});
        Files.write(root.resolve("20.blk.zstd"), new byte[] {0});

        List<BlockSource> sources = BlockZipsUtilities.findBlockSources(new Path[] {root}, null);

        assertEquals(2, sources.size());
        assertEquals(10, sources.get(0).blockNumber());
        assertEquals(20, sources.get(1).blockNumber());
        assertEquals(CompressionType.ZSTD, sources.get(0).compressionType());
    }

    @Test
    void findBlockSources_sortsByBlockNumber() throws IOException {
        writeMinimalBlock(root.resolve("99.blk"), 99);
        writeMinimalBlock(root.resolve("5.blk"), 5);
        writeMinimalBlock(root.resolve("42.blk"), 42);

        List<BlockSource> sources = BlockZipsUtilities.findBlockSources(new Path[] {root}, null);

        assertEquals(3, sources.size());
        assertEquals(5, sources.get(0).blockNumber());
        assertEquals(42, sources.get(1).blockNumber());
        assertEquals(99, sources.get(2).blockNumber());
    }

    @Test
    void findBlockSources_skipsNonexistentPaths() {
        Path missing = fs.getPath("/does-not-exist");
        List<BlockSource> sources = BlockZipsUtilities.findBlockSources(new Path[] {missing}, null);
        assertTrue(sources.isEmpty());
    }

    @Test
    void findBlockSources_emptyDirectory() throws IOException {
        Path emptyDir = fs.getPath("/empty");
        Files.createDirectories(emptyDir);

        List<BlockSource> sources = BlockZipsUtilities.findBlockSources(new Path[] {emptyDir}, null);
        assertTrue(sources.isEmpty());
    }

    @Test
    void findBlockSources_ignoresNonBlockFiles() throws IOException {
        Files.write(root.resolve("readme.txt"), new byte[] {});
        Files.write(root.resolve("data.json"), new byte[] {});
        writeMinimalBlock(root.resolve("7.blk"), 7);

        List<BlockSource> sources = BlockZipsUtilities.findBlockSources(new Path[] {root}, null);

        assertEquals(1, sources.size());
        assertEquals(7, sources.get(0).blockNumber());
    }

    @Test
    void findBlockSources_handlesNestedDirectories() throws IOException {
        Path subDir = root.resolve("sub");
        Files.createDirectories(subDir);
        writeMinimalBlock(subDir.resolve("3.blk"), 3);
        writeMinimalBlock(root.resolve("1.blk"), 1);

        List<BlockSource> sources = BlockZipsUtilities.findBlockSources(new Path[] {root}, null);

        assertEquals(2, sources.size());
        assertEquals(1, sources.get(0).blockNumber());
        assertEquals(3, sources.get(1).blockNumber());
    }

    @Test
    void findBlockSources_discoversZipEntries() throws Exception {
        // Create a zip file with block entries using ZipFileSystem
        Path zipPath = root.resolve("blocks.zip");
        URI zipUri = URI.create("jar:" + zipPath.toUri());
        try (FileSystem zipFs = java.nio.file.FileSystems.newFileSystem(zipUri, java.util.Map.of("create", "true"))) {
            Path entry0 = zipFs.getPath("/0.blk");
            Files.write(entry0, createMinimalBlockBytes(0));
            Path entry1 = zipFs.getPath("/1.blk");
            Files.write(entry1, createMinimalBlockBytes(1));
        }

        List<BlockSource> sources = BlockZipsUtilities.findBlockSources(new Path[] {root}, null);

        assertEquals(2, sources.size());
        assertEquals(0, sources.get(0).blockNumber());
        assertTrue(sources.get(0).isZipEntry());
        assertEquals(1, sources.get(1).blockNumber());
        assertTrue(sources.get(1).isZipEntry());
    }

    @Test
    void findBlockSources_corruptZipCounterIncrements() throws IOException {
        // Write a corrupt zip file (just random bytes)
        Files.write(root.resolve("corrupt.zip"), new byte[] {0x50, 0x4B, 0x00, 0x00});

        AtomicLong counter = new AtomicLong(0);
        List<BlockSource> sources = BlockZipsUtilities.findBlockSources(new Path[] {root}, counter);

        assertTrue(sources.isEmpty());
        assertEquals(1, counter.get());
    }

    @Test
    void extractBlockNumber_validNames() {
        assertEquals(0, BlockZipsUtilities.extractBlockNumber("0.blk"));
        assertEquals(42, BlockZipsUtilities.extractBlockNumber("42.blk"));
        assertEquals(123456, BlockZipsUtilities.extractBlockNumber("123456.blk.zstd"));
        assertEquals(99, BlockZipsUtilities.extractBlockNumber("99.blk.gz"));
    }

    @Test
    void extractBlockNumber_invalidNames() {
        assertEquals(-1, BlockZipsUtilities.extractBlockNumber("readme.txt"));
        assertEquals(-1, BlockZipsUtilities.extractBlockNumber("block.blk"));
        assertEquals(-1, BlockZipsUtilities.extractBlockNumber(""));
    }

    @Test
    void getCompressionType_values() {
        assertEquals(CompressionType.NONE, BlockZipsUtilities.getCompressionType("0.blk"));
        assertEquals(CompressionType.ZSTD, BlockZipsUtilities.getCompressionType("0.blk.zstd"));
        assertEquals(null, BlockZipsUtilities.getCompressionType("0.blk.gz"));
    }

    /** Write a minimal protobuf-encoded block to the given path. */
    private static void writeMinimalBlock(Path path, long blockNumber) throws IOException {
        Files.write(path, createMinimalBlockBytes(blockNumber));
    }

    /** Create minimal protobuf-encoded block bytes. */
    private static byte[] createMinimalBlockBytes(long blockNumber) {
        BlockHeader header = BlockHeader.newBuilder().number(blockNumber).build();
        BlockItem item = BlockItem.newBuilder().blockHeader(header).build();
        Block block = Block.newBuilder().items(List.of(item)).build();
        return Block.PROTOBUF.toBytes(block).toByteArray();
    }
}
