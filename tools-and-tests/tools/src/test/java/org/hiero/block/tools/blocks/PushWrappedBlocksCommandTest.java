// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.tools.blocks.model.BlockArchiveType;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.utils.Sha384;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Unit tests for {@link PushWrappedBlocksCommand}. Covers the parts of the command reachable
 * without a live Block Node: input-directory validation and the "nothing local to push" fast path,
 * which must return without ever attempting to contact a BN. Actual push/ACK behavior against a real
 * BN is exercised by the wrb-distribution Solo E2E test (steps 8-9 of #3125), matching the same
 * scoping boundary {@link org.hiero.block.tools.push.LiveBlockPushClientTest} already draws.
 */
class PushWrappedBlocksCommandTest {

    /** Port 1 is reserved and effectively always refuses TCP connections — used as an unreachable target. */
    private static final int UNREACHABLE_PORT = 1;

    private int runCommand(final Path dir) {
        return new CommandLine(new PushWrappedBlocksCommand())
                .execute("--input-dir", dir.toString(), "--bn-host", "127.0.0.1", "--bn-port", "" + UNREACHABLE_PORT);
    }

    private Block createTestBlock(final long blockNumber) {
        final BlockHeader header = BlockHeader.newBuilder().number(blockNumber).build();
        final BlockItem headerItem = BlockItem.newBuilder().blockHeader(header).build();
        return Block.newBuilder().items(List.of(headerItem)).build();
    }

    /** Writes a block and registers it in the blockStreamBlockHashes.bin watermark, mirroring what
     * ToWrappedBlocksCommand does for each block it wraps. */
    private void writeWrappedBlock(final Path dir, final long blockNumber) throws Exception {
        BlockWriter.writeBlock(dir, createTestBlock(blockNumber), BlockArchiveType.INDIVIDUAL_FILES);
        try (final BlockStreamBlockHashRegistry registry =
                new BlockStreamBlockHashRegistry(dir.resolve("blockStreamBlockHashes.bin"))) {
            registry.addBlock(blockNumber, new byte[Sha384.SHA_384_HASH_SIZE]);
        }
    }

    @Nested
    @DisplayName("Input validation")
    class InputValidation {

        @Test
        @DisplayName("missing input directory returns exit code 1")
        void missingInputDirReturns1(@TempDir final Path tempDir) {
            final Path missing = tempDir.resolve("does-not-exist");
            assertEquals(1, runCommand(missing));
        }
    }

    @Nested
    @DisplayName("Nothing-to-push fast path")
    class NothingToPush {

        @Test
        @DisplayName("empty input directory returns 0 without contacting a BN")
        @Timeout(10)
        void emptyDirectoryReturns0Fast(@TempDir final Path tempDir) {
            // No blockStreamBlockHashes.bin and no blocks written: highestBlockNumberStored() is -1, so
            // the command must return before ever calling queryLastAvailableBlock(). Bounding this test
            // to 10s proves that: an unreachable BN query would otherwise take much longer to time out.
            assertEquals(0, runCommand(tempDir));
        }
    }

    @Test
    @DisplayName("a wrapped block establishes the local watermark used to detect what's pushable")
    void writingBlockEstablishesWatermark(@TempDir final Path tempDir) throws Exception {
        writeWrappedBlock(tempDir, 0L);
        try (final BlockStreamBlockHashRegistry registry =
                new BlockStreamBlockHashRegistry(tempDir.resolve("blockStreamBlockHashes.bin"))) {
            assertEquals(0L, registry.highestBlockNumberStored());
        }
    }
}
