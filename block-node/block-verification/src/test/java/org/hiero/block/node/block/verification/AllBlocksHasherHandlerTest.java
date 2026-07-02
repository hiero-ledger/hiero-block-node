// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests for {@link AllBlocksHasherHandler}.
@DisplayName("AllBlocksHasherHandler Tests")
class AllBlocksHasherHandlerTest {

    private FileSystem jimfs;
    private Path hasherPath;

    @BeforeEach
    void setUp() {
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        hasherPath = jimfs.getPath("/state/hasher.bin");
    }

    @AfterEach
    void tearDown() throws IOException {
        jimfs.close();
    }

    private VerificationConfig disabledConfig() {
        return new VerificationConfig(hasherPath, false, false, 100, hasherPath, 100, true, false, hasherPath, 7);
    }

    private VerificationConfig enabledConfig(final int persistenceInterval) {
        return new VerificationConfig(
                hasherPath, true, false, persistenceInterval, hasherPath, 100, true, false, hasherPath, 7);
    }

    private static BlockNodeContext contextWith(final SimpleInMemoryHistoricalBlockFacility facility) {
        return new BlockNodeContext(null, null, null, null, facility, null, null, null, null, null, null, null, null);
    }

    // ─── Disabled config ──────────────────────────────────────────────────────

    @Test
    @DisplayName("disabled: isAvailable() returns false")
    void disabled_isAvailable_returnsFalse() {
        final AllBlocksHasherHandler handler =
                new AllBlocksHasherHandler(disabledConfig(), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        assertThat(handler.isAvailable()).isFalse();
    }

    @Test
    @DisplayName("disabled: getNumberOfBlocks() returns -1")
    void disabled_getNumberOfBlocks_returnsMinusOne() {
        final AllBlocksHasherHandler handler =
                new AllBlocksHasherHandler(disabledConfig(), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        assertThat(handler.getNumberOfBlocks()).isEqualTo(-1L);
    }

    @Test
    @DisplayName("disabled: lastBlockHash() returns null")
    void disabled_lastBlockHash_returnsNull() {
        final AllBlocksHasherHandler handler =
                new AllBlocksHasherHandler(disabledConfig(), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        assertThat(handler.lastBlockHash()).isNull();
    }

    @Test
    @DisplayName("disabled: computeRootHash() returns null")
    void disabled_computeRootHash_returnsNull() {
        final AllBlocksHasherHandler handler =
                new AllBlocksHasherHandler(disabledConfig(), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        assertThat(handler.computeRootHash()).isNull();
    }

    @Test
    @DisplayName("disabled: start() is a no-op (does not crash)")
    void disabled_start_isNoOp() {
        final AllBlocksHasherHandler handler =
                new AllBlocksHasherHandler(disabledConfig(), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        handler.start();
        assertThat(handler.isAvailable()).isFalse();
    }

    // ─── Genesis path (empty store) ───────────────────────────────────────────

    @Test
    @DisplayName("genesis: isAvailable() returns true")
    void genesis_isAvailable_returnsTrue() {
        final AllBlocksHasherHandler handler = new AllBlocksHasherHandler(
                enabledConfig(100), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        assertThat(handler.isAvailable()).isTrue();
    }

    @Test
    @DisplayName("genesis: getNumberOfBlocks() returns 0")
    void genesis_getNumberOfBlocks_returnsZero() {
        final AllBlocksHasherHandler handler = new AllBlocksHasherHandler(
                enabledConfig(100), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        assertThat(handler.getNumberOfBlocks()).isEqualTo(0L);
    }

    @Test
    @DisplayName("genesis: lastBlockHash() returns ZERO_BLOCK_HASH")
    void genesis_lastBlockHash_returnsZeroHash() {
        final AllBlocksHasherHandler handler = new AllBlocksHasherHandler(
                enabledConfig(100), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        assertThat(handler.lastBlockHash()).isEqualTo(AllBlocksHasherHandler.ZERO_BLOCK_HASH);
    }

    @Test
    @DisplayName("genesis: computeRootHash() returns ZERO_BLOCK_HASH when no leaves")
    void genesis_computeRootHash_returnsZeroHashWhenEmpty() {
        final AllBlocksHasherHandler handler = new AllBlocksHasherHandler(
                enabledConfig(100), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        assertThat(handler.computeRootHash()).isEqualTo(AllBlocksHasherHandler.ZERO_BLOCK_HASH);
    }

    @Test
    @DisplayName("genesis: hasher file is created")
    void genesis_hasherFile_isCreated() {
        new AllBlocksHasherHandler(enabledConfig(100), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        assertThat(Files.exists(hasherPath)).isTrue();
    }

    // ─── Append behavior ─────────────────────────────────────────────────────

    @Test
    @DisplayName("append: updates lastBlockHash after genesis")
    void append_updatesLastBlockHash() {
        final AllBlocksHasherHandler handler = new AllBlocksHasherHandler(
                enabledConfig(100), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        final byte[] blockHash = new byte[AllBlocksHasherHandler.BLOCK_HASH_LENGTH];
        blockHash[0] = 0x01;
        handler.appendLatestHashToAllPreviousBlocksStreamingHasher(blockHash, 0L);
        assertThat(handler.lastBlockHash()).isEqualTo(blockHash);
    }

    @Test
    @DisplayName("append: increments leaf count")
    void append_incrementsLeafCount() {
        final AllBlocksHasherHandler handler = new AllBlocksHasherHandler(
                enabledConfig(100), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        handler.appendLatestHashToAllPreviousBlocksStreamingHasher(
                new byte[AllBlocksHasherHandler.BLOCK_HASH_LENGTH], 0L);
        assertThat(handler.getNumberOfBlocks()).isEqualTo(1L);
    }

    @Test
    @DisplayName("append: computeRootHash returns non-null after one leaf")
    void append_computeRootHash_nonNullAfterOneLeaf() {
        final AllBlocksHasherHandler handler = new AllBlocksHasherHandler(
                enabledConfig(100), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        handler.appendLatestHashToAllPreviousBlocksStreamingHasher(
                new byte[AllBlocksHasherHandler.BLOCK_HASH_LENGTH], 0L);
        assertThat(handler.computeRootHash()).isNotNull();
    }

    // ─── start() + persistence ────────────────────────────────────────────────

    @Test
    @DisplayName("start + append: hasher remains available")
    void startAndAppend_hasherRemainsAvailable() {
        final AllBlocksHasherHandler handler = new AllBlocksHasherHandler(
                enabledConfig(100), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        handler.start();
        handler.appendLatestHashToAllPreviousBlocksStreamingHasher(
                new byte[AllBlocksHasherHandler.BLOCK_HASH_LENGTH], 0L);
        assertThat(handler.isAvailable()).isTrue();
    }

    @Test
    @DisplayName("start + append at interval: snapshot persisted to file")
    void startAndAppendAtInterval_snapshotPersisted() throws IOException {
        // interval=1 means persistence fires after the first append post-start
        final AllBlocksHasherHandler handler =
                new AllBlocksHasherHandler(enabledConfig(1), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        handler.start();
        handler.appendLatestHashToAllPreviousBlocksStreamingHasher(
                new byte[AllBlocksHasherHandler.BLOCK_HASH_LENGTH], 0L);
        assertThat(Files.size(hasherPath)).isGreaterThan(0L);
    }

    @Test
    @DisplayName("multiple appends before start: persistence counter not triggered")
    void appendBeforeStart_doesNotTriggerPersistence() throws IOException {
        final AllBlocksHasherHandler handler =
                new AllBlocksHasherHandler(enabledConfig(1), contextWith(new SimpleInMemoryHistoricalBlockFacility()));
        // Record file size written during init
        final long sizeAfterInit = Files.size(hasherPath);
        // append without calling start() → persistenceThresholdCounter stays at -1 → no re-persist
        handler.appendLatestHashToAllPreviousBlocksStreamingHasher(
                new byte[AllBlocksHasherHandler.BLOCK_HASH_LENGTH], 0L);
        // File size should be unchanged (no additional persist occurred)
        assertThat(Files.size(hasherPath)).isEqualTo(sizeAfterInit);
    }

    // ─── validateState mismatch → hasher null ─────────────────────────────────

    @Test
    @DisplayName("non-genesis, no file, no rebuild: validateState mismatch makes hasher unavailable")
    void nonGenesis_noFile_noRebuild_hasherUnavailable() {
        final SimpleInMemoryHistoricalBlockFacility facility = new SimpleInMemoryHistoricalBlockFacility();
        final SimpleBlockRangeSet blockSet = new SimpleBlockRangeSet();
        blockSet.add(0L, 10L);
        facility.setTemporaryAvailableBlocks(blockSet);
        // No hasher file exists, no rebuild → validateState sees leafCount=0 vs max=10 → null
        final AllBlocksHasherHandler handler = new AllBlocksHasherHandler(enabledConfig(100), contextWith(facility));
        assertThat(handler.isAvailable()).isFalse();
    }

    @Test
    @DisplayName("append when unavailable: lastBlockHash() still returns null")
    void appendWhenUnavailable_lastBlockHashStillNull() {
        final SimpleInMemoryHistoricalBlockFacility facility = new SimpleInMemoryHistoricalBlockFacility();
        final SimpleBlockRangeSet blockSet = new SimpleBlockRangeSet();
        blockSet.add(0L, 10L);
        facility.setTemporaryAvailableBlocks(blockSet);
        final AllBlocksHasherHandler handler = new AllBlocksHasherHandler(enabledConfig(100), contextWith(facility));
        handler.appendLatestHashToAllPreviousBlocksStreamingHasher(
                new byte[AllBlocksHasherHandler.BLOCK_HASH_LENGTH], 0L);
        assertThat(handler.lastBlockHash()).isNull();
    }

    @Test
    @DisplayName("BLOCK_HASH_LENGTH is 48 (SHA-384)")
    void blockHashLengthIs48() {
        assertThat(AllBlocksHasherHandler.BLOCK_HASH_LENGTH).isEqualTo(48);
    }

    @Test
    @DisplayName("ZERO_BLOCK_HASH has correct length")
    void zeroBlockHashHasCorrectLength() {
        assertThat(AllBlocksHasherHandler.ZERO_BLOCK_HASH).hasSize(AllBlocksHasherHandler.BLOCK_HASH_LENGTH);
    }
}
