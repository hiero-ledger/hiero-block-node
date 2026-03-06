// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapDeleteChange;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.token.Account;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link HbarSupplyValidation}. */
class HbarSupplyValidationTest {

    private static final BlockItem HEADER_ITEM = BlockItem.newBuilder()
            .blockHeader(BlockHeader.newBuilder().number(0).build())
            .build();
    private static final BlockItem RECORD_FILE_ITEM = BlockItem.newBuilder()
            .recordFile(RecordFileItem.newBuilder()
                    .creationTime(Timestamp.newBuilder().seconds(1L).build())
                    .recordFileContents(RecordStreamFile.newBuilder().build())
                    .build())
            .build();
    private static final BlockItem FOOTER_ITEM = BlockItem.newBuilder()
            .blockFooter(com.hedera.hapi.block.stream.output.BlockFooter.newBuilder()
                    .previousBlockRootHash(Bytes.wrap(EMPTY_TREE_HASH))
                    .rootHashOfAllBlockHashesTree(Bytes.wrap(EMPTY_TREE_HASH))
                    .startOfBlockStateRootHash(Bytes.wrap(EMPTY_TREE_HASH))
                    .build())
            .build();
    private static final BlockItem PROOF_ITEM =
            BlockItem.newBuilder().blockProof(BlockProof.DEFAULT).build();
    private static final Block VALID_BLOCK = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));

    @Test
    void emptyBlock_zeroSupply_throwsValidationException() {
        // With zero initial state and no transfers, total HBAR = 0, which != 50B.
        // The block with no state changes produces zero delta, so validation should fail.
        HbarSupplyValidation validation = new HbarSupplyValidation();
        // Build a block with only a header + footer + proof (no RecordFile or StateChanges)
        Block blockNoRecordFile = new Block(List.of(HEADER_ITEM, FOOTER_ITEM, PROOF_ITEM));
        try {
            validation.validate(blockNoRecordFile, 0);
            // Should have thrown
            assertTrue(false, "Should have thrown ValidationException for zero supply");
        } catch (ValidationException e) {
            assertTrue(e.getMessage().contains("HBAR supply mismatch"));
        }
    }

    @Test
    void getAccounts_returnsNonNull() {
        HbarSupplyValidation validation = new HbarSupplyValidation();
        assertNotNull(validation.getAccounts());
    }

    @Test
    void requiresGenesisStart() {
        HbarSupplyValidation validation = new HbarSupplyValidation();
        assertTrue(validation.requiresGenesisStart());
    }

    @Test
    void name_returnsExpected() {
        HbarSupplyValidation validation = new HbarSupplyValidation();
        assertTrue(validation.name().contains("HBAR"));
    }

    @Test
    void mergeRecordStreamItems_emptyAmendments_returnsOriginal() {
        // Test that mergeRecordStreamItems returns original list when amendments are empty
        assertDoesNotThrow(() -> {
            List<?> result = HbarSupplyValidation.mergeRecordStreamItems(List.of(), List.of());
            assertTrue(result.isEmpty());
        });
    }

    @Test
    void multipleMapUpdates_sameAccount_correctDelta() throws ValidationException {
        // Genesis block: account 2 = 50B
        HbarSupplyValidation validation = new HbarSupplyValidation();
        Block genesis = buildGenesisBlock(2, HbarSupplyValidation.FIFTY_BILLION_HBAR_IN_TINYBAR);
        validation.validate(genesis, 0);
        validation.commitState(genesis, 0);
        // Second block: two mapUpdate entries for account 2, first to 49B, then to 48B
        // Net delta: 48B - 50B = -2B, so total = 50B - 2B = 48B != 50B → should fail
        long fortyNineB = HbarSupplyValidation.FIFTY_BILLION_HBAR_IN_TINYBAR - 1_000_000_000_000_000_000L;
        long fortyEightB = HbarSupplyValidation.FIFTY_BILLION_HBAR_IN_TINYBAR - 2_000_000_000_000_000_000L;
        BlockItem update1 = buildMapUpdateItem(2, fortyNineB, 2L);
        BlockItem update2 = buildMapUpdateItem(2, fortyEightB, 3L);
        Block block1 =
                new Block(List.of(buildHeaderItem(1), update1, update2, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block1, 1));
        assertTrue(ex.getMessage().contains("HBAR supply mismatch"));
    }

    @Test
    void mapUpdateThenDelete_sameAccount_correctDelta() throws ValidationException {
        // Genesis: account 2 = 40B, account 3 = 10B → total = 50B
        long fortyB = 4_000_000_000_000_000_000L;
        long tenB = 1_000_000_000_000_000_000L;
        HbarSupplyValidation validation = new HbarSupplyValidation();
        Block genesis = buildGenesisBlockTwoAccounts(2, fortyB, 3, tenB);
        validation.validate(genesis, 0);
        validation.commitState(genesis, 0);
        // Second block: mapUpdate account 3 to 5B, then mapDelete account 3
        // With overlay: update gives delta = 5B - 10B = -5B, overlay now has acct3=5B
        // Delete gives delta = -5B (from overlay), total delta = -10B
        // Total = 50B - 10B = 40B != 50B → should fail
        long fiveB = 500_000_000_000_000_000L;
        BlockItem update = buildMapUpdateItem(3, fiveB, 2L);
        BlockItem delete = buildMapDeleteItem(3, 3L);
        Block block1 =
                new Block(List.of(buildHeaderItem(1), update, delete, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block1, 1));
        assertTrue(ex.getMessage().contains("HBAR supply mismatch"));
    }

    @Test
    void saveLoadRoundTrip_preservesState(@TempDir Path tempDir) throws Exception {
        // Validate and commit genesis
        HbarSupplyValidation validation = new HbarSupplyValidation();
        Block genesis = buildGenesisBlock(2, HbarSupplyValidation.FIFTY_BILLION_HBAR_IN_TINYBAR);
        validation.validate(genesis, 0);
        validation.commitState(genesis, 0);
        // Save state
        validation.save(tempDir);
        // Create new instance and load
        HbarSupplyValidation restored = new HbarSupplyValidation();
        restored.load(tempDir);
        // Validate next block with same content — no balance changes, should still be 50B
        // Use the genesis block again (it has the 50B state change) — but as block 1 the
        // state change would try to set account 2 = 50B again, which is same as committed
        // → delta = 0, total still 50B → should pass
        Block block1 = new Block(List.of(buildHeaderItem(1), RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
        assertDoesNotThrow(() -> restored.validate(block1, 1));
    }

    // ── Helper methods for building test blocks ──

    private static BlockItem buildHeaderItem(long blockNumber) {
        return BlockItem.newBuilder()
                .blockHeader(BlockHeader.newBuilder().number(blockNumber).build())
                .build();
    }

    private static BlockItem buildMapUpdateItem(long accountNum, long balance, long timestampSeconds) {
        AccountID accountId = AccountID.newBuilder()
                .shardNum(0)
                .realmNum(0)
                .accountNum(accountNum)
                .build();
        Account account = Account.newBuilder()
                .accountId(accountId)
                .tinybarBalance(balance)
                .build();
        StateChange sc = StateChange.newBuilder()
                .stateId(1)
                .mapUpdate(MapUpdateChange.newBuilder()
                        .key(MapChangeKey.newBuilder().accountIdKey(accountId))
                        .value(MapChangeValue.newBuilder().accountValue(account)))
                .build();
        return BlockItem.newBuilder()
                .stateChanges(StateChanges.newBuilder()
                        .consensusTimestamp(
                                Timestamp.newBuilder().seconds(timestampSeconds).build())
                        .stateChanges(List.of(sc))
                        .build())
                .build();
    }

    private static BlockItem buildMapDeleteItem(long accountNum, long timestampSeconds) {
        AccountID accountId = AccountID.newBuilder()
                .shardNum(0)
                .realmNum(0)
                .accountNum(accountNum)
                .build();
        StateChange sc = StateChange.newBuilder()
                .stateId(1)
                .mapDelete(MapDeleteChange.newBuilder()
                        .key(MapChangeKey.newBuilder().accountIdKey(accountId)))
                .build();
        return BlockItem.newBuilder()
                .stateChanges(StateChanges.newBuilder()
                        .consensusTimestamp(
                                Timestamp.newBuilder().seconds(timestampSeconds).build())
                        .stateChanges(List.of(sc))
                        .build())
                .build();
    }

    private static Block buildGenesisBlock(long accountNum, long balance) {
        BlockItem genesisStateChange = buildMapUpdateItem(accountNum, balance, 1L);
        return new Block(List.of(HEADER_ITEM, genesisStateChange, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
    }

    private static Block buildGenesisBlockTwoAccounts(long acct1, long bal1, long acct2, long bal2) {
        AccountID id1 =
                AccountID.newBuilder().shardNum(0).realmNum(0).accountNum(acct1).build();
        AccountID id2 =
                AccountID.newBuilder().shardNum(0).realmNum(0).accountNum(acct2).build();
        Account a1 = Account.newBuilder().accountId(id1).tinybarBalance(bal1).build();
        Account a2 = Account.newBuilder().accountId(id2).tinybarBalance(bal2).build();
        StateChange sc1 = StateChange.newBuilder()
                .stateId(1)
                .mapUpdate(MapUpdateChange.newBuilder()
                        .key(MapChangeKey.newBuilder().accountIdKey(id1))
                        .value(MapChangeValue.newBuilder().accountValue(a1)))
                .build();
        StateChange sc2 = StateChange.newBuilder()
                .stateId(1)
                .mapUpdate(MapUpdateChange.newBuilder()
                        .key(MapChangeKey.newBuilder().accountIdKey(id2))
                        .value(MapChangeValue.newBuilder().accountValue(a2)))
                .build();
        BlockItem stateChanges = BlockItem.newBuilder()
                .stateChanges(StateChanges.newBuilder()
                        .consensusTimestamp(Timestamp.newBuilder().seconds(1L).build())
                        .stateChanges(List.of(sc1, sc2))
                        .build())
                .build();
        return new Block(List.of(HEADER_ITEM, stateChanges, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
    }
}
