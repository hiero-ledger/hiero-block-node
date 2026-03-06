// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.tools.blocks.wrapped.BalanceCheckpointValidator;
import org.hiero.block.tools.blocks.wrapped.RunningAccountsState;
import org.junit.jupiter.api.Test;

/** Tests for {@link BalanceCheckpointValidation}. */
class BalanceCheckpointValidationTest {

    private static final BlockItem HEADER_ITEM = BlockItem.newBuilder()
            .blockHeader(BlockHeader.newBuilder().number(0).build())
            .build();
    private static final BlockItem RECORD_FILE_ITEM =
            BlockItem.newBuilder().recordFile(RecordFileItem.DEFAULT).build();
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
    void nameAndDescription_returnExpected() {
        RunningAccountsState accounts = new RunningAccountsState();
        BalanceCheckpointValidator cpValidator = new BalanceCheckpointValidator();
        BalanceCheckpointValidation validation = new BalanceCheckpointValidation(accounts, cpValidator);
        assertEquals("Balance Checkpoint", validation.name());
        assertFalse(validation.description().isEmpty());
        assertTrue(validation.requiresGenesisStart());
    }

    @Test
    void validate_delegatesToCheckpointValidator() {
        // With no checkpoints loaded, checkBlock is a no-op — should not throw
        RunningAccountsState accounts = new RunningAccountsState();
        BalanceCheckpointValidator cpValidator = new BalanceCheckpointValidator();
        BalanceCheckpointValidation validation = new BalanceCheckpointValidation(accounts, cpValidator);
        assertDoesNotThrow(() -> validation.validate(VALID_BLOCK, 0));
    }

    @Test
    void getCheckpointValidator_returnsSameInstance() {
        RunningAccountsState accounts = new RunningAccountsState();
        BalanceCheckpointValidator cpValidator = new BalanceCheckpointValidator();
        BalanceCheckpointValidation validation = new BalanceCheckpointValidation(accounts, cpValidator);
        assertSame(cpValidator, validation.getCheckpointValidator());
    }
}
