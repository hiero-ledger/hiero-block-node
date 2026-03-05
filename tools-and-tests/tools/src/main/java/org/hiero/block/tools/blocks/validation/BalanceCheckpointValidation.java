// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import com.hedera.hapi.block.stream.Block;
import org.hiero.block.tools.blocks.wrapped.BalanceCheckpointValidator;
import org.hiero.block.tools.blocks.wrapped.RunningAccountsState;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Validates account balances against pre-fetched checkpoint snapshots at periodic block numbers.
 *
 * <p>This validation wraps the existing {@link BalanceCheckpointValidator} and checks the
 * committed account state (i.e. state <em>before</em> this block) against the checkpoint
 * at each designated block number.
 *
 * <p>The checkpoint validator and its configuration (interval, sources) must be set up
 * externally before passing to this class.
 */
public final class BalanceCheckpointValidation implements BlockValidation {

    /** The accounts state to validate against checkpoints (shared with HbarSupplyValidation). */
    private final RunningAccountsState accounts;

    /** The underlying checkpoint validator with loaded checkpoint data. */
    private final BalanceCheckpointValidator checkpointValidator;

    /**
     * Creates a new balance checkpoint validation.
     *
     * @param accounts the running accounts state (shared with {@link HbarSupplyValidation})
     * @param checkpointValidator the configured checkpoint validator with loaded checkpoints
     */
    public BalanceCheckpointValidation(
            final RunningAccountsState accounts, final BalanceCheckpointValidator checkpointValidator) {
        this.accounts = accounts;
        this.checkpointValidator = checkpointValidator;
    }

    @Override
    public String name() {
        return "Balance Checkpoint";
    }

    @Override
    public String description() {
        return "Validates account balances against pre-fetched checkpoint snapshots";
    }

    @Override
    public boolean requiresGenesisStart() {
        return true;
    }

    @Override
    public void validate(final Block block, final long blockNumber) throws ValidationException {
        // Check committed state (before this block) against checkpoints
        checkpointValidator.checkBlock(blockNumber, accounts.getHbarBalances(), accounts.getTokenBalances());
    }

    /**
     * Returns the underlying checkpoint validator for summary printing and status checks.
     *
     * @return the balance checkpoint validator
     */
    public BalanceCheckpointValidator getCheckpointValidator() {
        return checkpointValidator;
    }
}
