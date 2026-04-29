// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import java.io.IOException;
import java.nio.file.Path;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.validation.ParallelBlockPreprocessor.PreprocessedData;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * A single validation rule that can be applied to a sequence of wrapped blocks.
 *
 * <p>Each implementation encapsulates one validation concern (e.g. chain hash continuity,
 * required items, 50-billion HBAR supply) and owns any cross-block state it needs.
 *
 * <p>The lifecycle has three phases:
 * <ol>
 *   <li><b>Per-block validate/commit</b> — the two-phase protocol ensures that if any
 *       validation fails for a block, no validation's state is updated:
 *       <ul>
 *         <li>{@link #validate(Block, long)} — checks the block against staged (uncommitted)
 *             state. Must NOT commit state changes. If multiple validations are run, a failure
 *             in one causes all staged changes to be discarded.
 *         <li>{@link #commitState(Block, long)} — called only after ALL validations pass,
 *             commits any staged state (e.g. updating the previous block hash, adding to a
 *             streaming hasher).
 *       </ul>
 *   <li><b>Finalize</b> — {@link #finalize(long, long)} is called once after all blocks
 *       have been processed. Use for end-of-stream checks such as comparing accumulated
 *       state against saved state files.
 *   <li><b>Cleanup</b> — {@link #close()} releases any resources held by this validation.
 * </ol>
 */
public interface BlockValidation {

    /**
     * Human-readable name for this validation (e.g. "Block Chain", "Required Items").
     *
     * @return a short name
     */
    String name();

    /**
     * Description of what this validation checks.
     *
     * @return a description string
     */
    String description();

    /**
     * Whether this validation can only run when starting from block 0.
     *
     * <p>Validations that require genesis start (e.g. historical block tree, HBAR supply)
     * need the full block history to compute correct state.
     *
     * @return true if this validation requires starting from block 0
     */
    boolean requiresGenesisStart();

    /**
     * Called once before the validation loop begins.
     *
     * @param firstBlockNumber the first block number that will be validated
     */
    default void init(long firstBlockNumber) {}

    /**
     * Validate a single block. Must NOT commit state changes — use an overlay or stage
     * changes internally so they can be discarded on failure.
     *
     * @param block the block to validate
     * @param blockNumber the block number
     * @throws ValidationException if the block fails this validation
     */
    void validate(BlockUnparsed block, long blockNumber) throws ValidationException;

    /**
     * Validate a single block with optional pre-computed data from parallel preprocessing.
     * The default implementation ignores the preprocessed data and delegates to
     * {@link #validate(BlockUnparsed, long)}. Implementations that can benefit from
     * pre-computed block hashes, block instants, or record file bytes should override.
     *
     * @param block the block to validate
     * @param blockNumber the block number
     * @param preprocessed pre-computed data from the parallel pool, or null
     * @throws ValidationException if the block fails this validation
     */
    default void validate(BlockUnparsed block, long blockNumber, PreprocessedData preprocessed)
            throws ValidationException {
        validate(block, blockNumber);
    }

    /**
     * Commit any staged state changes after ALL validations pass for a block. Called only
     * when every validation's {@link #validate} succeeded for this block.
     *
     * @param block the block that passed validation
     * @param blockNumber the block number
     */
    default void commitState(BlockUnparsed block, long blockNumber) {}

    /**
     * Save persistent state for crash recovery / resume.
     *
     * @param directory the directory to save state into
     * @throws IOException if saving fails
     */
    default void save(Path directory) throws IOException {}

    /**
     * Load persistent state from a previous run.
     *
     * @param directory the directory to load state from
     * @throws IOException if loading fails
     */
    default void load(Path directory) throws IOException {}

    /**
     * Called once after all blocks have been validated and committed. Use for end-of-stream
     * checks such as comparing accumulated state against saved state files.
     *
     * @param totalBlocksValidated the total number of blocks that were validated
     * @param lastBlockNumber the block number of the last validated block
     * @throws ValidationException if the finalization check fails
     */
    default void finalize(long totalBlocksValidated, long lastBlockNumber) throws ValidationException {}

    /** Release any resources held by this validation. */
    default void close() {}
}
