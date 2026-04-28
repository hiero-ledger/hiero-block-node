// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.hiero.block.tools.blocks.validation.BlockExtractionUtils.extractBlockInstant;
import static org.hiero.block.tools.blocks.validation.BlockExtractionUtils.extractRecordFileBytes;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Instant;
import org.hiero.block.internal.BlockUnparsed;
import org.jspecify.annotations.Nullable;

/**
 * Stateless, thread-safe utility that pre-computes expensive per-block data in the
 * decompression pool. This moves three costly operations off the main validation thread:
 * <ol>
 *   <li>{@code hashBlock()} — builds a 16-leaf SHA-384 merkle tree (most expensive per-block op)</li>
 *   <li>{@code extractBlockInstant()} — parses the BlockHeader for the block timestamp</li>
 *   <li>{@code extractRecordFileBytes()} — finds the RecordFile item bytes</li>
 * </ol>
 *
 * <p>Each extraction is independently try/caught so a failure in one does not prevent the others.
 */
public final class ParallelBlockPreprocessor {

    /** Result record holding the three pre-computed values (any may be null on failure). */
    public record PreprocessedData(
            byte @Nullable [] blockHash,
            @Nullable Instant blockInstant,
            @Nullable Bytes recordFileBytes) {}

    private ParallelBlockPreprocessor() {}

    /**
     * Pre-computes block hash, block instant, and record file bytes from the given block.
     * Each extraction is independent and wrapped in try/catch — a failure in one does not
     * affect the others. Callers on the main thread should fall back to on-demand computation
     * when a field is null.
     *
     * @param block the shallow-parsed block
     * @return pre-computed data (fields may be null if extraction failed)
     */
    public static PreprocessedData preprocess(final BlockUnparsed block) {
        byte[] blockHash = null;
        Instant blockInstant = null;
        Bytes recordFileBytes = null;

        try {
            blockHash = hashBlock(block);
        } catch (Exception ignored) {
            // Fall back to main-thread computation
        }

        try {
            blockInstant = extractBlockInstant(block);
        } catch (Exception ignored) {
            // Fall back to main-thread extraction
        }

        try {
            recordFileBytes = extractRecordFileBytes(block);
        } catch (Exception ignored) {
            // Fall back to main-thread extraction
        }

        return new PreprocessedData(blockHash, blockInstant, recordFileBytes);
    }
}
