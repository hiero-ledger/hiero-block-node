// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.validation.BlockExtractionUtils.extractBlockInstant;
import static org.hiero.block.tools.blocks.validation.BlockExtractionUtils.extractRecordFileBytes;
import static org.hiero.block.tools.blocks.validation.BlockExtractionUtils.extractTransactionBody;
import static org.hiero.block.tools.blocks.validation.ProtobufParsingConstants.MAX_DEPTH;
import static org.hiero.block.tools.blocks.validation.ProtobufParsingConstants.MAX_RECORD_FILE_SIZE;

import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.validation.ParallelBlockPreprocessor.PreprocessedData;
import org.hiero.block.tools.days.model.NodeStakeRegistry;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.jspecify.annotations.Nullable;
import picocli.CommandLine.Help.Ansi;

/**
 * Sequential validation that discovers {@code NodeStakeUpdate} transactions from
 * RecordFile items in each block and keeps the {@link NodeStakeRegistry} up to date.
 *
 * <p>This allows stake-weighted signature validation to use the correct stake weights
 * as they are discovered from the block stream. {@code NodeStakeUpdate} transactions
 * are issued daily at midnight UTC.
 */
public final class NodeStakeUpdateValidation implements BlockValidation {

    private static final String SAVE_FILE_NAME = "nodeStakeHistory.json";

    private final NodeStakeRegistry nodeStakeRegistry;
    private boolean firstStakeUpdateSeen = false;

    /**
     * Creates a new node stake update validation.
     *
     * @param nodeStakeRegistry the shared registry to update when stake changes are found
     */
    public NodeStakeUpdateValidation(final NodeStakeRegistry nodeStakeRegistry) {
        this.nodeStakeRegistry = nodeStakeRegistry;
    }

    @Override
    public String name() {
        return "NodeStakeUpdate";
    }

    @Override
    public String description() {
        return "Discovers NodeStakeUpdate transactions from block data and keeps the stake registry current";
    }

    @Override
    public boolean requiresGenesisStart() {
        return false;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        validate(block, blockNumber, (Instant) null, null);
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber, final PreprocessedData preprocessed)
            throws ValidationException {
        validate(
                block,
                blockNumber,
                preprocessed != null ? preprocessed.blockInstant() : null,
                preprocessed != null ? preprocessed.recordFileBytes() : null);
    }

    /**
     * Validates with optional pre-extracted block instant and record file bytes.
     *
     * @param block the shallow-parsed block
     * @param blockNumber the block number
     * @param preExtractedInstant pre-extracted block timestamp, or null to extract here
     * @param preExtractedRecordFileBytes pre-extracted RecordFile bytes, or null to extract here
     * @throws ValidationException if validation fails
     */
    public void validate(
            final BlockUnparsed block,
            final long blockNumber,
            final @Nullable Instant preExtractedInstant,
            final @Nullable Bytes preExtractedRecordFileBytes)
            throws ValidationException {
        try {
            final Instant blockInstant =
                    (preExtractedInstant != null) ? preExtractedInstant : extractBlockInstant(block);
            final Bytes recordFileBytes =
                    (preExtractedRecordFileBytes != null) ? preExtractedRecordFileBytes : extractRecordFileBytes(block);
            if (recordFileBytes == null || recordFileBytes.length() == 0 || blockInstant == null) {
                return;
            }
            processNodeStakeUpdates(recordFileBytes, blockInstant, blockNumber);
        } catch (Exception e) {
            // Don't fail validation for stake parse/extraction errors — the block's
            // record file might be in a format we don't fully handle (e.g., genesis or very early blocks).
            System.err.println("[NodeStakeUpdate] Block " + blockNumber + " - Error extracting stake data: "
                    + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @SuppressWarnings("DataFlowIssue")
    private void processNodeStakeUpdates(
            final Bytes recordFileBytes, final Instant blockInstant, final long blockNumber) throws Exception {
        final RecordFileItem recordFileItem = RecordFileItem.PROTOBUF.parse(
                recordFileBytes.toReadableSequentialData(), false, false, MAX_DEPTH, MAX_RECORD_FILE_SIZE);
        if (!recordFileItem.hasRecordFileContents()) {
            return;
        }
        final RecordStreamFile recordStreamFile = recordFileItem.recordFileContentsOrThrow();
        final List<? extends RecordStreamItem> items = recordStreamFile.recordStreamItems();
        for (final RecordStreamItem rsi : items) {
            if (!rsi.hasTransaction()) {
                continue;
            }
            final Transaction t = rsi.transactionOrThrow();
            final TransactionBody body = extractTransactionBody(t);
            if (body == null) {
                continue;
            }
            if (body.hasNodeStakeUpdate()) {
                if (!firstStakeUpdateSeen) {
                    firstStakeUpdateSeen = true;
                    System.out.println(Ansi.AUTO.string(
                            "@|green First NodeStakeUpdate transaction found at block " + blockNumber + "|@"));
                }
                // Store with timestamp +1ns so the new stakes apply to blocks AFTER
                // this one, matching the AddressBookUpdateValidation pattern.
                final String changes =
                        nodeStakeRegistry.updateStakes(blockInstant.plusNanos(1), body.nodeStakeUpdateOrThrow());
                if (changes != null) {
                    System.out.println(
                            Ansi.AUTO.string("@|yellow Node stake updated at block " + blockNumber + ":|@ " + changes));
                }
            }
        }
    }

    @Override
    public void save(final Path directory) throws IOException {
        nodeStakeRegistry.saveToJsonFile(directory.resolve(SAVE_FILE_NAME));
    }

    @Override
    public void load(final Path directory) throws IOException {
        final Path saved = directory.resolve(SAVE_FILE_NAME);
        if (saved.toFile().exists()) {
            nodeStakeRegistry.reloadFromFile(saved);
        }
    }
}
