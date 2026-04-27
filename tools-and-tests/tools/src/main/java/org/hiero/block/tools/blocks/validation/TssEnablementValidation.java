// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.days.model.TssEnablementRegistry;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import picocli.CommandLine.Help.Ansi;

/**
 * Sequential validation that discovers {@code LedgerIdPublication} transactions from
 * RecordFile items in each block and keeps the {@link TssEnablementRegistry} up to date.
 *
 * <p>When a publication is found, the raw protobuf binary is immediately written to
 * the configured output path so the block node's {@code VerificationServicePlugin}
 * can consume it directly.
 */
public final class TssEnablementValidation implements BlockValidation {

    private static final String SAVE_FILE_NAME = "tssPublicationHistory.json";
    private static final int MAX_DEPTH = 512;
    private static final int MAX_RECORD_FILE_SIZE = 128 * 1024 * 1024;

    private final TssEnablementRegistry tssRegistry;
    private final Path tssParametersBinPath;
    private boolean firstPublicationSeen = false;

    /**
     * Creates a new TSS enablement validation.
     *
     * @param tssRegistry the shared registry to update when publications are found
     * @param tssParametersBinPath the output path for the raw protobuf binary file
     */
    public TssEnablementValidation(final TssEnablementRegistry tssRegistry, final Path tssParametersBinPath) {
        this.tssRegistry = tssRegistry;
        this.tssParametersBinPath = tssParametersBinPath;
    }

    @Override
    public String name() {
        return "TssEnablement";
    }

    @Override
    public String description() {
        return "Discovers LedgerIdPublication transactions from block data and writes tss-enablement.bin";
    }

    @Override
    public boolean requiresGenesisStart() {
        return false;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        try {
            final Instant blockInstant = extractBlockInstant(block);
            final Bytes recordFileBytes = extractRecordFileBytes(block);
            if (recordFileBytes == null || recordFileBytes.length() == 0 || blockInstant == null) {
                return;
            }
            processLedgerIdPublications(recordFileBytes, blockInstant, blockNumber);
        } catch (Exception e) {
            System.err.println("[TssEnablement] Block " + blockNumber + " - Error extracting TSS data: "
                    + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @SuppressWarnings("DataFlowIssue") // OrThrow() calls are guarded by has*() checks above
    private void processLedgerIdPublications(
            final Bytes recordFileBytes, final Instant blockInstant, final long blockNumber)
            throws ParseException, IOException {
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
            if (body.hasLedgerIdPublication()) {
                if (!firstPublicationSeen) {
                    firstPublicationSeen = true;
                    System.out.println(Ansi.AUTO.string(
                            "@|bold,green TSS ENABLED: First LedgerIdPublication transaction found at block "
                                    + blockNumber + "|@"));
                }
                final String description =
                        tssRegistry.recordPublication(blockInstant, blockNumber, body.ledgerIdPublicationOrThrow());
                System.out.println(
                        Ansi.AUTO.string("@|yellow TSS publication at block " + blockNumber + ":|@ " + description));
                // Write tss-enablement.bin immediately on each detection
                try {
                    tssRegistry.writeTssParametersBin(tssParametersBinPath);
                } catch (Exception e) {
                    System.err.println(
                            "[TssEnablement] WARNING: Failed to write " + tssParametersBinPath + ": " + e.getMessage());
                }
            }
        }
    }

    private static TransactionBody extractTransactionBody(final Transaction t) throws ParseException {
        if (t.hasBody()) {
            return t.body();
        } else if (t.bodyBytes().length() > 0) {
            return TransactionBody.PROTOBUF.parse(t.bodyBytes());
        } else if (t.signedTransactionBytes().length() > 0) {
            final SignedTransaction st = SignedTransaction.PROTOBUF.parse(t.signedTransactionBytes());
            return TransactionBody.PROTOBUF.parse(st.bodyBytes());
        }
        return null;
    }

    private static Instant extractBlockInstant(final BlockUnparsed block) throws ParseException {
        for (final BlockItemUnparsed item : block.blockItems()) {
            if (item.hasBlockHeader()) {
                final BlockHeader header = BlockHeader.PROTOBUF.parse(item.blockHeaderOrThrow());
                final Timestamp ts = header.blockTimestampOrThrow();
                return Instant.ofEpochSecond(ts.seconds(), ts.nanos());
            }
        }
        return null;
    }

    private static Bytes extractRecordFileBytes(final BlockUnparsed block) {
        for (final BlockItemUnparsed item : block.blockItems()) {
            if (item.hasRecordFile()) {
                return item.recordFileOrThrow();
            }
        }
        return null;
    }

    @Override
    public void save(final Path directory) throws IOException {
        tssRegistry.saveToJsonFile(directory.resolve(SAVE_FILE_NAME));
    }

    @Override
    public void load(final Path directory) throws IOException {
        final Path saved = directory.resolve(SAVE_FILE_NAME);
        if (saved.toFile().exists()) {
            tssRegistry.reloadFromFile(saved);
            // Regenerate the bin file from loaded state
            if (tssRegistry.hasTssData()) {
                try {
                    tssRegistry.writeTssParametersBin(tssParametersBinPath);
                } catch (Exception e) {
                    throw new IOException("Failed to write " + tssParametersBinPath, e);
                }
                firstPublicationSeen = true;
            }
        }
    }

    @Override
    public void finalize(final long totalBlocksValidated, final long lastBlockNumber) throws ValidationException {
        if (tssRegistry.hasTssData()) {
            System.out.println(Ansi.AUTO.string(
                    "@|green [TssEnablement] " + tssRegistry.getPublicationCount() + " TSS publication(s) found|@"));
        } else {
            System.out.println("[TssEnablement] No TSS publications found");
        }
    }
}
