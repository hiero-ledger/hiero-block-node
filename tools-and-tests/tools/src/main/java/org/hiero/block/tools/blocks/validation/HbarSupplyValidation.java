// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.HasherStateFiles;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.AccountTransfer;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.ExtractedTransfers;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.NftTransferInfo;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.TokenTransferData;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.TransferData;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.TransferVisitor;
import org.hiero.block.tools.blocks.wrapped.RunningAccountsState;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Validates that the total HBAR supply equals exactly 50 billion HBAR (in tinybar) after
 * each block.
 *
 * <p>Account balances are updated from two sources within each block:
 * <ol>
 *   <li>{@code StateChanges} items set absolute HBAR balances or delete accounts.
 *   <li>{@code RecordFile} items apply relative balance changes via transfer lists.
 * </ol>
 *
 * <p>The validate/commit protocol works as follows:
 * <ul>
 *   <li>{@link #validate} parses StateChanges and extracts transfer lists once, computes the
 *       HBAR balance delta, and caches the results for reuse in {@link #commitState}.
 *   <li>{@link #commitState} applies the cached mutations to the base {@link RunningAccountsState}
 *       without re-parsing the block.
 * </ul>
 *
 * <p>RecordFile items are parsed using {@link TransferListExtractor} which navigates the
 * protobuf wire format directly, skipping the Transaction field and unused TransactionRecord
 * fields entirely. This avoids the vast majority of object allocation that a full parse would
 * incur.
 *
 * <p>This validation requires starting from block 0 because the full account state history
 * is needed.
 */
public final class HbarSupplyValidation implements BlockValidation {

    /** Total HBAR supply expressed in tinybar (50 billion HBAR * 100 million tinybar per HBAR). */
    public static final long FIFTY_BILLION_HBAR_IN_TINYBAR = 5_000_000_000_000_000_000L;

    /** Maximum parse size for protobuf messages (100 MB) to handle large StateChanges items. */
    private static final int MAX_PARSE_SIZE = 100 * 1024 * 1024;

    /** The running account state. */
    private final RunningAccountsState accounts = new RunningAccountsState();

    /** Parsed StateChanges cached between validate() and commitState() to avoid double-parsing. */
    private List<StateChanges> stagedStateChanges;

    /** Extracted transfer data cached between validate() and commitState() — only populated when amendments present. */
    private List<List<TransferData>> stagedMergedTransfers;

    /** Raw RecordFile bytes cached for commitState() re-extraction via visitor (zero-allocation path). */
    private List<Bytes> stagedRecordFileBytes;

    /** Reusable in-block balance overlay map, cleared between blocks to avoid per-block allocation. */
    private final LongLongHashMap inBlockBalances = new LongLongHashMap();

    @Override
    public String name() {
        return "HBAR Supply";
    }

    @Override
    public String description() {
        return "Verifies total HBAR supply equals exactly 50 billion after each block";
    }

    @Override
    public boolean requiresGenesisStart() {
        return true;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        // Parse StateChanges and extract transfer data, cache for commitState().
        final List<StateChanges> parsedStateChanges = new ArrayList<>();
        // Zero-allocation visitor path: accumulate hbarDelta directly, cache raw bytes for commitState().
        // Falls back to list-based path only when amendments are present (rare).
        final long[] hbarDeltaHolder = {0};
        final List<Bytes> recordFileBytesList = new ArrayList<>();
        List<List<TransferData>> mergedTransfersList = null; // only allocated if amendments present

        final TransferVisitor deltaVisitor = new TransferVisitor() {
            @Override
            public void onHbarTransfer(long accountNum, long amount) {
                hbarDeltaHolder[0] += amount;
            }

            @Override
            public void onFungibleTokenTransfer(long accountNum, long tokenNum, long amount) {
                // Not needed for HBAR supply check
            }

            @Override
            public void onNftTransfer(long senderNum, long receiverNum, long tokenNum, long serial) {
                // Not needed for HBAR supply check
            }
        };

        try {
            for (final BlockItemUnparsed item : block.blockItems()) {
                if (item.hasStateChanges()) {
                    parsedStateChanges.add(StateChanges.PROTOBUF.parse(
                            item.stateChangesOrThrow().toReadableSequentialData(),
                            false,
                            false,
                            Codec.DEFAULT_MAX_DEPTH,
                            MAX_PARSE_SIZE));
                } else if (item.hasRecordFile()) {
                    final Bytes recordFileBytes = item.recordFileOrThrow();
                    recordFileBytesList.add(recordFileBytes);
                    // Try zero-allocation visitor path first
                    if (!TransferListExtractor.extractInto(recordFileBytes, deltaVisitor)) {
                        // Amendments present — fall back to list-based extraction
                        if (mergedTransfersList == null) mergedTransfersList = new ArrayList<>();
                        final ExtractedTransfers extracted = TransferListExtractor.extract(recordFileBytes);
                        final List<TransferData> merged =
                                TransferListExtractor.mergeTransferData(extracted.items(), extracted.amendments());
                        mergedTransfersList.add(merged);
                        // Accumulate hbar delta from the list-based path
                        for (final TransferData td : merged) {
                            for (final AccountTransfer at : td.hbarTransfers()) {
                                hbarDeltaHolder[0] += at.amount();
                            }
                        }
                    }
                }
            }
        } catch (ParseException e) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Failed to parse block items: " + e.getMessage());
        }

        // Compute the net HBAR delta from StateChanges without modifying the base state.
        long hbarDelta = hbarDeltaHolder[0];
        inBlockBalances.clear();

        for (final StateChanges stateChanges : parsedStateChanges) {
            for (final StateChange stateChange : stateChanges.stateChanges()) {
                if (stateChange.hasMapUpdate()) {
                    final MapUpdateChange mapUpdate = stateChange.mapUpdateOrThrow();
                    final MapChangeKey key = mapUpdate.keyOrThrow();
                    final MapChangeValue value = mapUpdate.valueOrThrow();
                    if (key.hasAccountIdKey() && value.hasAccountValue()) {
                        final long accountNum = key.accountIdKeyOrThrow().accountNumOrThrow();
                        final long newBalance = value.accountValueOrThrow().tinybarBalance();
                        final long oldBalance = inBlockBalances.containsKey(accountNum)
                                ? inBlockBalances.get(accountNum)
                                : accounts.getHbarBalance(accountNum);
                        hbarDelta += (newBalance - oldBalance);
                        inBlockBalances.put(accountNum, newBalance);
                    }
                } else if (stateChange.hasMapDelete()) {
                    final MapChangeKey key = stateChange.mapDeleteOrThrow().keyOrThrow();
                    if (key.hasAccountIdKey()) {
                        final long accountNum = key.accountIdKeyOrThrow().accountNumOrThrow();
                        final long oldBalance = inBlockBalances.containsKey(accountNum)
                                ? inBlockBalances.get(accountNum)
                                : accounts.getHbarBalance(accountNum);
                        hbarDelta -= oldBalance;
                        inBlockBalances.remove(accountNum);
                    }
                }
            }
        }

        // Verify total HBAR supply using base total + delta
        final long totalBalance = accounts.totalHbarBalance() + hbarDelta;
        if (totalBalance != FIFTY_BILLION_HBAR_IN_TINYBAR) {
            final long difference = totalBalance - FIFTY_BILLION_HBAR_IN_TINYBAR;
            throw new ValidationException("Block: " + blockNumber + " - Total HBAR supply mismatch. Expected "
                    + FIFTY_BILLION_HBAR_IN_TINYBAR + " tinybar but found " + totalBalance + " tinybar (difference: "
                    + difference + ")");
        }

        // Stage for commitState()
        stagedStateChanges = parsedStateChanges;
        stagedRecordFileBytes = recordFileBytesList;
        stagedMergedTransfers = mergedTransfersList;
    }

    @Override
    public void commitState(final BlockUnparsed block, final long blockNumber) {
        // Apply StateChanges to the base state
        applyStateChangesToAccounts(stagedStateChanges, accounts);

        // Apply transfer data via visitor re-extraction (zero-allocation for no-amendment case)
        final TransferVisitor commitVisitor = new TransferVisitor() {
            @Override
            public void onHbarTransfer(long accountNum, long amount) {
                accounts.applyHbarChange(accountNum, amount);
            }

            @Override
            public void onFungibleTokenTransfer(long accountNum, long tokenNum, long amount) {
                accounts.applyFungibleTokenChange(accountNum, tokenNum, amount);
            }

            @Override
            public void onNftTransfer(long senderNum, long receiverNum, long tokenNum, long serial) {
                accounts.applyNftTransfer(senderNum, receiverNum, tokenNum, serial);
            }
        };

        try {
            for (final Bytes recordFileBytes : stagedRecordFileBytes) {
                // Returns false if amendments present — those are handled via stagedMergedTransfers below
                TransferListExtractor.extractInto(recordFileBytes, commitVisitor);
            }
        } catch (ParseException e) {
            throw new RuntimeException("Failed to re-extract transfers in commitState", e);
        }

        // Apply any amendment-path transfers that were staged
        if (stagedMergedTransfers != null) {
            applyMergedTransfersToAccounts(stagedMergedTransfers, accounts);
        }

        stagedStateChanges = null;
        stagedMergedTransfers = null;
        stagedRecordFileBytes = null;
    }

    /**
     * Returns the running account state. This can be used by other validations
     * (e.g. {@link BalanceCheckpointValidation}) that need access to the committed balances.
     *
     * @return the running accounts state
     */
    public RunningAccountsState getAccounts() {
        return accounts;
    }

    /**
     * Applies StateChanges to the running account state.
     */
    private static void applyStateChangesToAccounts(
            final List<StateChanges> stateChangesList, final RunningAccountsState accounts) {
        for (final StateChanges stateChanges : stateChangesList) {
            for (final StateChange stateChange : stateChanges.stateChanges()) {
                if (stateChange.hasMapUpdate()) {
                    final MapUpdateChange mapUpdate = stateChange.mapUpdateOrThrow();
                    final MapChangeKey key = mapUpdate.keyOrThrow();
                    final MapChangeValue value = mapUpdate.valueOrThrow();
                    if (key.hasAccountIdKey() && value.hasAccountValue()) {
                        accounts.setHbarBalance(
                                key.accountIdKeyOrThrow().accountNumOrThrow(),
                                value.accountValueOrThrow().tinybarBalance());
                    }
                } else if (stateChange.hasMapDelete()) {
                    final MapChangeKey key = stateChange.mapDeleteOrThrow().keyOrThrow();
                    if (key.hasAccountIdKey()) {
                        accounts.deleteAccount(key.accountIdKeyOrThrow().accountNumOrThrow());
                    }
                }
            }
        }
    }

    /**
     * Applies pre-merged transfer data (amendment fallback path) to the running account state.
     */
    private static void applyMergedTransfersToAccounts(
            final List<List<TransferData>> mergedTransfersList, final RunningAccountsState accounts) {
        for (final List<TransferData> mergedItems : mergedTransfersList) {
            for (final TransferData td : mergedItems) {
                for (final AccountTransfer at : td.hbarTransfers()) {
                    accounts.applyHbarChange(at.accountNum(), at.amount());
                }
                for (final TokenTransferData ttd : td.tokenTransfers()) {
                    for (final AccountTransfer ft : ttd.transfers()) {
                        accounts.applyFungibleTokenChange(ft.accountNum(), ttd.tokenNum(), ft.amount());
                    }
                    for (final NftTransferInfo nft : ttd.nftTransfers()) {
                        accounts.applyNftTransfer(
                                nft.senderAccountNum(), nft.receiverAccountNum(), ttd.tokenNum(), nft.serialNumber());
                    }
                }
            }
        }
    }

    private static final String SAVE_FILE_NAME = "hbarSupplyValidation.bin";

    @Override
    public void save(final Path directory) throws IOException {
        try {
            HasherStateFiles.saveAtomically(directory.resolve(SAVE_FILE_NAME), accounts::save);
        } catch (Exception e) {
            throw new IOException("Failed to save HBAR supply state", e);
        }
    }

    @Override
    public void load(final Path directory) throws IOException {
        Path file = directory.resolve(SAVE_FILE_NAME);
        HasherStateFiles.loadWithFallback(file, accounts::load);
    }
}
