// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.AccountTransfer;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.ExtractedTransfers;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.NftTransferInfo;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.TokenTransferData;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.TransferData;
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

    /** The running account state. */
    private final RunningAccountsState accounts = new RunningAccountsState();

    /** Parsed StateChanges cached between validate() and commitState() to avoid double-parsing. */
    private List<StateChanges> stagedStateChanges;

    /** Extracted transfer data cached between validate() and commitState() to avoid double-parsing. */
    private List<List<TransferData>> stagedMergedTransfers;

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
        // Parse StateChanges and extract transfer lists once, cache for commitState().
        final List<StateChanges> parsedStateChanges = new ArrayList<>();
        final List<List<TransferData>> mergedTransfersList = new ArrayList<>();

        try {
            for (final BlockItemUnparsed item : block.blockItems()) {
                if (item.hasStateChanges()) {
                    parsedStateChanges.add(StateChanges.PROTOBUF.parse(item.stateChangesOrThrow()));
                } else if (item.hasRecordFile()) {
                    final ExtractedTransfers extracted = TransferListExtractor.extract(item.recordFileOrThrow());
                    mergedTransfersList.add(
                            TransferListExtractor.mergeTransferData(extracted.items(), extracted.amendments()));
                }
            }
        } catch (ParseException e) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Failed to parse block items: " + e.getMessage());
        }

        // Compute the net HBAR delta without modifying the base state.
        // Use an in-block overlay to correctly handle multiple updates to the same account
        // within a single block (each delta is computed against the most recent balance, not
        // always against the committed base state).
        long hbarDelta = 0;
        final Map<Long, Long> inBlockBalances = new HashMap<>();

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

        for (final List<TransferData> mergedItems : mergedTransfersList) {
            for (final TransferData td : mergedItems) {
                for (final AccountTransfer at : td.hbarTransfers()) {
                    hbarDelta += at.amount();
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

        // Stage the parsed items for commitState()
        stagedStateChanges = parsedStateChanges;
        stagedMergedTransfers = mergedTransfersList;
    }

    @Override
    public void commitState(final BlockUnparsed block, final long blockNumber) {
        // Apply cached parsed items to the base state (no re-parsing needed)
        applyToAccounts(stagedStateChanges, stagedMergedTransfers, accounts);
        stagedStateChanges = null;
        stagedMergedTransfers = null;
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
     * Applies all balance mutations from pre-parsed StateChanges and extracted transfer data
     * to the given accounts state.
     *
     * @param stateChangesList the parsed StateChanges items
     * @param mergedTransfersList the pre-merged transfer data per RecordFileItem
     * @param accounts the running account state to mutate
     */
    private static void applyToAccounts(
            final List<StateChanges> stateChangesList,
            final List<List<TransferData>> mergedTransfersList,
            final RunningAccountsState accounts) {
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

        for (final List<TransferData> mergedItems : mergedTransfersList) {
            for (final TransferData td : mergedItems) {
                // HBAR transfers
                for (final AccountTransfer at : td.hbarTransfers()) {
                    accounts.applyHbarChange(at.accountNum(), at.amount());
                }
                // Token transfers
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
        accounts.save(directory.resolve(SAVE_FILE_NAME));
    }

    @Override
    public void load(final Path directory) throws IOException {
        Path file = directory.resolve(SAVE_FILE_NAME);
        if (!Files.exists(file)) return;
        accounts.load(file);
    }
}
