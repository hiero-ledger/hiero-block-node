// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.node.base.AccountAmount;
import com.hedera.hapi.node.base.NftTransfer;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.TokenTransferList;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
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
 *   <li>{@link #validate} computes the HBAR balance delta from this block's items without
 *       modifying the base state, then checks that {@code base_total + delta == 50B}.
 *   <li>{@link #commitState} applies the block's mutations to the base {@link RunningAccountsState}.
 * </ul>
 *
 * <p>This validation requires starting from block 0 because the full account state history
 * is needed.
 */
public final class HbarSupplyValidation implements BlockValidation {

    /** Total HBAR supply expressed in tinybar (50 billion HBAR * 100 million tinybar per HBAR). */
    public static final long FIFTY_BILLION_HBAR_IN_TINYBAR = 5_000_000_000_000_000_000L;

    /** The running account state. */
    private final RunningAccountsState accounts = new RunningAccountsState();

    /** The block staged for commit (set during validate, applied during commitState). */
    private BlockUnparsed stagedBlock;

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
        // Compute the net HBAR delta from this block's items without modifying the base state.
        // Use an in-block overlay to correctly handle multiple updates to the same account
        // within a single block (each delta is computed against the most recent balance, not
        // always against the committed base state).
        long hbarDelta = 0;
        final Map<Long, Long> inBlockBalances = new HashMap<>();

        try {
            for (final BlockItemUnparsed item : block.blockItems()) {
                if (item.hasStateChanges()) {
                    final StateChanges stateChanges = StateChanges.PROTOBUF.parse(item.stateChangesOrThrow());
                    for (final StateChange stateChange : stateChanges.stateChanges()) {
                        if (stateChange.hasMapUpdate()) {
                            final MapUpdateChange mapUpdate = stateChange.mapUpdateOrThrow();
                            final MapChangeKey key = mapUpdate.keyOrThrow();
                            final MapChangeValue value = mapUpdate.valueOrThrow();
                            if (key.hasAccountIdKey() && value.hasAccountValue()) {
                                final long accountNum =
                                        key.accountIdKeyOrThrow().accountNumOrThrow();
                                final long newBalance =
                                        value.accountValueOrThrow().tinybarBalance();
                                final long oldBalance = inBlockBalances.containsKey(accountNum)
                                        ? inBlockBalances.get(accountNum)
                                        : accounts.getHbarBalance(accountNum);
                                hbarDelta += (newBalance - oldBalance);
                                inBlockBalances.put(accountNum, newBalance);
                            }
                        } else if (stateChange.hasMapDelete()) {
                            final MapChangeKey key =
                                    stateChange.mapDeleteOrThrow().keyOrThrow();
                            if (key.hasAccountIdKey()) {
                                final long accountNum =
                                        key.accountIdKeyOrThrow().accountNumOrThrow();
                                final long oldBalance = inBlockBalances.containsKey(accountNum)
                                        ? inBlockBalances.get(accountNum)
                                        : accounts.getHbarBalance(accountNum);
                                hbarDelta -= oldBalance;
                                inBlockBalances.remove(accountNum);
                            }
                        }
                    }
                } else if (item.hasRecordFile()) {
                    final RecordFileItem recordFile = RecordFileItem.PROTOBUF.parse(item.recordFileOrThrow());
                    final List<RecordStreamItem> mergedItems = mergeRecordStreamItems(
                            recordFile.recordFileContentsOrThrow().recordStreamItems(), recordFile.amendments());
                    for (final RecordStreamItem recordStreamItem : mergedItems) {
                        for (final AccountAmount accountAmount : recordStreamItem
                                .recordOrThrow()
                                .transferListOrThrow()
                                .accountAmounts()) {
                            hbarDelta += accountAmount.amount();
                        }
                    }
                }
            }
        } catch (ParseException e) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Failed to parse block items: " + e.getMessage());
        }

        // Verify total HBAR supply using base total + delta
        final long totalBalance = accounts.totalHbarBalance() + hbarDelta;
        if (totalBalance != FIFTY_BILLION_HBAR_IN_TINYBAR) {
            final long difference = totalBalance - FIFTY_BILLION_HBAR_IN_TINYBAR;
            throw new ValidationException("Block: " + blockNumber + " - Total HBAR supply mismatch. Expected "
                    + FIFTY_BILLION_HBAR_IN_TINYBAR + " tinybar but found " + totalBalance + " tinybar (difference: "
                    + difference + ")");
        }

        // Stage the block for commit
        stagedBlock = block;
    }

    @Override
    public void commitState(final BlockUnparsed block, final long blockNumber) {
        // Apply all mutations to the base state
        applyBlockToAccounts(stagedBlock, accounts);
        stagedBlock = null;
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
     * Applies all balance mutations from a block to the given accounts state.
     * Selectively parses only StateChanges and RecordFile items from the unparsed block.
     *
     * @param block the block whose state changes and transfers to apply
     * @param accounts the running account state to mutate
     */
    public static void applyBlockToAccounts(final BlockUnparsed block, final RunningAccountsState accounts) {
        try {
            for (final BlockItemUnparsed item : block.blockItems()) {
                if (item.hasStateChanges()) {
                    final StateChanges stateChanges = StateChanges.PROTOBUF.parse(item.stateChangesOrThrow());
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
                            final MapChangeKey key =
                                    stateChange.mapDeleteOrThrow().keyOrThrow();
                            if (key.hasAccountIdKey()) {
                                accounts.deleteAccount(key.accountIdKeyOrThrow().accountNumOrThrow());
                            }
                        }
                    }
                } else if (item.hasRecordFile()) {
                    final RecordFileItem recordFile = RecordFileItem.PROTOBUF.parse(item.recordFileOrThrow());
                    final List<RecordStreamItem> mergedItems = mergeRecordStreamItems(
                            recordFile.recordFileContentsOrThrow().recordStreamItems(), recordFile.amendments());
                    for (final RecordStreamItem recordStreamItem : mergedItems) {
                        // HBAR transfers
                        for (final AccountAmount accountAmount : recordStreamItem
                                .recordOrThrow()
                                .transferListOrThrow()
                                .accountAmounts()) {
                            accounts.applyHbarChange(
                                    accountAmount.accountIDOrThrow().accountNumOrThrow(), accountAmount.amount());
                        }
                        // Token transfers
                        for (final TokenTransferList tokenTransferList :
                                recordStreamItem.recordOrThrow().tokenTransferLists()) {
                            final long tokenNum =
                                    tokenTransferList.tokenOrThrow().tokenNum();
                            for (final AccountAmount transfer : tokenTransferList.transfers()) {
                                accounts.applyFungibleTokenChange(
                                        transfer.accountIDOrThrow().accountNumOrThrow(), tokenNum, transfer.amount());
                            }
                            for (final NftTransfer nftTransfer : tokenTransferList.nftTransfers()) {
                                accounts.applyNftTransfer(
                                        nftTransfer.senderAccountIDOrThrow().accountNumOrThrow(),
                                        nftTransfer.receiverAccountIDOrThrow().accountNumOrThrow(),
                                        tokenNum,
                                        nftTransfer.serialNumber());
                            }
                        }
                    }
                }
            }
        } catch (ParseException e) {
            throw new RuntimeException("Failed to parse block items in applyBlockToAccounts", e);
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

    /**
     * Merges original record stream items with amendments, sorted by consensus timestamp.
     *
     * @param original the original record stream items from the record file
     * @param amendments the amendment items to merge in (may replace or add)
     * @return a new list containing merged items sorted by consensus timestamp
     */
    static List<RecordStreamItem> mergeRecordStreamItems(
            final List<RecordStreamItem> original, final List<RecordStreamItem> amendments) {
        if (amendments.isEmpty()) {
            return original;
        }
        final Map<Long, RecordStreamItem> itemsByTimestamp = new TreeMap<>();
        for (final RecordStreamItem item : original) {
            final Timestamp ts = item.recordOrThrow().consensusTimestampOrThrow();
            final long key = ts.seconds() * 1_000_000_000L + ts.nanos();
            itemsByTimestamp.put(key, item);
        }
        for (final RecordStreamItem amendment : amendments) {
            final Timestamp ts = amendment.recordOrThrow().consensusTimestampOrThrow();
            final long key = ts.seconds() * 1_000_000_000L + ts.nanos();
            itemsByTimestamp.put(key, amendment);
        }
        return new ArrayList<>(itemsByTimestamp.values());
    }
}
