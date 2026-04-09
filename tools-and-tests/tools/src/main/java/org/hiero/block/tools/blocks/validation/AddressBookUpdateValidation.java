// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import picocli.CommandLine.Help.Ansi;

/**
 * Sequential validation that discovers address book updates from RecordFile items
 * in each block and keeps the {@link AddressBookRegistry} up to date.
 *
 * <p>This allows the validate command to process blocks without requiring a
 * pre-generated addressBookHistory.json that covers the entire block range.
 * Address book changes are discovered from file update/append transactions
 * targeting file 0.0.102 within the block's RecordFile data.
 */
public final class AddressBookUpdateValidation implements BlockValidation {

    private static final String SAVE_FILE_NAME = "addressBookHistory.json";
    private static final int MAX_DEPTH = 512;
    private static final int MAX_RECORD_FILE_SIZE = 64 * 1024 * 1024;

    private final AddressBookRegistry addressBookRegistry;

    /**
     * Creates a new address book update validation.
     *
     * @param addressBookRegistry the shared registry to update when changes are found
     */
    public AddressBookUpdateValidation(final AddressBookRegistry addressBookRegistry) {
        this.addressBookRegistry = addressBookRegistry;
    }

    @Override
    public String name() {
        return "AddressBookUpdate";
    }

    @Override
    public String description() {
        return "Discovers address book updates from block data and keeps the registry current";
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
            final List<Transaction> transactions = extractTransactions(recordFileBytes);
            if (transactions.isEmpty()) {
                return;
            }
            applyAddressBookUpdates(transactions, blockInstant, blockNumber);
        } catch (Exception e) {
            // Don't fail validation for address book parse/extraction errors — the block's
            // record file might be in a format we don't fully handle (e.g., genesis or very early blocks).
            // Signature validation will catch if the address book is actually wrong.
        }
    }

    private static Instant extractBlockInstant(final BlockUnparsed block) throws Exception {
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

    private static List<Transaction> extractTransactions(final Bytes recordFileBytes) throws Exception {
        final RecordFileItem recordFileItem = RecordFileItem.PROTOBUF.parse(
                recordFileBytes.toReadableSequentialData(), false, false, MAX_DEPTH, MAX_RECORD_FILE_SIZE);
        if (!recordFileItem.hasRecordFileContents()) {
            return List.of();
        }
        final RecordStreamFile recordStreamFile = recordFileItem.recordFileContentsOrThrow();
        final List<Transaction> transactions = new ArrayList<>();
        for (final RecordStreamItem rsi : recordStreamFile.recordStreamItems()) {
            if (rsi.hasTransaction()) {
                transactions.add(rsi.transactionOrThrow());
            }
        }
        return transactions;
    }

    private void applyAddressBookUpdates(
            final List<Transaction> transactions, final Instant blockInstant, final long blockNumber) throws Exception {
        final List<TransactionBody> addressBookTxns =
                AddressBookRegistry.filterToJustAddressBookTransactions(transactions);
        if (!addressBookTxns.isEmpty()) {
            // Store with timestamp +1ns so the new address book applies to blocks AFTER
            // this one. The current block was signed with the OLD keys, so
            // getAddressBookForBlock(blockInstant) must still return the previous book.
            final String changes = addressBookRegistry.updateAddressBook(blockInstant.plusNanos(1), addressBookTxns);
            if (changes != null) {
                System.out.println(
                        Ansi.AUTO.string("@|yellow Address book updated at block " + blockNumber + ":|@ " + changes));
            }
        }
    }

    @Override
    public void save(final Path directory) throws IOException {
        addressBookRegistry.saveAddressBookRegistryToJsonFile(directory.resolve(SAVE_FILE_NAME));
    }

    @Override
    public void load(final Path directory) throws IOException {
        final Path saved = directory.resolve(SAVE_FILE_NAME);
        if (saved.toFile().exists()) {
            addressBookRegistry.reloadFromFile(saved);
        }
    }
}
