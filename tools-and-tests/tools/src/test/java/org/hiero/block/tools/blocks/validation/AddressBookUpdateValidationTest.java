// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.FileID;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.file.FileUpdateTransactionBody;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link AddressBookUpdateValidation}. */
class AddressBookUpdateValidationTest {

    private static BlockUnparsed toUnparsed(Block block) {
        try {
            return BlockUnparsed.PROTOBUF.parse(Block.PROTOBUF.toBytes(block));
        } catch (Exception e) {
            throw new AssertionError("Failed to convert block to unparsed", e);
        }
    }

    @Nested
    @DisplayName("Basic properties")
    class BasicProperties {

        @Test
        @DisplayName("name returns AddressBookUpdate")
        void nameReturnsExpected() {
            AddressBookRegistry registry = new AddressBookRegistry();
            AddressBookUpdateValidation validation = new AddressBookUpdateValidation(registry);
            assertEquals("AddressBookUpdate", validation.name());
        }

        @Test
        @DisplayName("does not require genesis start")
        void doesNotRequireGenesisStart() {
            AddressBookRegistry registry = new AddressBookRegistry();
            AddressBookUpdateValidation validation = new AddressBookUpdateValidation(registry);
            assertFalse(validation.requiresGenesisStart());
        }
    }

    @Nested
    @DisplayName("Validate")
    class ValidateTests {

        @Test
        @DisplayName("block without RecordFile does not throw")
        void blockWithoutRecordFileDoesNotThrow() {
            AddressBookRegistry registry = new AddressBookRegistry();
            AddressBookUpdateValidation validation = new AddressBookUpdateValidation(registry);

            BlockItem headerItem = BlockItem.newBuilder()
                    .blockHeader(BlockHeader.newBuilder()
                            .number(0)
                            .blockTimestamp(Timestamp.newBuilder().seconds(1L).build())
                            .build())
                    .build();
            Block block = new Block(List.of(headerItem));
            assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 0));
        }

        @Test
        @DisplayName("block with RecordFile containing no address book txns does not change registry")
        void blockWithNoAddressBookTxnsDoesNotChangeRegistry() {
            AddressBookRegistry registry = new AddressBookRegistry();
            int initialCount = registry.getAddressBookCount();
            AddressBookUpdateValidation validation = new AddressBookUpdateValidation(registry);

            // Create a block with a RecordFile that has no transactions (only a record)
            BlockItem headerItem = BlockItem.newBuilder()
                    .blockHeader(BlockHeader.newBuilder()
                            .number(1)
                            .blockTimestamp(Timestamp.newBuilder().seconds(100L).build())
                            .build())
                    .build();

            RecordStreamItem rsi = RecordStreamItem.newBuilder().build();
            RecordStreamFile rsf = RecordStreamFile.newBuilder()
                    .hapiProtoVersion(SemanticVersion.newBuilder()
                            .major(0)
                            .minor(50)
                            .patch(0)
                            .build())
                    .recordStreamItems(List.of(rsi))
                    .build();
            RecordFileItem rfi = RecordFileItem.newBuilder()
                    .creationTime(Timestamp.newBuilder().seconds(100L).build())
                    .recordFileContents(rsf)
                    .build();
            BlockItem recordItem = BlockItem.newBuilder().recordFile(rfi).build();

            Block block = new Block(List.of(headerItem, recordItem));
            assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 1));
            assertEquals(initialCount, registry.getAddressBookCount(), "Registry should not change");
        }

        @Test
        @DisplayName("block with file 102 update transaction updates the registry")
        void blockWithAddressBookUpdateChangesRegistry() throws Exception {
            AddressBookRegistry registry = new AddressBookRegistry();
            int initialCount = registry.getAddressBookCount();
            AddressBookUpdateValidation validation = new AddressBookUpdateValidation(registry);

            // Build a new address book with an extra node
            List<NodeAddress> nodes =
                    new ArrayList<>(registry.getCurrentAddressBook().nodeAddress());
            nodes.add(NodeAddress.newBuilder()
                    .nodeId(99)
                    .rsaPubKey(
                            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
                    .nodeAccountId(AccountID.newBuilder()
                            .shardNum(0)
                            .realmNum(0)
                            .accountNum(102)
                            .build())
                    .memo(Bytes.wrap("0.0.102".getBytes(StandardCharsets.UTF_8)))
                    .build());
            NodeAddressBook newBook = new NodeAddressBook(nodes);
            Bytes newBookBytes = NodeAddressBook.PROTOBUF.toBytes(newBook);

            // Create a file update transaction body targeting file 0.0.102
            TransactionBody txBody = TransactionBody.newBuilder()
                    .fileUpdate(FileUpdateTransactionBody.newBuilder()
                            .fileID(FileID.newBuilder()
                                    .shardNum(0)
                                    .realmNum(0)
                                    .fileNum(102)
                                    .build())
                            .contents(newBookBytes)
                            .build())
                    .build();
            Transaction txn = Transaction.newBuilder().body(txBody).build();

            // Build RecordStreamItem with the transaction
            RecordStreamItem rsi =
                    RecordStreamItem.newBuilder().transaction(txn).build();
            RecordStreamFile rsf = RecordStreamFile.newBuilder()
                    .hapiProtoVersion(SemanticVersion.newBuilder()
                            .major(0)
                            .minor(50)
                            .patch(0)
                            .build())
                    .recordStreamItems(List.of(rsi))
                    .build();
            RecordFileItem rfi = RecordFileItem.newBuilder()
                    .creationTime(Timestamp.newBuilder().seconds(200L).build())
                    .recordFileContents(rsf)
                    .build();

            BlockItem headerItem = BlockItem.newBuilder()
                    .blockHeader(BlockHeader.newBuilder()
                            .number(5)
                            .blockTimestamp(Timestamp.newBuilder().seconds(200L).build())
                            .build())
                    .build();
            BlockItem recordItem = BlockItem.newBuilder().recordFile(rfi).build();

            Block block = new Block(List.of(headerItem, recordItem));
            validation.validate(toUnparsed(block), 5);

            assertEquals(initialCount + 1, registry.getAddressBookCount(), "Registry should have one more entry");
        }

        @Test
        @DisplayName("block with empty RecordFile bytes does not throw")
        void blockWithEmptyRecordFileBytesDoesNotThrow() {
            AddressBookRegistry registry = new AddressBookRegistry();
            AddressBookUpdateValidation validation = new AddressBookUpdateValidation(registry);

            BlockItem headerItem = BlockItem.newBuilder()
                    .blockHeader(BlockHeader.newBuilder()
                            .number(0)
                            .blockTimestamp(Timestamp.newBuilder().seconds(1L).build())
                            .build())
                    .build();
            // Empty RecordFileItem (no contents)
            BlockItem recordItem =
                    BlockItem.newBuilder().recordFile(RecordFileItem.DEFAULT).build();
            Block block = new Block(List.of(headerItem, recordItem));
            assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 0));
        }

        @Test
        @DisplayName("block without BlockHeader does not throw")
        void blockWithoutBlockHeaderDoesNotThrow() {
            AddressBookRegistry registry = new AddressBookRegistry();
            AddressBookUpdateValidation validation = new AddressBookUpdateValidation(registry);

            // Block with only a RecordFile, no header
            RecordStreamFile rsf = RecordStreamFile.newBuilder()
                    .hapiProtoVersion(SemanticVersion.newBuilder()
                            .major(0)
                            .minor(50)
                            .patch(0)
                            .build())
                    .recordStreamItems(List.of())
                    .build();
            RecordFileItem rfi = RecordFileItem.newBuilder()
                    .creationTime(Timestamp.newBuilder().seconds(100L).build())
                    .recordFileContents(rsf)
                    .build();
            BlockItem recordItem = BlockItem.newBuilder().recordFile(rfi).build();
            Block block = new Block(List.of(recordItem));
            assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 0));
        }

        @Test
        @DisplayName("address book update applies with +1ns offset so current block uses old book")
        void addressBookUpdateAppliesWithOffsetTimestamp() throws Exception {
            AddressBookRegistry registry = new AddressBookRegistry();
            AddressBookUpdateValidation validation = new AddressBookUpdateValidation(registry);

            // Build a new address book with an extra node
            List<NodeAddress> nodes =
                    new ArrayList<>(registry.getCurrentAddressBook().nodeAddress());
            nodes.add(NodeAddress.newBuilder()
                    .nodeId(99)
                    .rsaPubKey(
                            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
                    .nodeAccountId(AccountID.newBuilder()
                            .shardNum(0)
                            .realmNum(0)
                            .accountNum(102)
                            .build())
                    .memo(Bytes.wrap("0.0.102".getBytes(StandardCharsets.UTF_8)))
                    .build());
            NodeAddressBook newBook = new NodeAddressBook(nodes);
            Bytes newBookBytes = NodeAddressBook.PROTOBUF.toBytes(newBook);

            TransactionBody txBody = TransactionBody.newBuilder()
                    .fileUpdate(FileUpdateTransactionBody.newBuilder()
                            .fileID(FileID.newBuilder()
                                    .shardNum(0)
                                    .realmNum(0)
                                    .fileNum(102)
                                    .build())
                            .contents(newBookBytes)
                            .build())
                    .build();
            Transaction txn = Transaction.newBuilder().body(txBody).build();

            RecordStreamItem rsi =
                    RecordStreamItem.newBuilder().transaction(txn).build();
            RecordStreamFile rsf = RecordStreamFile.newBuilder()
                    .hapiProtoVersion(SemanticVersion.newBuilder()
                            .major(0)
                            .minor(50)
                            .patch(0)
                            .build())
                    .recordStreamItems(List.of(rsi))
                    .build();
            // Use a timestamp well after genesis (mainnet genesis is 2019-09-13)
            long blockSeconds = 1_700_000_000L; // Nov 14, 2023
            RecordFileItem rfi = RecordFileItem.newBuilder()
                    .creationTime(Timestamp.newBuilder().seconds(blockSeconds).build())
                    .recordFileContents(rsf)
                    .build();

            BlockItem headerItem = BlockItem.newBuilder()
                    .blockHeader(BlockHeader.newBuilder()
                            .number(10)
                            .blockTimestamp(
                                    Timestamp.newBuilder().seconds(blockSeconds).build())
                            .build())
                    .build();
            BlockItem recordItem = BlockItem.newBuilder().recordFile(rfi).build();
            Block block = new Block(List.of(headerItem, recordItem));
            validation.validate(toUnparsed(block), 10);

            // The address book at the exact block time should still be the OLD book
            // because the update was stored at blockTime + 1ns
            Instant blockTime = Instant.ofEpochSecond(blockSeconds);
            int oldNodeCount =
                    registry.getAddressBookForBlock(blockTime).nodeAddress().size();

            // The address book 1ns later should be the NEW book
            Instant afterBlockTime = blockTime.plusNanos(1);
            int newNodeCount = registry.getAddressBookForBlock(afterBlockTime)
                    .nodeAddress()
                    .size();

            assertEquals(oldNodeCount + 1, newNodeCount, "New book should have one more node than old book");
        }
    }

    @Nested
    @DisplayName("Save and Load")
    class SaveLoadTests {

        @Test
        @DisplayName("save and load round-trip preserves address book state")
        void saveAndLoadRoundTrip(@TempDir Path tempDir) throws Exception {
            AddressBookRegistry registry = new AddressBookRegistry();
            AddressBookUpdateValidation validation = new AddressBookUpdateValidation(registry);

            // Save the current state
            validation.save(tempDir);
            Path savedFile = tempDir.resolve("addressBookHistory.json");
            assert Files.exists(savedFile) : "Saved file should exist";

            // Create a new registry and validation, load from the saved state
            AddressBookRegistry registry2 = new AddressBookRegistry();
            AddressBookUpdateValidation validation2 = new AddressBookUpdateValidation(registry2);
            validation2.load(tempDir);

            assertEquals(
                    registry.getAddressBookCount(),
                    registry2.getAddressBookCount(),
                    "Loaded registry should have same count");
        }
    }
}
