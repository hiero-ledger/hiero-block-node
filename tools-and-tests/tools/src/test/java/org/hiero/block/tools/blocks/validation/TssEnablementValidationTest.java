// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.node.tss.LedgerIdNodeContribution;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.hapi.streams.HashAlgorithm;
import com.hedera.hapi.streams.HashObject;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.api.TssData;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.days.model.TssEnablementRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("TssEnablementValidation")
class TssEnablementValidationTest {

    private static final Timestamp BLOCK_TS =
            Timestamp.newBuilder().seconds(1000).nanos(0).build();
    private static final byte[] DUMMY_HASH = new byte[48];
    private static final Bytes LEDGER_ID = Bytes.wrap(new byte[] {1, 2, 3, 4});
    private static final Bytes WRAPS_VK = Bytes.wrap(new byte[] {10, 20, 30});

    private static BlockUnparsed toUnparsed(Block block) {
        try {
            return BlockUnparsed.PROTOBUF.parse(Block.PROTOBUF.toBytes(block));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Nested
    @DisplayName("metadata")
    class Metadata {

        @Test
        @DisplayName("name returns TssEnablement")
        void name(@TempDir Path tempDir) {
            TssEnablementValidation v =
                    new TssEnablementValidation(new TssEnablementRegistry(), tempDir.resolve("tss.bin"));
            assertEquals("TssEnablement", v.name());
        }

        @Test
        @DisplayName("description is non-empty")
        void description(@TempDir Path tempDir) {
            TssEnablementValidation v =
                    new TssEnablementValidation(new TssEnablementRegistry(), tempDir.resolve("tss.bin"));
            assertFalse(v.description().isEmpty());
        }

        @Test
        @DisplayName("does not require genesis start")
        void requiresGenesisStart(@TempDir Path tempDir) {
            TssEnablementValidation v =
                    new TssEnablementValidation(new TssEnablementRegistry(), tempDir.resolve("tss.bin"));
            assertFalse(v.requiresGenesisStart());
        }
    }

    @Nested
    @DisplayName("validate")
    class Validate {

        @Test
        @DisplayName("block without LedgerIdPublication does not change registry")
        void blockWithoutPublicationLeavesRegistryEmpty(@TempDir Path tempDir) throws Exception {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            TssEnablementValidation validation = new TssEnablementValidation(registry, tempDir.resolve("tss.bin"));

            Block block = createBlockWithTransactions(List.of());
            validation.validate(toUnparsed(block), 1);
            assertFalse(registry.hasTssData());
        }

        @Test
        @DisplayName("block with LedgerIdPublication updates registry and writes bin file")
        void blockWithPublicationPopulatesRegistry(@TempDir Path tempDir) throws Exception {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            Path binPath = tempDir.resolve("tss-enablement.bin");
            TssEnablementValidation validation = new TssEnablementValidation(registry, binPath);

            LedgerIdPublicationTransactionBody pubBody = LedgerIdPublicationTransactionBody.newBuilder()
                    .ledgerId(LEDGER_ID)
                    .historyProofVerificationKey(WRAPS_VK)
                    .nodeContributions(List.of(
                            LedgerIdNodeContribution.newBuilder()
                                    .nodeId(0)
                                    .weight(1000)
                                    .historyProofKey(Bytes.wrap(new byte[] {0xA}))
                                    .build(),
                            LedgerIdNodeContribution.newBuilder()
                                    .nodeId(1)
                                    .weight(2000)
                                    .historyProofKey(Bytes.wrap(new byte[] {0xB}))
                                    .build()))
                    .build();
            TransactionBody txBody =
                    TransactionBody.newBuilder().ledgerIdPublication(pubBody).build();
            Transaction tx = Transaction.newBuilder().body(txBody).build();

            Block block = createBlockWithTransactions(List.of(tx));
            validation.validate(toUnparsed(block), 1);
            assertTrue(registry.hasTssData());
            assertEquals(1, registry.getPublicationCount());
            assertTrue(Files.exists(binPath));

            // Verify the bin file is parseable as TssData
            Bytes fileBytes = Bytes.wrap(Files.readAllBytes(binPath));
            TssData parsed = TssData.PROTOBUF.parse(fileBytes);
            assertEquals(LEDGER_ID, parsed.ledgerId());
        }
    }

    @Nested
    @DisplayName("persistence")
    class Persistence {

        @Test
        @DisplayName("save and load roundtrip preserves data")
        void saveLoadRoundtrip(@TempDir Path tempDir) throws Exception {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            Path binPath = tempDir.resolve("tss-enablement.bin");
            TssEnablementValidation validation = new TssEnablementValidation(registry, binPath);

            LedgerIdPublicationTransactionBody pubBody = LedgerIdPublicationTransactionBody.newBuilder()
                    .ledgerId(LEDGER_ID)
                    .historyProofVerificationKey(WRAPS_VK)
                    .nodeContributions(List.of(LedgerIdNodeContribution.newBuilder()
                            .nodeId(0)
                            .weight(5000)
                            .historyProofKey(Bytes.wrap(new byte[] {0xF}))
                            .build()))
                    .build();
            TransactionBody txBody =
                    TransactionBody.newBuilder().ledgerIdPublication(pubBody).build();
            Transaction tx = Transaction.newBuilder().body(txBody).build();
            Block block = createBlockWithTransactions(List.of(tx));
            validation.validate(toUnparsed(block), 1);

            // Save
            validation.save(tempDir);

            // Load into fresh instances
            TssEnablementRegistry freshRegistry = new TssEnablementRegistry();
            Path freshBinPath = tempDir.resolve("tss-enablement-fresh.bin");
            TssEnablementValidation freshValidation = new TssEnablementValidation(freshRegistry, freshBinPath);
            freshValidation.load(tempDir);

            assertTrue(freshRegistry.hasTssData());
            assertEquals(1, freshRegistry.getPublicationCount());
            assertEquals(
                    LEDGER_ID, freshRegistry.getLatestPublication().tssData().ledgerId());
            // Verify bin file was regenerated on load
            assertTrue(Files.exists(freshBinPath));
        }
    }

    private static Block createBlockWithTransactions(List<Transaction> transactions) {
        HashObject runningHash = HashObject.newBuilder()
                .algorithm(HashAlgorithm.SHA_384)
                .length(48)
                .hash(Bytes.wrap(DUMMY_HASH))
                .build();

        List<RecordStreamItem> rsis = transactions.stream()
                .map(tx -> RecordStreamItem.newBuilder()
                        .transaction(tx)
                        .record(TransactionRecord.newBuilder()
                                .consensusTimestamp(BLOCK_TS)
                                .build())
                        .build())
                .toList();

        RecordStreamFile rsf = RecordStreamFile.newBuilder()
                .startObjectRunningHash(runningHash)
                .endObjectRunningHash(runningHash)
                .recordStreamItems(rsis)
                .build();
        RecordFileItem rfi = RecordFileItem.newBuilder()
                .creationTime(BLOCK_TS)
                .recordFileContents(rsf)
                .build();

        BlockHeader header =
                BlockHeader.newBuilder().number(1).blockTimestamp(BLOCK_TS).build();

        return new Block(List.of(
                BlockItem.newBuilder().blockHeader(header).build(),
                BlockItem.newBuilder().recordFile(rfi).build(),
                BlockItem.newBuilder().blockFooter(BlockFooter.DEFAULT).build()));
    }
}
