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
import com.hedera.hapi.node.transaction.NodeStake;
import com.hedera.hapi.node.transaction.NodeStakeUpdateTransactionBody;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.HashAlgorithm;
import com.hedera.hapi.streams.HashObject;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.days.model.NodeStakeRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("NodeStakeUpdateValidation")
class NodeStakeUpdateValidationTest {

    private static final Timestamp BLOCK_TS =
            Timestamp.newBuilder().seconds(1000).nanos(0).build();
    private static final byte[] DUMMY_HASH = new byte[48];

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
        @DisplayName("name returns NodeStakeUpdate")
        void name() {
            NodeStakeUpdateValidation v = new NodeStakeUpdateValidation(new NodeStakeRegistry());
            assertEquals("NodeStakeUpdate", v.name());
        }

        @Test
        @DisplayName("description is non-empty")
        void description() {
            NodeStakeUpdateValidation v = new NodeStakeUpdateValidation(new NodeStakeRegistry());
            assertFalse(v.description().isEmpty());
        }

        @Test
        @DisplayName("does not require genesis start")
        void requiresGenesisStart() {
            NodeStakeUpdateValidation v = new NodeStakeUpdateValidation(new NodeStakeRegistry());
            assertFalse(v.requiresGenesisStart());
        }
    }

    @Nested
    @DisplayName("validate")
    class Validate {

        @Test
        @DisplayName("block without NodeStakeUpdate does not change registry")
        void blockWithoutNodeStakeUpdateLeavesRegistryEmpty() throws Exception {
            NodeStakeRegistry registry = new NodeStakeRegistry();
            NodeStakeUpdateValidation validation = new NodeStakeUpdateValidation(registry);

            // Block with empty RecordStreamFile (no NodeStakeUpdate transaction)
            Block block = createBlockWithTransactions(List.of());
            validation.validate(toUnparsed(block), 1);
            assertFalse(registry.hasStakeData());
        }

        @Test
        @DisplayName("block with NodeStakeUpdate updates registry")
        void blockWithNodeStakeUpdatePopulatesRegistry() throws Exception {
            NodeStakeRegistry registry = new NodeStakeRegistry();
            NodeStakeUpdateValidation validation = new NodeStakeUpdateValidation(registry);

            // Create a NodeStakeUpdate transaction
            NodeStakeUpdateTransactionBody stakeUpdateBody = NodeStakeUpdateTransactionBody.newBuilder()
                    .nodeStake(List.of(
                            NodeStake.newBuilder().nodeId(0).stake(1000).build(),
                            NodeStake.newBuilder().nodeId(1).stake(2000).build()))
                    .build();
            TransactionBody txBody = TransactionBody.newBuilder()
                    .nodeStakeUpdate(stakeUpdateBody)
                    .build();
            Transaction tx = Transaction.newBuilder().body(txBody).build();

            Block block = createBlockWithTransactions(List.of(tx));
            validation.validate(toUnparsed(block), 1);
            assertTrue(registry.hasStakeData());
            assertEquals(1, registry.getSnapshotCount());
        }
    }

    @Nested
    @DisplayName("persistence")
    class Persistence {

        @Test
        @DisplayName("save and load roundtrip preserves data")
        void saveLoadRoundtrip(@TempDir Path tempDir) throws Exception {
            NodeStakeRegistry registry = new NodeStakeRegistry();
            NodeStakeUpdateValidation validation = new NodeStakeUpdateValidation(registry);

            // Create a NodeStakeUpdate transaction and validate
            NodeStakeUpdateTransactionBody stakeUpdateBody = NodeStakeUpdateTransactionBody.newBuilder()
                    .nodeStake(
                            List.of(NodeStake.newBuilder().nodeId(0).stake(5000).build()))
                    .build();
            TransactionBody txBody = TransactionBody.newBuilder()
                    .nodeStakeUpdate(stakeUpdateBody)
                    .build();
            Transaction tx = Transaction.newBuilder().body(txBody).build();
            Block block = createBlockWithTransactions(List.of(tx));
            validation.validate(toUnparsed(block), 1);

            // Save
            validation.save(tempDir);

            // Load into a fresh registry
            NodeStakeRegistry freshRegistry = new NodeStakeRegistry();
            NodeStakeUpdateValidation freshValidation = new NodeStakeUpdateValidation(freshRegistry);
            freshValidation.load(tempDir);

            assertTrue(freshRegistry.hasStakeData());
            assertEquals(1, freshRegistry.getSnapshotCount());
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
