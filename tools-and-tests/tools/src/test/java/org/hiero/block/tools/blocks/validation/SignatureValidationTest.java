// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.transaction.NodeStake;
import com.hedera.hapi.node.transaction.NodeStakeUpdateTransactionBody;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.TestBlockFactory;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.model.NodeStakeRegistry;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link SignatureValidation}. */
class SignatureValidationTest {

    private static final BlockItem HEADER_ITEM = BlockItem.newBuilder()
            .blockHeader(BlockHeader.newBuilder()
                    .number(0)
                    .blockTimestamp(Timestamp.newBuilder().seconds(1L).build())
                    .build())
            .build();
    private static final BlockItem RECORD_FILE_ITEM =
            BlockItem.newBuilder().recordFile(RecordFileItem.DEFAULT).build();
    private static final BlockItem FOOTER_ITEM =
            BlockItem.newBuilder().blockFooter(BlockFooter.DEFAULT).build();

    private static BlockUnparsed toUnparsed(Block block) {
        try {
            return BlockUnparsed.PROTOBUF.parse(Block.PROTOBUF.toBytes(block));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void noBlockProof_throwsValidationException() {
        // Block with no proof item at all
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM));
        SignatureValidation validation = new SignatureValidation(null);
        ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 0));
        assertTrue(ex.getMessage().contains("No BlockProof found"));
    }

    @Test
    void emptyTssSignature_throwsValidationException() {
        // TSS proof with empty signature
        BlockItem proofItem = BlockItem.newBuilder()
                .blockProof(BlockProof.newBuilder()
                        .signedBlockProof(TssSignedBlockProof.newBuilder()
                                .blockSignature(Bytes.EMPTY)
                                .build())
                        .build())
                .build();
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, proofItem));
        SignatureValidation validation = new SignatureValidation(null);
        ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 42));
        assertTrue(ex.getMessage().contains("Empty TSS block signature"));
    }

    @Test
    void nonEmptyTssSignature_passes() {
        // TSS proof with non-empty signature
        BlockItem proofItem = BlockItem.newBuilder()
                .blockProof(BlockProof.newBuilder()
                        .signedBlockProof(TssSignedBlockProof.newBuilder()
                                .blockSignature(Bytes.wrap(new byte[] {1, 2, 3}))
                                .build())
                        .build())
                .build();
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, proofItem));
        SignatureValidation validation = new SignatureValidation(null);
        assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 42));
    }

    @Test
    void unknownProofType_throwsValidationException() {
        // BlockProof with no proof set
        BlockItem proofItem =
                BlockItem.newBuilder().blockProof(BlockProof.DEFAULT).build();
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, proofItem));
        SignatureValidation validation = new SignatureValidation(null);
        ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 0));
        assertTrue(ex.getMessage().contains("Unknown proof type"));
    }

    @Test
    void emptyRecordFileSignatures_throwsValidationException() {
        // SignedRecordFileProof with empty signature list
        BlockItem proofItem = BlockItem.newBuilder()
                .blockProof(BlockProof.newBuilder()
                        .signedRecordFileProof(
                                SignedRecordFileProof.newBuilder().build())
                        .build())
                .build();
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, proofItem));
        SignatureValidation validation = new SignatureValidation(null);
        ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 0));
        assertTrue(ex.getMessage().contains("No signatures"));
    }

    @Test
    void doesNotRequireGenesisStart() {
        SignatureValidation validation = new SignatureValidation(null);
        assertFalse(validation.requiresGenesisStart());
    }

    @Test
    void duplicateSignerEntries_countedOnceForThreshold(@TempDir Path tempDir) throws Exception {
        // Create a valid chain (5 nodes, threshold = 2)
        List<Block> chain = TestBlockFactory.createValidChain(1);
        Block block = chain.getFirst();
        // Replace signatures with 3 copies of node 0's signature
        List<BlockItem> items = new ArrayList<>();
        for (BlockItem item : block.items()) {
            if (item.hasBlockProof()) {
                SignedRecordFileProof orig = item.blockProofOrThrow().signedRecordFileProofOrThrow();
                RecordFileSignature firstSig = orig.recordFileSignatures().getFirst();
                // 3 copies of node 0
                List<RecordFileSignature> duplicatedSigs = List.of(firstSig, firstSig, firstSig);
                SignedRecordFileProof newProof = SignedRecordFileProof.newBuilder()
                        .version(orig.version())
                        .recordFileSignatures(duplicatedSigs)
                        .build();
                items.add(BlockItem.newBuilder()
                        .blockProof(BlockProof.newBuilder()
                                .signedRecordFileProof(newProof)
                                .build())
                        .build());
            } else {
                items.add(item);
            }
        }
        Block blockWithDuplicateSigs = new Block(items);
        // Write address book history and create registry
        TestBlockFactory.writeAddressBookHistory(tempDir);
        AddressBookRegistry registry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));
        SignatureValidation validation = new SignatureValidation(registry);
        // Only 1 unique node signed — below threshold of 2
        ValidationException ex = assertThrows(
                ValidationException.class, () -> validation.validate(toUnparsed(blockWithDuplicateSigs), 0));
        assertTrue(ex.getMessage().contains("Insufficient valid signatures"));
    }

    @Test
    void insufficientSignaturesErrorContainsDiagnostics(@TempDir Path tempDir) throws Exception {
        // Create a valid chain (5 nodes, threshold = 2) but keep only 1 signature
        List<Block> chain = TestBlockFactory.createValidChain(1);
        Block block = chain.getFirst();
        List<BlockItem> items = new ArrayList<>();
        for (BlockItem item : block.items()) {
            if (item.hasBlockProof()) {
                SignedRecordFileProof orig = item.blockProofOrThrow().signedRecordFileProofOrThrow();
                // Keep only 1 valid signature — below threshold of 2
                List<RecordFileSignature> oneSig =
                        List.of(orig.recordFileSignatures().getFirst());
                SignedRecordFileProof newProof = SignedRecordFileProof.newBuilder()
                        .version(orig.version())
                        .recordFileSignatures(oneSig)
                        .build();
                items.add(BlockItem.newBuilder()
                        .blockProof(BlockProof.newBuilder()
                                .signedRecordFileProof(newProof)
                                .build())
                        .build());
            } else {
                items.add(item);
            }
        }
        Block blockWith1Sig = new Block(items);
        TestBlockFactory.writeAddressBookHistory(tempDir);
        AddressBookRegistry registry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));
        SignatureValidation validation = new SignatureValidation(registry);
        ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(blockWith1Sig), 0));
        String msg = ex.getMessage();
        // Should contain per-signature diagnostics
        assertTrue(msg.contains("blockTime="), "Should include blockTime in diagnostic");
        assertTrue(msg.contains("Per-signature results:"), "Should include per-signature header");
        assertTrue(msg.contains("VERIFIED key=..."), "Should include VERIFIED result for the valid signature");
    }

    @Test
    void insufficientSignaturesErrorContainsErrorDiagnosticForUnknownNode(@TempDir Path tempDir) throws Exception {
        // Create a block with only an unknown node signature
        List<Block> chain = TestBlockFactory.createValidChain(1);
        Block block = chain.getFirst();
        List<BlockItem> items = new ArrayList<>();
        for (BlockItem item : block.items()) {
            if (item.hasBlockProof()) {
                SignedRecordFileProof orig = item.blockProofOrThrow().signedRecordFileProofOrThrow();
                // Only an unknown node signature
                List<RecordFileSignature> unknownSig = List.of(RecordFileSignature.newBuilder()
                        .nodeId(99)
                        .signaturesBytes(Bytes.wrap(new byte[] {1, 2, 3}))
                        .build());
                SignedRecordFileProof newProof = SignedRecordFileProof.newBuilder()
                        .version(orig.version())
                        .recordFileSignatures(unknownSig)
                        .build();
                items.add(BlockItem.newBuilder()
                        .blockProof(BlockProof.newBuilder()
                                .signedRecordFileProof(newProof)
                                .build())
                        .build());
            } else {
                items.add(item);
            }
        }
        Block blockWithUnknown = new Block(items);
        TestBlockFactory.writeAddressBookHistory(tempDir);
        AddressBookRegistry registry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));
        SignatureValidation validation = new SignatureValidation(registry);
        ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(blockWithUnknown), 0));
        String msg = ex.getMessage();
        assertTrue(msg.contains("ERROR"), "Should include ERROR diagnostic for unknown node");
    }

    @Test
    void unknownSignerMixedWithValid_thresholdStillMet(@TempDir Path tempDir) throws Exception {
        // Create a valid chain (5 nodes, threshold = 2)
        List<Block> chain = TestBlockFactory.createValidChain(1);
        Block block = chain.getFirst();
        // Keep only 2 valid signatures and add one from an unknown node (nodeId=99)
        List<BlockItem> items = new ArrayList<>();
        for (BlockItem item : block.items()) {
            if (item.hasBlockProof()) {
                SignedRecordFileProof orig = item.blockProofOrThrow().signedRecordFileProofOrThrow();
                List<RecordFileSignature> twoValid =
                        new ArrayList<>(orig.recordFileSignatures().subList(0, 2));
                // Add unknown node signature (bogus bytes)
                twoValid.add(RecordFileSignature.newBuilder()
                        .nodeId(99)
                        .signaturesBytes(Bytes.wrap(new byte[] {1, 2, 3}))
                        .build());
                SignedRecordFileProof newProof = SignedRecordFileProof.newBuilder()
                        .version(orig.version())
                        .recordFileSignatures(twoValid)
                        .build();
                items.add(BlockItem.newBuilder()
                        .blockProof(BlockProof.newBuilder()
                                .signedRecordFileProof(newProof)
                                .build())
                        .build());
            } else {
                items.add(item);
            }
        }
        Block blockWithUnknownSigner = new Block(items);
        // Write address book and create registry
        TestBlockFactory.writeAddressBookHistory(tempDir);
        AddressBookRegistry registry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));
        SignatureValidation validation = new SignatureValidation(registry);
        // 2 valid nodes >= threshold of 2 → should pass
        assertDoesNotThrow(() -> validation.validate(toUnparsed(blockWithUnknownSigner), 0));
    }

    @Nested
    @DisplayName("Stake-weighted consensus")
    class StakeWeightedTests {

        @Test
        @DisplayName("valid chain passes with null stake registry (equal-weight fallback)")
        void validChainPassesWithNullStakeRegistry(@TempDir Path tempDir) throws Exception {
            List<Block> chain = TestBlockFactory.createValidChain(1);
            TestBlockFactory.writeAddressBookHistory(tempDir);
            AddressBookRegistry abRegistry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));
            // null stake registry → equal-weight mode
            SignatureValidation validation = new SignatureValidation(abRegistry, null);
            assertDoesNotThrow(() -> validation.validate(toUnparsed(chain.getFirst()), 0));
        }

        @Test
        @DisplayName("valid chain passes with empty stake registry (equal-weight fallback)")
        void validChainPassesWithEmptyStakeRegistry(@TempDir Path tempDir) throws Exception {
            List<Block> chain = TestBlockFactory.createValidChain(1);
            TestBlockFactory.writeAddressBookHistory(tempDir);
            AddressBookRegistry abRegistry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));
            NodeStakeRegistry stakeRegistry = new NodeStakeRegistry();
            // Empty stake registry → equal-weight fallback
            SignatureValidation validation = new SignatureValidation(abRegistry, stakeRegistry);
            assertDoesNotThrow(() -> validation.validate(toUnparsed(chain.getFirst()), 0));
        }

        @Test
        @DisplayName("two-arg constructor backward compatible with single-arg")
        void twoArgConstructorBackwardCompatible(@TempDir Path tempDir) throws Exception {
            List<Block> chain = TestBlockFactory.createValidChain(1);
            TestBlockFactory.writeAddressBookHistory(tempDir);
            AddressBookRegistry abRegistry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));
            // Single-arg constructor should still work
            SignatureValidation validation = new SignatureValidation(abRegistry);
            assertDoesNotThrow(() -> validation.validate(toUnparsed(chain.getFirst()), 0));
        }

        @Test
        @DisplayName("stake-weighted mode shows mode in error diagnostics")
        void stakeWeightedModeShowsModeInDiagnostics(@TempDir Path tempDir) throws Exception {
            List<Block> chain = TestBlockFactory.createValidChain(1);
            Block block = chain.getFirst();
            // Keep only 1 signature — below threshold
            List<BlockItem> items = new ArrayList<>();
            for (BlockItem item : block.items()) {
                if (item.hasBlockProof()) {
                    SignedRecordFileProof orig = item.blockProofOrThrow().signedRecordFileProofOrThrow();
                    List<RecordFileSignature> oneSig =
                            List.of(orig.recordFileSignatures().getFirst());
                    SignedRecordFileProof newProof = SignedRecordFileProof.newBuilder()
                            .version(orig.version())
                            .recordFileSignatures(oneSig)
                            .build();
                    items.add(BlockItem.newBuilder()
                            .blockProof(BlockProof.newBuilder()
                                    .signedRecordFileProof(newProof)
                                    .build())
                            .build());
                } else {
                    items.add(item);
                }
            }
            Block blockWith1Sig = new Block(items);
            TestBlockFactory.writeAddressBookHistory(tempDir);
            AddressBookRegistry abRegistry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));
            // Empty stake registry → equal-weight fallback
            NodeStakeRegistry stakeRegistry = new NodeStakeRegistry();
            SignatureValidation validation = new SignatureValidation(abRegistry, stakeRegistry);
            ValidationException ex =
                    assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(blockWith1Sig), 0));
            assertTrue(ex.getMessage().contains("mode=equal-weight"), "Should show equal-weight mode");
        }

        @Test
        @DisplayName("all signatures pass with stake-weighted threshold")
        void allSignaturesPassStakeWeighted(@TempDir Path tempDir) throws Exception {
            // TestBlockFactory: 5 nodes (IDs 0-4), block time ~1568411631
            List<Block> chain = TestBlockFactory.createValidChain(1);
            TestBlockFactory.writeAddressBookHistory(tempDir);
            AddressBookRegistry abRegistry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));

            // Populate stake registry with data before block time
            NodeStakeRegistry stakeRegistry = new NodeStakeRegistry();
            stakeRegistry.updateStakes(
                    Instant.ofEpochSecond(1),
                    NodeStakeUpdateTransactionBody.newBuilder()
                            .nodeStake(List.of(
                                    NodeStake.newBuilder().nodeId(0).stake(100).build(),
                                    NodeStake.newBuilder().nodeId(1).stake(200).build(),
                                    NodeStake.newBuilder().nodeId(2).stake(300).build(),
                                    NodeStake.newBuilder().nodeId(3).stake(400).build(),
                                    NodeStake.newBuilder().nodeId(4).stake(500).build()))
                            .build());

            // totalStake=1500, threshold=ceil(1500/3)=500. All 5 nodes sign → 1500 >= 500
            SignatureValidation validation = new SignatureValidation(abRegistry, stakeRegistry);
            assertDoesNotThrow(() -> validation.validate(toUnparsed(chain.getFirst()), 0));
        }

        @Test
        @DisplayName("insufficient stake fails with stake-weighted diagnostic")
        void insufficientStakeFailsStakeWeighted(@TempDir Path tempDir) throws Exception {
            List<Block> chain = TestBlockFactory.createValidChain(1);
            Block block = chain.getFirst();

            // Keep only node 0's signature (stake=100)
            List<BlockItem> items = new ArrayList<>();
            for (BlockItem item : block.items()) {
                if (item.hasBlockProof()) {
                    SignedRecordFileProof orig = item.blockProofOrThrow().signedRecordFileProofOrThrow();
                    List<RecordFileSignature> oneSig =
                            List.of(orig.recordFileSignatures().getFirst());
                    items.add(BlockItem.newBuilder()
                            .blockProof(BlockProof.newBuilder()
                                    .signedRecordFileProof(SignedRecordFileProof.newBuilder()
                                            .version(orig.version())
                                            .recordFileSignatures(oneSig)
                                            .build())
                                    .build())
                            .build());
                } else {
                    items.add(item);
                }
            }
            Block blockWith1Sig = new Block(items);

            TestBlockFactory.writeAddressBookHistory(tempDir);
            AddressBookRegistry abRegistry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));

            // Give node 0 low stake, others high — threshold won't be met by node 0 alone
            NodeStakeRegistry stakeRegistry = new NodeStakeRegistry();
            stakeRegistry.updateStakes(
                    Instant.ofEpochSecond(1),
                    NodeStakeUpdateTransactionBody.newBuilder()
                            .nodeStake(List.of(
                                    NodeStake.newBuilder().nodeId(0).stake(100).build(),
                                    NodeStake.newBuilder().nodeId(1).stake(200).build(),
                                    NodeStake.newBuilder().nodeId(2).stake(300).build(),
                                    NodeStake.newBuilder().nodeId(3).stake(400).build(),
                                    NodeStake.newBuilder().nodeId(4).stake(500).build()))
                            .build());

            // totalStake=1500, threshold=500. Node 0 stake=100 < 500
            SignatureValidation validation = new SignatureValidation(abRegistry, stakeRegistry);
            ValidationException ex =
                    assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(blockWith1Sig), 0));
            assertTrue(ex.getMessage().contains("mode=stake-weighted"), "Should show stake-weighted mode");
            assertTrue(ex.getMessage().contains("Insufficient validated stake"), "Should show stake diagnostic");
        }

        @Test
        @DisplayName("zero total stake falls back to equal-weight mode")
        void zeroTotalStakeFallsBackToEqualWeight(@TempDir Path tempDir) throws Exception {
            List<Block> chain = TestBlockFactory.createValidChain(1);
            TestBlockFactory.writeAddressBookHistory(tempDir);
            AddressBookRegistry abRegistry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));

            // All nodes have stake=0
            NodeStakeRegistry stakeRegistry = new NodeStakeRegistry();
            stakeRegistry.updateStakes(
                    Instant.ofEpochSecond(1),
                    NodeStakeUpdateTransactionBody.newBuilder()
                            .nodeStake(List.of(
                                    NodeStake.newBuilder().nodeId(0).stake(0).build(),
                                    NodeStake.newBuilder().nodeId(1).stake(0).build(),
                                    NodeStake.newBuilder().nodeId(2).stake(0).build(),
                                    NodeStake.newBuilder().nodeId(3).stake(0).build(),
                                    NodeStake.newBuilder().nodeId(4).stake(0).build()))
                            .build());

            // totalStake=0 → should fall back to equal-weight, not pass with threshold=0
            // 5 nodes all sign → passes equal-weight threshold of 2
            SignatureValidation validation = new SignatureValidation(abRegistry, stakeRegistry);
            assertDoesNotThrow(() -> validation.validate(toUnparsed(chain.getFirst()), 0));
        }
    }
}
