// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.node.base.AccountAmount;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.TransferList;
import com.hedera.hapi.node.state.token.Account;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.HashAlgorithm;
import com.hedera.hapi.streams.HashObject;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.AddressBookHistory;
import org.hiero.block.internal.DatedNodeAddressBook;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.records.model.parsed.ParsedRecordFile;

/**
 * Test helper that generates synthetic blocks with real RSA signatures for testing
 * {@link ValidateBlocksCommand}.
 *
 * <p>Generates blocks with 5 test RSA-2048 key pairs for nodes 0-4 (accounts 3-7).
 * Each block contains a valid BlockHeader, RecordFileItem, BlockFooter, and BlockProof
 * with verifiable RSA-SHA384 signatures.
 */
public final class TestBlockFactory {

    /** 50 billion HBAR in tinybar. */
    private static final long FIFTY_BILLION = 5_000_000_000_000_000_000L;

    /** Number of test nodes. */
    private static final int NODE_COUNT = 5;

    /** HAPI version used for test blocks. */
    private static final SemanticVersion HAPI_VERSION =
            SemanticVersion.newBuilder().major(0).minor(50).patch(0).build();

    /** Test RSA-2048 key pairs for nodes 0..4. */
    private static final KeyPair[] KEY_PAIRS = new KeyPair[NODE_COUNT];

    /** Hex-encoded DER public keys for the address book. */
    private static final String[] PUB_KEY_HEX = new String[NODE_COUNT];

    /** The test address book. */
    private static final NodeAddressBook TEST_ADDRESS_BOOK;

    /** A dummy 48-byte hash used for running hashes. */
    private static final byte[] DUMMY_HASH = new byte[48];

    static {
        try {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            List<NodeAddress> nodes = new ArrayList<>();
            java.util.HexFormat hf = java.util.HexFormat.of();
            for (int i = 0; i < NODE_COUNT; i++) {
                KEY_PAIRS[i] = kpg.generateKeyPair();
                PUB_KEY_HEX[i] = hf.formatHex(KEY_PAIRS[i].getPublic().getEncoded());
                long accountNum = i + 3;
                nodes.add(NodeAddress.newBuilder()
                        .nodeId(i)
                        .memo(Bytes.wrap(("0.0." + accountNum).getBytes(StandardCharsets.UTF_8)))
                        .rsaPubKey(PUB_KEY_HEX[i])
                        .nodeAccountId(AccountID.newBuilder()
                                .shardNum(0)
                                .realmNum(0)
                                .accountNum(accountNum)
                                .build())
                        .build());
            }
            TEST_ADDRESS_BOOK = new NodeAddressBook(nodes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate test RSA keys", e);
        }
    }

    private TestBlockFactory() {}

    /** Returns the test address book. */
    public static NodeAddressBook getTestAddressBook() {
        return TEST_ADDRESS_BOOK;
    }

    /**
     * Writes addressBookHistory.json to the given directory.
     *
     * @param dir directory to write into
     * @throws IOException if writing fails
     */
    public static void writeAddressBookHistory(Path dir) throws IOException {
        Timestamp ts = Timestamp.newBuilder().seconds(0).nanos(0).build();
        DatedNodeAddressBook dated = new DatedNodeAddressBook(ts, TEST_ADDRESS_BOOK);
        AddressBookHistory history = new AddressBookHistory(List.of(dated));
        Path jsonFile = dir.resolve("addressBookHistory.json");
        try (var out = new WritableStreamingData(Files.newOutputStream(jsonFile))) {
            AddressBookHistory.JSON.write(history, out);
        }
    }

    /**
     * Creates a valid chain of blocks from genesis (block 0).
     *
     * @param count number of blocks to create
     * @return list of valid blocks with correct hashes and signatures
     */
    public static List<Block> createValidChain(int count) {
        List<Block> blocks = new ArrayList<>();
        StreamingHasher streamingHasher = new StreamingHasher();
        byte[] previousBlockHash = null;

        for (int i = 0; i < count; i++) {
            Instant blockTime = Instant.ofEpochSecond(1568411631L + i);
            Block block = createBlock(i, blockTime, previousBlockHash, streamingHasher, i == 0);
            blocks.add(block);
            previousBlockHash = BlockStreamBlockHasher.hashBlock(block);
            streamingHasher.addNodeByHash(previousBlockHash);
        }
        return blocks;
    }

    /**
     * Creates a single valid block.
     */
    private static Block createBlock(
            long blockNumber,
            Instant blockTime,
            byte[] previousBlockHash,
            StreamingHasher streamingHasher,
            boolean isGenesis) {
        List<BlockItem> items = new ArrayList<>();

        // 1. BlockHeader
        Timestamp consensusTs = Timestamp.newBuilder()
                .seconds(blockTime.getEpochSecond())
                .nanos(blockTime.getNano())
                .build();
        BlockHeader header = BlockHeader.newBuilder()
                .number(blockNumber)
                .hashAlgorithm(com.hedera.hapi.node.base.BlockHashAlgorithm.SHA2_384)
                .hapiProtoVersion(HAPI_VERSION)
                .blockTimestamp(consensusTs)
                .build();
        items.add(BlockItem.newBuilder().blockHeader(header).build());

        // 2. StateChanges for genesis (set 50B HBAR on account 2)
        if (isGenesis) {
            Account account = Account.newBuilder()
                    .accountId(AccountID.newBuilder()
                            .shardNum(0)
                            .realmNum(0)
                            .accountNum(2)
                            .build())
                    .tinybarBalance(FIFTY_BILLION)
                    .build();
            StateChange sc = StateChange.newBuilder()
                    .stateId(1) // STATE_ID_ACCOUNTS
                    .mapUpdate(MapUpdateChange.newBuilder()
                            .key(MapChangeKey.newBuilder()
                                    .accountIdKey(AccountID.newBuilder()
                                            .shardNum(0)
                                            .realmNum(0)
                                            .accountNum(2)
                                            .build()))
                            .value(MapChangeValue.newBuilder().accountValue(account)))
                    .build();
            StateChanges stateChanges = StateChanges.newBuilder()
                    .consensusTimestamp(consensusTs)
                    .stateChanges(List.of(sc))
                    .build();
            items.add(BlockItem.newBuilder().stateChanges(stateChanges).build());
        }

        // 3. RecordFileItem with minimal RecordStreamFile
        HashObject runningHash = HashObject.newBuilder()
                .algorithm(HashAlgorithm.SHA_384)
                .length(48)
                .hash(Bytes.wrap(DUMMY_HASH))
                .build();

        // Create a zero-net transfer (100 tinybar 2→3 and 3→2)
        TransferList transferList = TransferList.newBuilder()
                .accountAmounts(List.of(
                        AccountAmount.newBuilder()
                                .accountID(AccountID.newBuilder()
                                        .shardNum(0)
                                        .realmNum(0)
                                        .accountNum(2)
                                        .build())
                                .amount(-100)
                                .build(),
                        AccountAmount.newBuilder()
                                .accountID(AccountID.newBuilder()
                                        .shardNum(0)
                                        .realmNum(0)
                                        .accountNum(3)
                                        .build())
                                .amount(100)
                                .build()))
                .build();
        TransactionRecord txRecord = TransactionRecord.newBuilder()
                .consensusTimestamp(consensusTs)
                .transferList(transferList)
                .build();
        RecordStreamItem rsi = RecordStreamItem.newBuilder().record(txRecord).build();
        RecordStreamFile rsf = RecordStreamFile.newBuilder()
                .hapiProtoVersion(HAPI_VERSION)
                .startObjectRunningHash(runningHash)
                .endObjectRunningHash(runningHash)
                .recordStreamItems(List.of(rsi))
                .blockNumber(blockNumber)
                .build();
        RecordFileItem rfi = RecordFileItem.newBuilder()
                .creationTime(consensusTs)
                .recordFileContents(rsf)
                .build();
        items.add(BlockItem.newBuilder().recordFile(rfi).build());

        // 4. BlockFooter
        byte[] prevHash = (previousBlockHash != null) ? previousBlockHash : EMPTY_TREE_HASH;
        byte[] treeRoot = streamingHasher.computeRootHash();
        BlockFooter footer = BlockFooter.newBuilder()
                .previousBlockRootHash(Bytes.wrap(prevHash))
                .rootHashOfAllBlockHashesTree(Bytes.wrap(treeRoot))
                .startOfBlockStateRootHash(Bytes.wrap(EMPTY_TREE_HASH))
                .build();
        items.add(BlockItem.newBuilder().blockFooter(footer).build());

        // 5. BlockProof with RSA signatures
        // First compute the signed hash via ParsedRecordFile.parse()
        byte[] signedHash = ParsedRecordFile.parse(blockTime, 6, HAPI_VERSION, DUMMY_HASH, rsf)
                .signedHash();

        List<RecordFileSignature> sigs = new ArrayList<>();
        for (int n = 0; n < NODE_COUNT; n++) {
            byte[] sigBytes = rsaSign(KEY_PAIRS[n], signedHash);
            sigs.add(RecordFileSignature.newBuilder()
                    .nodeId(n)
                    .signaturesBytes(Bytes.wrap(sigBytes))
                    .build());
        }
        SignedRecordFileProof srf = SignedRecordFileProof.newBuilder()
                .version(6)
                .recordFileSignatures(sigs)
                .build();
        BlockProof proof = BlockProof.newBuilder().signedRecordFileProof(srf).build();
        items.add(BlockItem.newBuilder().blockProof(proof).build());

        return new Block(items);
    }

    /** RSA-SHA384 sign the given data with the private key. */
    private static byte[] rsaSign(KeyPair keyPair, byte[] data) {
        try {
            Signature sig = Signature.getInstance("SHA384withRSA");
            sig.initSign(keyPair.getPrivate());
            sig.update(data);
            return sig.sign();
        } catch (Exception e) {
            throw new RuntimeException("RSA signing failed", e);
        }
    }

    // ── Mutation methods: return a new block with one thing wrong ──

    /** Returns a copy with a corrupted previousBlockRootHash in the footer. */
    public static Block withBrokenPreviousHash(Block block) {
        return replaceFooter(block, footer -> BlockFooter.newBuilder()
                .previousBlockRootHash(Bytes.wrap(new byte[48]))
                .rootHashOfAllBlockHashesTree(footer.rootHashOfAllBlockHashesTree())
                .startOfBlockStateRootHash(footer.startOfBlockStateRootHash())
                .build());
    }

    /** Returns a copy with a corrupted rootHashOfAllBlockHashesTree in the footer. */
    public static Block withBrokenTreeRoot(Block block) {
        return replaceFooter(block, footer -> BlockFooter.newBuilder()
                .previousBlockRootHash(footer.previousBlockRootHash())
                .rootHashOfAllBlockHashesTree(Bytes.wrap(new byte[48]))
                .startOfBlockStateRootHash(footer.startOfBlockStateRootHash())
                .build());
    }

    /** Returns a copy with only 1 signature (below the threshold of 2). */
    public static Block withInsufficientSignatures(Block block) {
        List<BlockItem> items = new ArrayList<>();
        for (BlockItem item : block.items()) {
            if (item.hasBlockProof()) {
                SignedRecordFileProof orig = item.blockProofOrThrow().signedRecordFileProofOrThrow();
                SignedRecordFileProof newProof = SignedRecordFileProof.newBuilder()
                        .version(orig.version())
                        .recordFileSignatures(orig.recordFileSignatures().subList(0, 1))
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
        return new Block(items);
    }

    /** Returns a copy with the first signature's bytes flipped. */
    public static Block withCorruptSignature(Block block) {
        List<BlockItem> items = new ArrayList<>();
        for (BlockItem item : block.items()) {
            if (item.hasBlockProof()) {
                SignedRecordFileProof orig = item.blockProofOrThrow().signedRecordFileProofOrThrow();
                List<RecordFileSignature> sigs = new ArrayList<>(orig.recordFileSignatures());
                RecordFileSignature first = sigs.getFirst();
                byte[] flipped = first.signaturesBytes().toByteArray();
                flipped[0] = (byte) (flipped[0] ^ 0xFF);
                sigs.set(
                        0,
                        RecordFileSignature.newBuilder()
                                .nodeId(first.nodeId())
                                .signaturesBytes(Bytes.wrap(flipped))
                                .build());
                SignedRecordFileProof newProof = SignedRecordFileProof.newBuilder()
                        .version(orig.version())
                        .recordFileSignatures(sigs)
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
        return new Block(items);
    }

    /** Returns a copy with the BlockFooter removed. */
    public static Block withMissingFooter(Block block) {
        return filterItems(block, item -> !item.hasBlockFooter());
    }

    /** Returns a copy with the BlockHeader removed. */
    public static Block withMissingHeader(Block block) {
        return filterItems(block, item -> !item.hasBlockHeader());
    }

    /** Returns a copy with the RecordFile removed. */
    public static Block withMissingRecordFile(Block block) {
        return filterItems(block, item -> !item.hasRecordFile());
    }

    /** Returns a copy with the BlockProof removed. */
    public static Block withMissingProof(Block block) {
        return filterItems(block, item -> !item.hasBlockProof());
    }

    /** Returns a copy with a duplicate BlockHeader inserted. */
    public static Block withDuplicateHeader(Block block) {
        List<BlockItem> items = new ArrayList<>(block.items());
        // Insert a second header after the first
        items.add(1, items.getFirst());
        return new Block(items);
    }

    /** Returns a copy with RecordFile and BlockFooter swapped. */
    public static Block withItemsOutOfOrder(Block block) {
        List<BlockItem> items = new ArrayList<>(block.items());
        // Find and swap record file and footer
        int rfIdx = -1, ftIdx = -1;
        for (int i = 0; i < items.size(); i++) {
            if (items.get(i).hasRecordFile()) rfIdx = i;
            if (items.get(i).hasBlockFooter()) ftIdx = i;
        }
        if (rfIdx >= 0 && ftIdx >= 0) {
            BlockItem tmp = items.get(rfIdx);
            items.set(rfIdx, items.get(ftIdx));
            items.set(ftIdx, tmp);
        }
        return new Block(items);
    }

    /** Returns a copy with an unbalanced HBAR transfer that breaks 50B supply. */
    public static Block withExtraHbar(Block block, long amount) {
        List<BlockItem> items = new ArrayList<>();
        for (BlockItem item : block.items()) {
            if (item.hasRecordFile()) {
                RecordFileItem rfi = item.recordFileOrThrow();
                RecordStreamFile rsf = rfi.recordFileContentsOrThrow();
                List<RecordStreamItem> rsis = new ArrayList<>(rsf.recordStreamItems());
                // Add an unbalanced transfer
                TransferList tl = TransferList.newBuilder()
                        .accountAmounts(List.of(AccountAmount.newBuilder()
                                .accountID(AccountID.newBuilder()
                                        .shardNum(0)
                                        .realmNum(0)
                                        .accountNum(2)
                                        .build())
                                .amount(amount)
                                .build()))
                        .build();
                TransactionRecord tr = TransactionRecord.newBuilder()
                        .consensusTimestamp(rfi.creationTime())
                        .transferList(tl)
                        .build();
                rsis.add(RecordStreamItem.newBuilder().record(tr).build());
                RecordStreamFile newRsf = RecordStreamFile.newBuilder()
                        .hapiProtoVersion(rsf.hapiProtoVersion())
                        .startObjectRunningHash(rsf.startObjectRunningHash())
                        .endObjectRunningHash(rsf.endObjectRunningHash())
                        .recordStreamItems(rsis)
                        .blockNumber(rsf.blockNumber())
                        .build();
                items.add(BlockItem.newBuilder()
                        .recordFile(RecordFileItem.newBuilder()
                                .creationTime(rfi.creationTime())
                                .recordFileContents(newRsf)
                                .build())
                        .build());
            } else {
                items.add(item);
            }
        }
        return new Block(items);
    }

    // ── Helper methods ──

    @FunctionalInterface
    private interface FooterTransform {
        BlockFooter apply(BlockFooter footer);
    }

    private static Block replaceFooter(Block block, FooterTransform transform) {
        List<BlockItem> items = new ArrayList<>();
        for (BlockItem item : block.items()) {
            if (item.hasBlockFooter()) {
                items.add(BlockItem.newBuilder()
                        .blockFooter(transform.apply(item.blockFooterOrThrow()))
                        .build());
            } else {
                items.add(item);
            }
        }
        return new Block(items);
    }

    private static Block filterItems(Block block, java.util.function.Predicate<BlockItem> predicate) {
        List<BlockItem> items = block.items().stream().filter(predicate).toList();
        return new Block(items);
    }
}
