package org.hiero.block.tools.states;


import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.block.stream.output.StateIdentifier;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.AccountID.AccountOneOfType;
import com.hedera.hapi.node.base.ContractID;
import com.hedera.hapi.node.base.ContractID.ContractOneOfType;
import com.hedera.hapi.node.base.FileID;
import com.hedera.hapi.node.base.Key;
import com.hedera.hapi.node.base.KeyList;
import com.hedera.hapi.node.base.ThresholdKey;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.contract.Bytecode;
import com.hedera.hapi.node.state.contract.SlotKey;
import com.hedera.hapi.node.state.contract.SlotValue;
import com.hedera.hapi.node.state.file.File;
import com.hedera.hapi.node.state.token.Account;
import org.hiero.block.tools.states.SmartContracts;
import org.hiero.block.tools.states.SmartContracts.DataWordPair;
import org.hiero.block.tools.states.model.FCMap;
import org.hiero.block.tools.states.model.HGCAppState;
import org.hiero.block.tools.states.model.JFileInfo;
import org.hiero.block.tools.states.model.JKey;
import org.hiero.block.tools.states.model.JKey.JKeyList;
import org.hiero.block.tools.states.model.MapKey;
import org.hiero.block.tools.states.model.MapValue;
import org.hiero.block.tools.states.model.SignedState;
import org.hiero.block.tools.states.model.StorageKey;
import org.hiero.block.tools.states.model.StorageValue;
import org.hiero.block.tools.states.postgres.BinaryObjectCsvRow;
import org.hiero.block.tools.states.postgres.BlobType;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.BufferedOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 *  1568411616.448357000 first transaction in block zero
 *  1568331900.310557000 round 33312259 saved state
 */
public class BlockStreamWriter {

    public static void writeBlockStream(String fileName, SignedState signedState,
            Map<String, BinaryObjectCsvRow> binaryObjectByHexHashMap) {
        final HGCAppState state = signedState.state();
        final FCMap<StorageKey, StorageValue> storageMap = state.storageMap();
        final Timestamp consensusTimestamp = new Timestamp(signedState.consensusTimestamp().getEpochSecond(),
                signedState.consensusTimestamp().getNano());
        // Implementation for writing the block stream to a file
        // This is a placeholder for the actual implementation
        System.out.println("Writing block stream to " + fileName);
        // list to collect block items into
        final List<BlockItem> blockItems = new ArrayList<>();
        // start with converting accounts into state changes
        List<StateChange> accountStateChanges = state.accountMap().entrySet().stream()
                .map(entry -> {
                    MapKey key = entry.getKey();
                    MapValue mapValue = entry.getValue();
                    AccountID accountId = new AccountID(key.shardId(), key.realmId(),
                            new OneOf<>(AccountOneOfType.ACCOUNT_NUM, key.accountId()));
                    Account account = Account.newBuilder()
                            .accountId(accountId)
                            .tinybarBalance(mapValue.balance())
                            // TODO what are long receiverThreshold and long senderThreshold
                            .receiverSigRequired(mapValue.receiverSigRequired())
                            .key(convertKey(mapValue.accountKeys()))
                            // TODO: Handle proxyAccount if needed
                            .autoRenewSeconds(mapValue.autoRenewPeriod())
                            .deleted(mapValue.deleted())
                            .expirationSecond(mapValue.expirationTime())
                            .memo(mapValue.memo())
                            .smartContract(mapValue.isSmartContract())
                            .build();
                    return StateChange.newBuilder()
                            .stateId(StateIdentifier.STATE_ID_ACCOUNTS.protoOrdinal())
                            .mapUpdate(
                                    MapUpdateChange.newBuilder()
                                            .key(MapChangeKey.newBuilder().accountIdKey(accountId))
                                            .value(MapChangeValue.newBuilder().accountValue(account)))
                            .build();
                })
                .toList();
        blockItems.add(new BlockItem(new OneOf<>(ItemOneOfType.STATE_CHANGES,
                new StateChanges(consensusTimestamp,accountStateChanges))));
        System.out.println("   Converted " + accountStateChanges.size() +
                " accounts into state changes for block stream");
        // ============================================================================================================
        // Next is storage contents
        List<StateChange> filesStateChanges = new ArrayList<>();
        List<StateChange> contractByteCodeStateChanges = new ArrayList<>();
        List<StateChange> contractByteDataStateChanges = new ArrayList<>();
        blockItems.add(new BlockItem(new OneOf<>(ItemOneOfType.STATE_CHANGES,
                new StateChanges(consensusTimestamp,filesStateChanges))));
        blockItems.add(new BlockItem(new OneOf<>(ItemOneOfType.STATE_CHANGES,
                new StateChanges(consensusTimestamp,contractByteCodeStateChanges))));
        blockItems.add(new BlockItem(new OneOf<>(ItemOneOfType.STATE_CHANGES,
                new StateChanges(consensusTimestamp,contractByteDataStateChanges))));
        // first group storage entries by blob type
        Map<BlobType, List<Entry<StorageKey, StorageValue>>> blobTypeMap =
                storageMap.entrySet().stream()
                        .collect(Collectors.groupingBy(
                                entry -> entry.getKey().getBlobType(),
                                Collectors.toList()));
        // now loop through each blob type and convert the entries
        for(BlobType blobType : blobTypeMap.keySet()) {
            System.out.println("=============================================================");
            System.out.println("Blob Type: " + blobType);
            List<Entry<StorageKey, StorageValue>> entries = blobTypeMap.get(blobType).stream()
                    .sorted(Comparator.comparingLong(e -> e.getKey().getId()))
                    .toList();
            System.out.println("   Number of entries: " + entries.size());

            for (Entry<StorageKey, StorageValue> entry : entries) {
                StorageKey key = entry.getKey();
                StorageValue value = entry.getValue();
                System.out.println("  0.0." + key.getId());
                System.out.println("      Key: " + key + ", Value: " + value);
                // try and find the binary object by hash
                BinaryObjectCsvRow binaryObject = binaryObjectByHexHashMap.get(value.data().hash().hex());
                if (binaryObject == null)  throw new IllegalStateException(
                        "Binary object not found for entity: "+key.getId()+" hash: " + value.data().hash().hex());
                switch(blobType) {
                    case FILE_METADATA -> {
                        JFileInfo jFileInfo = JFileInfo.deserialize(binaryObject.fileContents());
                        // find the matching file data
                        StorageValue fileDataStorageValue = storageMap.get(new StorageKey("/0/f"+key.getId()));
                        BinaryObjectCsvRow fileDataBinaryObject = binaryObjectByHexHashMap.get(fileDataStorageValue.data().hash().hex());
                        Bytes fileContents = Bytes.wrap(fileDataBinaryObject.fileContents());
                        // create a file ID and add to state changes
                        FileID fileId = new FileID(0, 0, key.getId());
                        filesStateChanges.add(StateChange.newBuilder()
                                .stateId(StateIdentifier.STATE_ID_FILES.protoOrdinal())
                                .mapUpdate(MapUpdateChange.newBuilder()
                                        .key(MapChangeKey.newBuilder().fileIdKey(fileId))
                                        .value(MapChangeValue.newBuilder().fileValue(
                                                File.newBuilder()
                                                        .fileId(fileId)
                                                        .keys(new KeyList(List.of(convertKey(jFileInfo.wacl()))))
                                                        .contents(fileContents)
                                                        .expirationSecond(jFileInfo.expirationTimeInSec())
                                                        .deleted(jFileInfo.deleted())
                                        ))
                                ).build());
                    }
                    case CONTRACT_BYTECODE -> {
                        final ContractID contractId = new ContractID(0, 0,
                                new OneOf<>(ContractOneOfType.CONTRACT_NUM, key.getId()));
                        final Bytes bytecode = Bytes.wrap(binaryObject.fileContents());
                        filesStateChanges.add(StateChange.newBuilder()
                                .stateId(StateIdentifier.STATE_ID_FILES.protoOrdinal())
                                .mapUpdate(MapUpdateChange.newBuilder()
                                        .key(MapChangeKey.newBuilder().contractIdKey(contractId))
                                        .value(MapChangeValue.newBuilder().bytecodeValue(
                                                new Bytecode(bytecode)))
                                ).build());
                    }
                    case CONTRACT_STORAGE -> {
                        // create a storage key from the storage key
                        final ContractID contractId = new ContractID(0, 0,
                                new OneOf<>(ContractOneOfType.CONTRACT_NUM, key.getId()));
                        // parse pairs from the binary object
                        List<DataWordPair> pairs = SmartContracts.deserializeKeyValuePairs(binaryObject.fileContents());
                        // convert pairs to ContractSlot objects
                        List<ContractSlot> slotEntries = pairs.stream()
                                .map(pair -> new ContractSlot(
                                        new SlotKey(contractId, Bytes.wrap(pair.key().data())),
                                        Bytes.wrap(pair.value().data())
                                ) )
                                .toList();
                        for (int i = 0; i < pairs.size(); i++) {
                            DataWordPair previousPair = i > 0 ? pairs.get(i-1) : null;
                            DataWordPair currentPair = pairs.get(i);
                            DataWordPair nextPair = i < pairs.size() - 1 ? pairs.get(i+1) : null;
                            SlotKey slotKey =  new SlotKey(contractId, Bytes.wrap(currentPair.key().data()));
                            SlotValue slotValue = new SlotValue(
                                    Bytes.wrap(currentPair.value().data()),
                                    previousPair != null ? Bytes.wrap(previousPair.key().data()) : null,
                                    nextPair != null ? Bytes.wrap(nextPair.key().data()) : null
                            );

                            // create a state change for the contract storage
                            StateChange stateChange = StateChange.newBuilder()
                                    .stateId(StateIdentifier.STATE_ID_CONTRACT_STORAGE.protoOrdinal())
                                    .mapUpdate(MapUpdateChange.newBuilder()
                                            .key(MapChangeKey.newBuilder().slotKeyKey(slotKey))
                                            .value(MapChangeValue.newBuilder().slotValueValue(slotValue)))
                                    .build();
                            contractByteDataStateChanges.add(stateChange);
                        }
                    }
                }
            }
        }


        // create the block object with the collected block items
        Block block = new Block(blockItems);
        // write protobuf binary file
        Path protobufFilePath = Path.of(fileName + ".blk");
        try (WritableStreamingData out = new WritableStreamingData(new BufferedOutputStream(
                Files.newOutputStream(protobufFilePath, StandardOpenOption.CREATE), 8 * 1024 * 1024))) {
            Block.PROTOBUF.write(block, out);
        } catch (Exception e) {
            System.err.println("Error writing block stream: " + e.getMessage());
            e.printStackTrace();
        }
        // write JSON file
        Path jsonFilePath = Path.of(fileName + ".json");
        try (WritableStreamingData out = new WritableStreamingData(new BufferedOutputStream(
                Files.newOutputStream(jsonFilePath, StandardOpenOption.CREATE), 8 * 1024 * 1024))) {
            Block.JSON.write(block, out);
        } catch (Exception e) {
            System.err.println("Error writing block stream JSON: " + e.getMessage());
            e.printStackTrace();
        }
    }

    static <K extends JKey> Key convertKey(K jKey) {
        return switch (jKey) {
            case JKey.JThresholdKey thresholdKey -> Key.newBuilder().thresholdKey(
                    new ThresholdKey(thresholdKey.getThreshold(), convertKeyList(thresholdKey.getKeys()))
                ).build();
            case JKey.JEd25519Key ed25519Key ->  Key.newBuilder().ed25519(
                    Bytes.wrap(ed25519Key.getEd25519())
                ).build();
            case JKey.JECDSA_384Key ed384Key -> Key.newBuilder().ecdsa384(
                    Bytes.wrap(ed384Key.getECDSA384())
                ).build();
            case JKey.JRSA_3072Key jrsa3072Key -> Key.newBuilder().rsa3072(
                    Bytes.wrap(jrsa3072Key.getRSA3072())
                ).build();
            case JKey.JContractIDKey contractIDKey -> Key.newBuilder().contractID(
                    new ContractID(
                        contractIDKey.getShardNum(),
                        contractIDKey.getRealmNum(),
                        new OneOf<>(ContractOneOfType.CONTRACT_NUM, contractIDKey.getContractNum())
                    )
                ).build();
            case JKeyList keyList -> Key.newBuilder().keyList(
                    convertKeyList(keyList)
                ).build();
            default -> throw new IllegalArgumentException("Unsupported JKey type: " + jKey.getClass().getSimpleName());
        };
    }

    static KeyList convertKeyList(JKeyList jKeyList) {
        return new KeyList(jKeyList.getKeysList().stream().map(BlockStreamWriter::convertKey).toList());
    }


    public static void printLargestAccounts(TreeMap<Long, AccountID>  accountBalances) {
        // print first 100 largest account balances
        System.out.println("\n=== Largest account balances ===============================================================");
        accountBalances.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e2.getKey(), e1.getKey())) // Sort by balance descending
                .limit(100) // Limit to top 100
                .forEach(entry -> {
                    System.out.printf("     Account ID: %s, Balance: %d%n", entry.getValue(), entry.getKey());
                });
    }

    public record ContractSlot(SlotKey slotKey, Bytes value) {}
}
