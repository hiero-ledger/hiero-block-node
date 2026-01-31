// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states;

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
import com.hedera.hapi.node.base.KeyList;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.contract.Bytecode;
import com.hedera.hapi.node.state.contract.SlotKey;
import com.hedera.hapi.node.state.contract.SlotValue;
import com.hedera.hapi.node.state.file.File;
import com.hedera.hapi.node.state.token.Account;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.hiero.block.tools.states.model.CompleteSavedState;
import org.hiero.block.tools.states.model.FCMap;
import org.hiero.block.tools.states.model.HGCAppState;
import org.hiero.block.tools.states.model.JFileInfo;
import org.hiero.block.tools.states.model.MapKey;
import org.hiero.block.tools.states.model.MapValue;
import org.hiero.block.tools.states.model.SignedState;
import org.hiero.block.tools.states.model.StorageKey;
import org.hiero.block.tools.states.model.StorageValue;
import org.hiero.block.tools.states.postgres.BinaryObjectCsvRow;
import org.hiero.block.tools.states.postgres.BlobType;
import org.hiero.block.tools.states.postgres.SmartContractKVPairs;
import org.hiero.block.tools.states.postgres.SmartContractKVPairs.DataWordPair;
import org.hiero.block.tools.states.utils.CryptoUtils;

/**
 *  Converter class to transform a SavedState (SignedState + binary objects) into block stream items
 */
public class SavedStateConverter {

    /**
     * Convert a saved state located in the given resource directory path into a list of BlockItems
     * representing state changes
     *
     * @param resourceDirPath the resource directory path containing the saved state files
     * @return complete saved state
     */
    public static CompleteSavedState loadState(String resourceDirPath) {
        final URL signedStateUrl = SavedStateConverter.class.getResource(resourceDirPath + "/SignedState.swh.gz");
        if (signedStateUrl == null) {
            throw new IllegalArgumentException(
                    "Signed state resource not found at path: " + resourceDirPath + "/SignedState.swh.gz");
        }
        final URL binCsvUrl = SavedStateConverter.class.getResource(resourceDirPath + "/binary_objects.csv.gz");
        if (binCsvUrl == null) {
            throw new IllegalArgumentException(
                    "Binary objects CSV resource not found at path: " + resourceDirPath + "/binary_objects.csv.gz");
        }
        SignedState signedState;
        try {
            signedState = SignedState.load(signedStateUrl);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load signed state from URL: " + signedStateUrl, e);
        }
        // load binary objects, try compressed first
        Map<String, BinaryObjectCsvRow> binaryObjectByHexHashMap = BinaryObjectCsvRow.loadBinaryObjectsMap(binCsvUrl);
        // return complete saved state
        return new CompleteSavedState(signedState, binaryObjectByHexHashMap);
    }

    /**
     * Convert a saved state located in the given directory path into a list of BlockItems
     * representing state changes
     *
     * @param savedStateDir the directory path containing the saved state files
     * @return complete saved state
     */
    public static CompleteSavedState loadState(Path savedStateDir) {
        SignedState signedState;
        // load signed state, try compressed first
        final Path compressedStateFile = savedStateDir.resolve("SignedState.swh.gz");
        final Path stateFile =
                Files.exists(compressedStateFile) ? compressedStateFile : savedStateDir.resolve("SignedState.swh");
        try {
            signedState = SignedState.load(stateFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load signed state from file: " + compressedStateFile, e);
        }
        // load binary objects, try compressed first
        Path compressedBinaryObjectsCsvFile = savedStateDir.resolve("binary_objects.csv.gz");
        Path binaryObjectsCsvFile = Files.exists(compressedBinaryObjectsCsvFile)
                ? compressedBinaryObjectsCsvFile
                : savedStateDir.resolve("binary_objects.csv");
        Map<String, BinaryObjectCsvRow> binaryObjectByHexHashMap =
                BinaryObjectCsvRow.loadBinaryObjectsMap(binaryObjectsCsvFile);
        // return complete saved state
        return new CompleteSavedState(signedState, binaryObjectByHexHashMap);
    }

    /**
     * Convert a SignedState into a list of BlockItems representing state changes
     *
     * @param completeSavedState the signed state to convert
     * @return list of BlockItems representing state changes
     */
    public static List<BlockItem> signedStateToStateChanges(CompleteSavedState completeSavedState) {
        final SignedState signedState = completeSavedState.signedState();
        final Map<String, BinaryObjectCsvRow> binaryObjectByHexHashMap = completeSavedState.binaryObjectByHexHashMap();
        final HGCAppState state = signedState.state();
        final FCMap<StorageKey, StorageValue> storageMap = state.storageMap();
        final Timestamp consensusTimestamp = new Timestamp(
                signedState.consensusTimestamp().getEpochSecond(),
                signedState.consensusTimestamp().getNano());
        // list to collect block items into
        final List<BlockItem> blockItems = new ArrayList<>();
        // start with converting accounts into state changes
        List<StateChange> accountStateChanges = state.accountMap().entrySet().stream()
                .map(entry -> {
                    MapKey key = entry.getKey();
                    MapValue mapValue = entry.getValue();
                    AccountID accountId = new AccountID(
                            key.shardId(), key.realmId(), new OneOf<>(AccountOneOfType.ACCOUNT_NUM, key.accountId()));
                    Account account = Account.newBuilder()
                            .accountId(accountId)
                            .tinybarBalance(mapValue.balance())
                            // TODO what are long receiverThreshold and long senderThreshold
                            .receiverSigRequired(mapValue.receiverSigRequired())
                            .key(CryptoUtils.convertKey(mapValue.accountKeys()))
                            // TODO: Handle proxyAccount if needed
                            .autoRenewSeconds(mapValue.autoRenewPeriod())
                            .deleted(mapValue.deleted())
                            .expirationSecond(mapValue.expirationTime())
                            .memo(mapValue.memo())
                            .smartContract(mapValue.isSmartContract())
                            .build();
                    return StateChange.newBuilder()
                            .stateId(StateIdentifier.STATE_ID_ACCOUNTS.protoOrdinal())
                            .mapUpdate(MapUpdateChange.newBuilder()
                                    .key(MapChangeKey.newBuilder().accountIdKey(accountId))
                                    .value(MapChangeValue.newBuilder().accountValue(account)))
                            .build();
                })
                .toList();
        blockItems.add(new BlockItem(
                new OneOf<>(ItemOneOfType.STATE_CHANGES, new StateChanges(consensusTimestamp, accountStateChanges))));
        System.out.println(
                "   Converted " + accountStateChanges.size() + " accounts into state changes for block stream");
        // ============================================================================================================
        // Next is storage contents
        List<StateChange> filesStateChanges = new ArrayList<>();
        List<StateChange> contractByteCodeStateChanges = new ArrayList<>();
        List<StateChange> contractByteDataStateChanges = new ArrayList<>();
        blockItems.add(new BlockItem(
                new OneOf<>(ItemOneOfType.STATE_CHANGES, new StateChanges(consensusTimestamp, filesStateChanges))));
        blockItems.add(new BlockItem(new OneOf<>(
                ItemOneOfType.STATE_CHANGES, new StateChanges(consensusTimestamp, contractByteCodeStateChanges))));
        blockItems.add(new BlockItem(new OneOf<>(
                ItemOneOfType.STATE_CHANGES, new StateChanges(consensusTimestamp, contractByteDataStateChanges))));
        // first group storage entries by blob type
        Map<BlobType, List<Entry<StorageKey, StorageValue>>> blobTypeMap = storageMap.entrySet().stream()
                .collect(Collectors.groupingBy(entry -> entry.getKey().getBlobType(), Collectors.toList()));
        // now loop through each blob type and convert the entries
        for (BlobType blobType : blobTypeMap.keySet()) {
            System.out.println("=============================================================");
            System.out.println("Blob Type: " + blobType);
            List<Entry<StorageKey, StorageValue>> entries = blobTypeMap.get(blobType).stream()
                    .sorted(Comparator.comparingLong(e -> e.getKey().getId()))
                    .toList();
            System.out.println("   Number of entries: " + entries.size());

            for (Entry<StorageKey, StorageValue> entry : entries) {
                StorageKey key = entry.getKey();
                StorageValue value = entry.getValue();
                // try and find the binary object by hash
                BinaryObjectCsvRow binaryObject =
                        binaryObjectByHexHashMap.get(value.data().hash().hex());
                if (binaryObject == null)
                    throw new IllegalStateException("Binary object not found for entity: " + key.getId() + " hash: "
                            + value.data().hash().hex());
                switch (blobType) {
                    case FILE_METADATA -> {
                        JFileInfo jFileInfo = JFileInfo.deserialize(binaryObject.fileContents());
                        // find the matching file data
                        StorageValue fileDataStorageValue = storageMap.get(new StorageKey("/0/f" + key.getId()));
                        BinaryObjectCsvRow fileDataBinaryObject = binaryObjectByHexHashMap.get(
                                fileDataStorageValue.data().hash().hex());
                        Bytes fileContents = Bytes.wrap(fileDataBinaryObject.fileContents());
                        // create a file ID and add to state changes
                        FileID fileId = new FileID(0, 0, key.getId());
                        filesStateChanges.add(StateChange.newBuilder()
                                .stateId(StateIdentifier.STATE_ID_FILES.protoOrdinal())
                                .mapUpdate(MapUpdateChange.newBuilder()
                                        .key(MapChangeKey.newBuilder().fileIdKey(fileId))
                                        .value(MapChangeValue.newBuilder()
                                                .fileValue(File.newBuilder()
                                                        .fileId(fileId)
                                                        .keys(new KeyList(
                                                                List.of(CryptoUtils.convertKey(jFileInfo.wacl()))))
                                                        .contents(fileContents)
                                                        .expirationSecond(jFileInfo.expirationTimeInSec())
                                                        .deleted(jFileInfo.deleted()))))
                                .build());
                    }
                    case CONTRACT_BYTECODE -> {
                        final ContractID contractId =
                                new ContractID(0, 0, new OneOf<>(ContractOneOfType.CONTRACT_NUM, key.getId()));
                        final Bytes bytecode = Bytes.wrap(binaryObject.fileContents());
                        filesStateChanges.add(StateChange.newBuilder()
                                .stateId(StateIdentifier.STATE_ID_FILES.protoOrdinal())
                                .mapUpdate(MapUpdateChange.newBuilder()
                                        .key(MapChangeKey.newBuilder().contractIdKey(contractId))
                                        .value(MapChangeValue.newBuilder().bytecodeValue(new Bytecode(bytecode))))
                                .build());
                    }
                    case CONTRACT_STORAGE -> {
                        // create a storage key from the storage key
                        final ContractID contractId =
                                new ContractID(0, 0, new OneOf<>(ContractOneOfType.CONTRACT_NUM, key.getId()));
                        // parse pairs from the binary object
                        List<DataWordPair> pairs =
                                SmartContractKVPairs.deserializeKeyValuePairs(binaryObject.fileContents());
                        // convert pairs to ContractSlot objects
                        for (int i = 0; i < pairs.size(); i++) {
                            DataWordPair previousPair = i > 0 ? pairs.get(i - 1) : null;
                            DataWordPair currentPair = pairs.get(i);
                            DataWordPair nextPair = i < pairs.size() - 1 ? pairs.get(i + 1) : null;
                            SlotKey slotKey = new SlotKey(
                                    contractId, Bytes.wrap(currentPair.key().data()));
                            SlotValue slotValue = new SlotValue(
                                    Bytes.wrap(currentPair.value().data()),
                                    previousPair != null
                                            ? Bytes.wrap(previousPair.key().data())
                                            : null,
                                    nextPair != null ? Bytes.wrap(nextPair.key().data()) : null);

                            // create a state change for the contract storage
                            StateChange stateChange = StateChange.newBuilder()
                                    .stateId(StateIdentifier.STATE_ID_STORAGE.protoOrdinal())
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
        return blockItems;
    }
}
