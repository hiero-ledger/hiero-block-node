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
 * Converter class that performs a <b>lossy, one-way</b> transformation of a legacy {@link CompleteSavedState}
 * (composed of a {@link SignedState} and a Postgres binary-objects CSV export) into block stream
 * {@link BlockItem}s containing {@link StateChanges}. The output is intended to represent the initial state of the
 * network at block zero.
 *
 * <h2>Conversion is Lossy</h2>
 *
 * <p>This conversion is intentionally <b>not lossless</b>. The legacy saved state format contains data that either
 * has no equivalent in the modern block stream protobuf schema, was never meaningfully used, or is not relevant when
 * treating block zero as the genesis of the network. A round-trip (convert to block stream then back to saved state)
 * will <b>not</b> reproduce the original saved state.
 *
 * <h2>What is Converted</h2>
 *
 * <p>The following state is extracted and emitted as {@link StateChanges} block items, each tagged with the
 * consensus timestamp from the signed state:
 *
 * <ul>
 *   <li><b>Accounts</b> ({@link StateIdentifier#STATE_ID_ACCOUNTS}) &mdash; Every entry in the account FCMap is
 *       converted to a modern {@link com.hedera.hapi.node.state.token.Account} protobuf. The fields carried over are:
 *       {@code accountId}, {@code tinybarBalance}, {@code receiverSigRequired}, {@code key}, {@code autoRenewSeconds},
 *       {@code deleted}, {@code expirationSecond}, {@code memo}, and {@code smartContract}.</li>
 *   <li><b>Files</b> ({@link StateIdentifier#STATE_ID_FILES}) &mdash; File metadata ({@code FILE_METADATA} blob type)
 *       is paired with its corresponding file data ({@code FILE_DATA} blob type) to produce a single
 *       {@link com.hedera.hapi.node.state.file.File} per entity. Fields carried: {@code fileId}, {@code keys} (WACL),
 *       {@code contents}, {@code expirationSecond}, and {@code deleted}.</li>
 *   <li><b>Contract Bytecode</b> ({@link StateIdentifier#STATE_ID_BYTECODE}) &mdash; Each {@code CONTRACT_BYTECODE}
 *       storage entry is converted to a {@link com.hedera.hapi.node.state.contract.Bytecode} keyed by
 *       {@link ContractID}.</li>
 *   <li><b>Contract Storage Slots</b> ({@link StateIdentifier#STATE_ID_STORAGE}) &mdash; Each
 *       {@code CONTRACT_STORAGE} entry is deserialized into 32-byte key/value pairs and emitted as
 *       {@link com.hedera.hapi.node.state.contract.SlotKey}/{@link com.hedera.hapi.node.state.contract.SlotValue}
 *       state changes. Previous/next slot key linked-list pointers are derived from the ordering of pairs within each
 *       contract's storage.</li>
 * </ul>
 *
 * <h2>What is Intentionally Not Converted</h2>
 *
 * <h3>Account fields dropped</h3>
 * <ul>
 *   <li>{@code receiverThreshold} / {@code senderThreshold} &mdash; These fields do not exist in the modern Account
 *       protobuf. They only controlled whether extra transaction records were generated for the account holder and
 *       carry no balance or authorization significance.</li>
 *   <li>{@code proxyAccount} &mdash; Proxy staking was never activated on mainnet. The field was always effectively
 *       unused.</li>
 *   <li>{@code recordLinkedList} &mdash; A linked list of recently-executed transaction records cached on the account.
 *       Since block zero is treated as the genesis of the network, partial historical transaction records from before
 *       block zero are not meaningful and are discarded.</li>
 * </ul>
 *
 * <h3>Top-level / singleton state dropped</h3>
 * <ul>
 *   <li>{@code sequenceNum} (entity ID sequence counter), {@code exchangeRateSetWrapper} (current/next exchange
 *       rates), {@code lastHandleTxConsensusTime}, and the {@code addressBook} &mdash; In the modern block stream
 *       schema these are singleton state values. Singletons did not exist at the time of the legacy state format, so
 *       there is no block-zero equivalent to emit. These values are expected to be established by the network through
 *       normal operation after genesis.</li>
 * </ul>
 *
 * <h3>SignedState platform-level data dropped</h3>
 * <ul>
 *   <li>{@code round}, {@code events}, {@code sigSet}, {@code minGenInfo}, {@code hashEventsCons} &mdash; These are
 *       platform/consensus infrastructure fields (hashgraph events, signature collections, round metadata). They have
 *       no representation in the block stream state change model and are not relevant to application-level genesis
 *       state.</li>
 * </ul>
 *
 * <h3>Storage blob types dropped</h3>
 * <ul>
 *   <li>{@code SYSTEM_DELETED_ENTITY_EXPIRY} &mdash; Records of when system-deleted entities expire. This bookkeeping
 *       data is not needed at genesis.</li>
 * </ul>
 *
 * <h2>Output Structure</h2>
 *
 * <p>The method {@link #signedStateToStateChanges(CompleteSavedState)} returns a list of four {@link BlockItem}s,
 * each wrapping a {@link StateChanges} instance:
 * <ol>
 *   <li>Account state changes ({@code STATE_ID_ACCOUNTS})</li>
 *   <li>File state changes ({@code STATE_ID_FILES})</li>
 *   <li>Contract bytecode state changes ({@code STATE_ID_BYTECODE})</li>
 *   <li>Contract storage slot state changes ({@code STATE_ID_STORAGE})</li>
 * </ol>
 *
 * <h2>Input Requirements</h2>
 *
 * <p>Loading requires two files from the saved state directory:
 * <ul>
 *   <li>{@code SignedState.swh} (or {@code .swh.gz}) &mdash; The serialized signed state.</li>
 *   <li>{@code binary_objects.csv} (or {@code .csv.gz}) &mdash; A Postgres export of binary objects (file contents,
 *       bytecode, contract storage blobs) referenced by SHA-384 hash from the signed state's storage map.</li>
 * </ul>
 *
 * <p>Binary objects are matched to storage entries by their SHA-384 hash. If any referenced hash is missing from the
 * CSV, an {@link IllegalStateException} is thrown.
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
        // IMPORTANT: As all general state in block stream state is in singletons, and they did not exist at block zero.
        // We are not including any of the general top-level state fields in the block stream changes. For example,
        // `sequenceNum`,`exchangeRateSetWrapper` etc.
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
                    // IMPORTANT: the long receiverThreshold and long senderThreshold fields are ignored as they do not
                    // exist in the modern Account object we are converting to. They are only about giving user options
                    // to get extra records about their transactions.
                    // IMPORTANT: `recordLinkedList` is a list of records of recently executed transactions. This is not
                    // needed, as we are treating block zero as the start of the network, we can ignore partial
                    // information about what happened before the start of block zero.
                    // IMPORTANT: `proxyAccount` was never used, so we can ignore it.
                    // IMPORTANT: None of the above is critical data to retain. It does mean the conversion is not loss
                    // less!
                    Account account = Account.newBuilder()
                            .accountId(accountId)
                            .tinybarBalance(mapValue.balance())
                            .receiverSigRequired(mapValue.receiverSigRequired())
                            .key(CryptoUtils.convertKey(mapValue.accountKeys()))
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
            List<Entry<StorageKey, StorageValue>> entries = blobTypeMap.get(blobType).stream()
                    .sorted(Comparator.comparingLong(e -> e.getKey().getId()))
                    .toList();

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
                    case FILE_DATA -> {
                        // IMPORTANT: file data is handled together with file metadata, so we can ignore it here
                    }
                    case CONTRACT_BYTECODE -> {
                        final ContractID contractId =
                                new ContractID(0, 0, new OneOf<>(ContractOneOfType.CONTRACT_NUM, key.getId()));
                        final Bytes bytecode = Bytes.wrap(binaryObject.fileContents());
                        contractByteCodeStateChanges.add(StateChange.newBuilder()
                                .stateId(StateIdentifier.STATE_ID_BYTECODE.protoOrdinal())
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
                    case SYSTEM_DELETED_ENTITY_EXPIRY -> {
                        // IMPORTANT: ignored, we do not need to convert expired data
                    }
                }
            }
        }
        return blockItems;
    }
}
