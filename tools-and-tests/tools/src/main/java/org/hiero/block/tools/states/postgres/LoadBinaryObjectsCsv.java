// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.postgres;

import static org.hiero.block.tools.states.model.SignedState.load;

import com.hedera.hapi.node.base.CurrentAndNextFeeSchedule;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.transaction.ExchangeRateSet;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.hiero.block.tools.states.model.FCMap;
import org.hiero.block.tools.states.model.HGCAppState;
import org.hiero.block.tools.states.model.MapKey;
import org.hiero.block.tools.states.model.MapValue;
import org.hiero.block.tools.states.model.SignedState;
import org.hiero.block.tools.states.model.StorageKey;
import org.hiero.block.tools.states.model.StorageValue;

/**
 * Expand db dump tar.gz file then cd to directory
 * <pre><code>
 * docker run -it --rm -e POSTGRES_PASSWORD=password --volume "$(pwd):/data" postgres:10
 * docker exec -it &lt;id&gt; bash
 * psql -h localhost -U postgres
 * create database fcfs with owner postgres;
 * create role swirlds;
 * \q
 * pg_restore -h localhost -U postgres --format=d -d fcfs /data
 * psql -h localhost -U postgres -d fcfs
 * \COPY (SELECT id,ref_count,encode(hash, 'hex') AS hash_hex,file_oid,encode(lo_get(file_oid), 'hex') AS file_base64 FROM binary_objects) TO '/data/binary_objects.csv' WITH CSV HEADER
 * </code></pre>
 */
public class LoadBinaryObjectsCsv {
    public static long ADDRESS_FILE_ACCOUNT_NUM = 101;
    public static long NODE_DETAILS_FILE = 102;
    public static long FEE_FILE_ACCOUNT_NUM = 111;
    public static long EXCHANGE_RATE_FILE_ACCOUNT_NUM = 112;

    /**
     * Loads binary objects from a CSV file and returns a map where the key is the hex representation of the hash
     * and the value is the BinaryObjectRow.
     *
     * @param csvPath Path to the CSV file containing binary objects.
     * @return Map of binary objects keyed by their hash in hex format.
     * @throws Exception if an error occurs while reading the CSV file.
     */
    public static Map<String, BinaryObjectCsvRow> loadBinaryObjectsMap(Path csvPath) throws Exception {
        List<BinaryObjectCsvRow> binaryObjectRows = BinaryObjectCsvRow.loadBinaryObjects(csvPath);
        Map<String, BinaryObjectCsvRow> binaryObjectMap = new HashMap<>();
        for (BinaryObjectCsvRow row : binaryObjectRows) {
            binaryObjectMap.put(HexFormat.of().formatHex(row.hash()), row);
        }
        return binaryObjectMap;
    }

    public static void main(String[] args) throws Exception {
        Path csvPath = Path.of("mainnet-data/33485415/binary_objects.csv");
        final List<BinaryObjectCsvRow> binaryObjectRows = BinaryObjectCsvRow.loadBinaryObjects(csvPath);
        // Print the loaded rows
        //        for (BinaryObjectCsvRow row : binaryObjectRows) {
        //            System.out.println("\n\nID: " + row.id() + ", Hash: " + HexFormat.of().formatHex(row.hash()) +
        //                    ", File ID: " + row.fileId()+
        //                    ", Hash Size: " + row.hash().length +
        //                    ", File Contents Length: " + row.fileContents().length);
        //            System.out.println(new String(row.fileContents(), StandardCharsets.UTF_8));
        //        }

        // binary objects by hash map
        Map<String, BinaryObjectCsvRow> binaryObjectByHexHashMap = new HashMap<>();
        for (BinaryObjectCsvRow row : binaryObjectRows) {
            binaryObjectByHexHashMap.put(row.hexHash(), row);
        }

        final Path stateFile = Path.of("mainnet-data/33485415/SignedState.swh");
        SignedState signedState = load(stateFile);
        HGCAppState hgcAppState = signedState.state();
        FCMap<StorageKey, StorageValue> storageMap = hgcAppState.storageMap();
        // print storage map
        System.out.println("\n\nStorage Map:");
        Map<BlobType, List<Entry<StorageKey, StorageValue>>> blobTypeMap = storageMap.entrySet().stream()
                .collect(Collectors.groupingBy(entry -> entry.getKey().getBlobType(), Collectors.toList()));

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
                System.out.println("  0.0." + key.getId());
                System.out.println("      Key: " + key + ", Value: " + value);
                // try and find the binary object by hash
                BinaryObjectCsvRow binaryObject =
                        binaryObjectByHexHashMap.get(value.data().hash().hex());
                if (binaryObject != null) {
                    System.out.println("      Binary Object: " + binaryObject);
                } else {
                    System.out.println("      No binary object found for Hash ");
                }
                // check the computed hash
                //                System.out.println("      Computed Hash matches: " +
                // binaryObject.hexHash().equals(binaryObject.computeHexHash()));
                //                System.out.println("      File Size: " + binaryObject.fileContents().length);
                switch (blobType) {
                    //                    case FILE_METADATA -> {
                    //                        JFileInfo jFileInfo = JFileInfo.deserialize(binaryObject.fileContents());
                    //                        System.out.println("      File Meta: " + jFileInfo);
                    //                    }
                    //                    case FILE_DATA -> {
                    //                        System.out.println("      File Contents: " +
                    // dumpBinaryData(binaryObject.fileContents()));
                    //                    }
                    case CONTRACT_BYTECODE -> {
                        if (key.getId() == 13707) {
                            String hexContent = HexFormat.of().formatHex(binaryObject.fileContents());
                            System.out.println("      Matches: " + hexContent.startsWith(KNOWN_HEX_FOR_13707));
                            System.out.println("      File Contents: " + hexContent);
                            MapValue account = hgcAppState.accountMap().get(new MapKey(0, 0, 13707));
                            System.out.println("     account = " + account);
                        }
                    }
                }
            }
        }
        // dump system files
        //        dumpSystemFiles(storageMap, binaryObjectByHexHashMap);
    }

    public static String KNOWN_HEX_FOR_13707 =
            "60806040526004361061006c5763ffffffff7c01000000000000000000000000000000000000000000000000000000006000350416632e1a7d4d811461007e5780637682ff0a146100a85780638da5cb5b146100bf578063b69ef8a8146100fd578063b6b55f2514610112575b34801561007857600080fd5b50600080fd5b34801561008a57600080fd5b5061009660043561012a565b60408051918252519081900360200190f35b3480156100b457600080fd5b506100bd61019f565b005b3480156100cb57600080fd5b506100d46101c0565b6040805173ffffffffffffffffffffffffffffffffffffffff9092168252519081900360200190f35b34801561010957600080fd5b506100966101dc565b34801561011e57600080fd5b506100966004356101ef565b33600090815260208190526040812054821161018a5733600081815260208190526040808220805486900390555184156108fc0291859190818181858888f19350505050151561018a573360009081526020819052604090208054830190555b50503360009081526020819052604090205490565b6001805473ffffffffffffffffffffffffffffffffffffffff191633179055565b60015473ffffffffffffffffffff";

    public static void dumpSystemFiles(
            FCMap<StorageKey, StorageValue> storageMap, Map<String, BinaryObjectCsvRow> binaryObjectByHexHashMap)
            throws ParseException {
        // get system files
        System.out.println("=============================================================");
        System.out.println("System Files:");
        var feeFileStorageValue = storageMap.get(new StorageKey("/0/f" + FEE_FILE_ACCOUNT_NUM));
        BinaryObjectCsvRow feeFileBinObj =
                binaryObjectByHexHashMap.get(feeFileStorageValue.data().hash().hex());
        CurrentAndNextFeeSchedule currentAndNextFeeSchedule =
                CurrentAndNextFeeSchedule.PROTOBUF.parse(Bytes.wrap(feeFileBinObj.fileContents()));
        System.out.println("=== currentAndNextFeeSchedule ====");
        //        System.out.println(CurrentAndNextFeeSchedule.JSON.toJSON(currentAndNextFeeSchedule));
        var nodeAddressBookStorageValue = storageMap.get(new StorageKey("/0/f" + ADDRESS_FILE_ACCOUNT_NUM));
        BinaryObjectCsvRow nodeAddressBookBinObj = binaryObjectByHexHashMap.get(
                nodeAddressBookStorageValue.data().hash().hex());
        NodeAddressBook nodeAddressBook =
                NodeAddressBook.PROTOBUF.parse(Bytes.wrap(nodeAddressBookBinObj.fileContents()));
        System.out.println("=== nodeAddressBook ====");
        nodeAddressBook.nodeAddress().forEach(nodeAddress -> {
            System.out.println("    Node ID: " + nodeAddress.nodeId() + ", IP: "
                    + nodeAddress.ipAddress().asUtf8String() + ", Port: "
                    + nodeAddress.portno() + ", Memo: "
                    + nodeAddress.memo().asUtf8String());
        });
        // ExchangeRateSet
        var exchangeRateStorageValue = storageMap.get(new StorageKey("/0/f" + EXCHANGE_RATE_FILE_ACCOUNT_NUM));
        BinaryObjectCsvRow exchangeRateBinObj = binaryObjectByHexHashMap.get(
                exchangeRateStorageValue.data().hash().hex());
        ExchangeRateSet exchangeRateSet = ExchangeRateSet.PROTOBUF.parse(Bytes.wrap(exchangeRateBinObj.fileContents()));
        System.out.println("=== Exchange Rate ====");
        System.out.println(ExchangeRateSet.JSON.toJSON(exchangeRateSet));
        // NODE_DETAILS_FILE
        var nodeDetailsStorageValue = storageMap.get(new StorageKey("/0/f" + NODE_DETAILS_FILE));
        BinaryObjectCsvRow nodeDetailsBinObj = binaryObjectByHexHashMap.get(
                nodeDetailsStorageValue.data().hash().hex());
        NodeAddressBook nodeDetails = NodeAddressBook.PROTOBUF.parse(Bytes.wrap(nodeDetailsBinObj.fileContents()));
        System.out.println("=== Node Details ====");
        nodeDetails.nodeAddress().forEach(nodeAddress -> {
            System.out.println(
                    "    Memo: " + nodeAddress.memo().asUtf8String() + ", rsaPubKey : " + nodeAddress.rsaPubKey());
        });
    }

    public static String dumpBinaryData(byte[] data) {
        StringBuilder result = new StringBuilder();
        for (byte b : data) {
            int value = b & 0xFF; // Convert to unsigned
            if (value >= 32 && value <= 126) { // Printable ASCII range
                result.append((char) value);
            } else {
                result.append(String.format("\\x%02X", value)); // Represent as \xNN
            }
        }
        return result.toString();
    }
}
