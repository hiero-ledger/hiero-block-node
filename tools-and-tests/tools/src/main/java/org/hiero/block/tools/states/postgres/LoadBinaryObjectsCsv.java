// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.postgres;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

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
     */
    public static Map<String, BinaryObjectCsvRow> loadBinaryObjectsMap(Path csvPath) {
        List<BinaryObjectCsvRow> binaryObjectRows = BinaryObjectCsvRow.loadBinaryObjects(csvPath);
        Map<String, BinaryObjectCsvRow> binaryObjectMap = new HashMap<>();
        for (BinaryObjectCsvRow row : binaryObjectRows) {
            binaryObjectMap.put(HexFormat.of().formatHex(row.hash()), row);
        }
        return binaryObjectMap;
    }
}
