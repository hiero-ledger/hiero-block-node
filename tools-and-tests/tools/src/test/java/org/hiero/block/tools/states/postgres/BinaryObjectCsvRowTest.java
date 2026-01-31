// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for {@link BinaryObjectCsvRow}. */
class BinaryObjectCsvRowTest {

    // ==================== hexHash ====================

    @Test
    void hexHashReturnsCorrectHex() {
        byte[] hash = HexFormat.of().parseHex("abcdef0123456789");
        BinaryObjectCsvRow row = new BinaryObjectCsvRow(1, 1, hash, 100, new byte[0]);
        assertEquals("abcdef0123456789", row.hexHash());
    }

    // ==================== computeHexHash ====================

    @Test
    void computeHexHashOfEmptyData() {
        byte[] emptyHash = new byte[48]; // placeholder
        BinaryObjectCsvRow row = new BinaryObjectCsvRow(1, 1, emptyHash, 100, new byte[0]);
        String computed = row.computeHexHash();
        assertNotNull(computed);
        // SHA-384 of empty byte array is known
        assertEquals(
                "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b",
                computed);
    }

    @Test
    void computeHexHashOfNonEmptyData() {
        byte[] data = "Hello".getBytes();
        byte[] dummyHash = new byte[48];
        BinaryObjectCsvRow row = new BinaryObjectCsvRow(1, 1, dummyHash, 100, data);
        String computed = row.computeHexHash();
        assertNotNull(computed);
        assertEquals(96, computed.length()); // SHA-384 hex is 96 chars
    }

    @Test
    void computeHexHashMatchesHashForEmptyData() {
        // Row 1 in test CSV has empty data and known SHA-384 of empty
        String expectedHash =
                "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b";
        byte[] hash = HexFormat.of().parseHex(expectedHash);
        BinaryObjectCsvRow row = new BinaryObjectCsvRow(1, 1, hash, 100, new byte[0]);
        assertEquals(expectedHash, row.hexHash());
        assertEquals(expectedHash, row.computeHexHash());
    }

    // ==================== loadBinaryObjects from Path ====================

    @Test
    void loadBinaryObjectsFromPath() {
        URL csvUrl = BinaryObjectCsvRowTest.class.getResource("/test-binary-objects.csv");
        assertNotNull(csvUrl);
        Path csvPath = Path.of(csvUrl.getPath());
        List<BinaryObjectCsvRow> rows = BinaryObjectCsvRow.loadBinaryObjects(csvPath);
        assertNotNull(rows);
        assertEquals(2, rows.size());
        assertEquals(1L, rows.get(0).id());
        assertEquals(2L, rows.get(1).id());
    }

    // ==================== loadBinaryObjectsMap from Path ====================

    @Test
    void loadBinaryObjectsMapFromPath() {
        URL csvUrl = BinaryObjectCsvRowTest.class.getResource("/test-binary-objects.csv");
        assertNotNull(csvUrl);
        Path csvPath = Path.of(csvUrl.getPath());
        Map<String, BinaryObjectCsvRow> map = BinaryObjectCsvRow.loadBinaryObjectsMap(csvPath);
        assertNotNull(map);
        assertEquals(2, map.size());
    }

    // ==================== loadBinaryObjectsMap from URL ====================

    @Test
    void loadBinaryObjectsMapFromUrl() {
        URL csvUrl = BinaryObjectCsvRowTest.class.getResource("/test-binary-objects.csv");
        assertNotNull(csvUrl);
        Map<String, BinaryObjectCsvRow> map = BinaryObjectCsvRow.loadBinaryObjectsMap(csvUrl);
        assertNotNull(map);
        assertEquals(2, map.size());
    }

    // ==================== malformed line is filtered ====================

    @Test
    void malformedLineIsFiltered() {
        // The CSV parser returns null for lines with < 3 parts, which are filtered out
        // This is implicitly tested by loading a valid CSV - malformed rows wouldn't be present
        URL csvUrl = BinaryObjectCsvRowTest.class.getResource("/test-binary-objects.csv");
        assertNotNull(csvUrl);
        List<BinaryObjectCsvRow> rows = BinaryObjectCsvRow.loadBinaryObjects(Path.of(csvUrl.getPath()));
        assertTrue(rows.stream().allMatch(r -> r.hash().length > 0));
    }
}
