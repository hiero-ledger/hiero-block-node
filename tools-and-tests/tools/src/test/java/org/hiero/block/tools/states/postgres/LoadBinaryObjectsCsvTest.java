// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for {@link LoadBinaryObjectsCsv}. */
class LoadBinaryObjectsCsvTest {

    // ==================== loadBinaryObjectsMap ====================

    @Test
    void loadBinaryObjectsMapFromPath() throws Exception {
        URL csvUrl = LoadBinaryObjectsCsvTest.class.getResource("/test-binary-objects.csv");
        assertNotNull(csvUrl);
        Path csvPath = Path.of(csvUrl.getPath());
        Map<String, BinaryObjectCsvRow> map = LoadBinaryObjectsCsv.loadBinaryObjectsMap(csvPath);
        assertNotNull(map);
        assertEquals(2, map.size());
    }

    // ==================== constants ====================

    @Test
    void constantsHaveExpectedValues() {
        assertEquals(101L, LoadBinaryObjectsCsv.ADDRESS_FILE_ACCOUNT_NUM);
        assertEquals(102L, LoadBinaryObjectsCsv.NODE_DETAILS_FILE);
        assertEquals(111L, LoadBinaryObjectsCsv.FEE_FILE_ACCOUNT_NUM);
        assertEquals(112L, LoadBinaryObjectsCsv.EXCHANGE_RATE_FILE_ACCOUNT_NUM);
    }
}
