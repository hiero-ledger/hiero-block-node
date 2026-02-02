// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.balances;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URL;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for {@link CsvAccountBalances}. */
class CsvAccountBalancesTest {

    // ==================== load from plain CSV ====================

    @Test
    void loadCsvBalancesFromPlainCsv() {
        URL csvUrl = CsvAccountBalancesTest.class.getResource("/test-balances.csv");
        assertNotNull(csvUrl, "test-balances.csv should be on classpath");
        Map<Long, Long> balances = CsvAccountBalances.loadCsvBalances(csvUrl);
        assertNotNull(balances);
        assertEquals(3, balances.size());
        assertEquals(100000L, balances.get(1L));
        assertEquals(200000L, balances.get(2L));
        assertEquals(300000L, balances.get(3L));
    }

    // ==================== load from gzipped resource ====================

    @Test
    void loadCsvBalancesFromGzippedResource() {
        // The real gzipped balance file used in production
        URL csvUrl = CsvAccountBalancesTest.class.getResource("/2019-09-13T22_00_00.000081Z_Balances.csv.gz");
        if (csvUrl == null) {
            // Skip if resource not available (CI may not have it)
            return;
        }
        Map<Long, Long> balances = CsvAccountBalances.loadCsvBalances(csvUrl);
        assertNotNull(balances);
        // The file should have many accounts
        assert balances.size() > 0;
    }

    // ==================== parseLong error path ====================

    @Test
    void loadCsvBalancesWithBadValueUsesMinValue() {
        URL csvUrl = CsvAccountBalancesTest.class.getResource("/test-balances-bad.csv");
        assertNotNull(csvUrl);
        Map<Long, Long> balances = CsvAccountBalances.loadCsvBalances(csvUrl);
        assertNotNull(balances);
        // The bad account value should be parsed as Long.MIN_VALUE
        assertEquals(true, balances.containsKey(Long.MIN_VALUE));
    }

    // ==================== missing URL throws ====================

    @Test
    void loadCsvBalancesNullUrlThrows() {
        assertThrows(Exception.class, () -> CsvAccountBalances.loadCsvBalances(null));
    }
}
