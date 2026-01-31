// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link SmartContractKVPairs}. */
class SmartContractKVPairsTest {

    // ==================== deserializeKeyValuePairs ====================

    @Test
    void deserializeKeyValuePairsSinglePair() {
        byte[] data = new byte[64];
        // key: first 32 bytes, value: next 32 bytes
        data[0] = 1;
        data[31] = 2;
        data[32] = 3;
        data[63] = 4;

        List<SmartContractKVPairs.DataWordPair> pairs = SmartContractKVPairs.deserializeKeyValuePairs(data);
        assertEquals(1, pairs.size());
        assertEquals(1, pairs.getFirst().key().data()[0]);
        assertEquals(2, pairs.getFirst().key().data()[31]);
        assertEquals(3, pairs.getFirst().value().data()[0]);
        assertEquals(4, pairs.getFirst().value().data()[31]);
    }

    @Test
    void deserializeKeyValuePairsMultiplePairs() {
        byte[] data = new byte[128]; // 2 pairs
        data[0] = 10;
        data[64] = 20;

        List<SmartContractKVPairs.DataWordPair> pairs = SmartContractKVPairs.deserializeKeyValuePairs(data);
        assertEquals(2, pairs.size());
    }

    @Test
    void deserializeKeyValuePairsEmpty() {
        byte[] data = new byte[0];
        List<SmartContractKVPairs.DataWordPair> pairs = SmartContractKVPairs.deserializeKeyValuePairs(data);
        assertTrue(pairs.isEmpty());
    }

    @Test
    void dataWordInvalidLengthThrows() {
        assertThrows(IllegalArgumentException.class, () -> new SmartContractKVPairs.DataWord(new byte[10]));
    }
}
