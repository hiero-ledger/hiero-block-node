// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hederahashgraph.api.proto.java.SemanticVersion;
import com.hederahashgraph.api.proto.java.Timestamp;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class AbstractBlockItemHandlerTest {

    private TestBlockItemHandler handler;

    // Concrete implementation for testing abstract class
    private static class TestBlockItemHandler extends AbstractBlockItemHandler {
        public TestBlockItemHandler() {
            this.blockItem = BlockItem.newBuilder().build();
        }

        // Expose protected methods for testing
        public Timestamp getTestTimestamp() {
            return getTimestamp();
        }

        public SemanticVersion getTestSemanticVersion() {
            return getSemanticVersion();
        }

        public long generateTestRandomValue(long min, long max) {
            return generateRandomValue(min, max);
        }
    }

    @BeforeEach
    void setUp() {
        handler = new TestBlockItemHandler();
    }

    @Test
    void testGetItem() {
        BlockItem item = handler.getItem();
        assertNotNull(item);
    }

    @Test
    void testUnparseBlockItem() throws BlockSimulatorParsingException {
        BlockItemUnparsed unparsed = handler.unparseBlockItem();
        assertNotNull(unparsed);
    }

    @Test
    void testGetTimestamp() {
        Timestamp timestamp = handler.getTestTimestamp();
        assertNotNull(timestamp);

        // Timestamp should be recent (within last minute)
        long currentTimeSeconds = System.currentTimeMillis() / 1000;
        assertTrue(Math.abs(currentTimeSeconds - timestamp.getSeconds()) < 60);

        // Nanos should be 0 as per implementation
        assertEquals(0, timestamp.getNanos());
    }

    @Test
    void testGetSemanticVersion() {
        SemanticVersion version = handler.getTestSemanticVersion();
        assertNotNull(version);
        assertEquals(0, version.getMajor());
        assertEquals(68, version.getMinor());
        assertEquals(0, version.getPatch());
    }

    @ParameterizedTest
    @CsvSource({"1, 10", "1, 1000", "100, 200", "1, 2"})
    void testGenerateRandomValue(long min, long max) {
        for (int i = 0; i < 100; i++) { // Test multiple times to ensure range
            long value = handler.generateTestRandomValue(min, max);
            assertTrue(value >= min);
            assertTrue(value < max);
        }
    }

    @Test
    void testGenerateRandomValueWithEqualMinMax() {
        long min = 5;
        long max = 5;
        assertThrows(IllegalArgumentException.class, () -> handler.generateTestRandomValue(min, max));
    }

    @Test
    void testGenerateRandomValueWithNegativeMin() {
        assertThrows(IllegalArgumentException.class, () -> handler.generateTestRandomValue(-1, 10));
    }

    @Test
    void testGenerateRandomValueWithNegativeMax() {
        assertThrows(IllegalArgumentException.class, () -> handler.generateTestRandomValue(1, -10));
    }

    @Test
    void testGenerateRandomValueWithMinGreaterThanMax() {
        assertThrows(IllegalArgumentException.class, () -> handler.generateTestRandomValue(10, 5));
    }
}
