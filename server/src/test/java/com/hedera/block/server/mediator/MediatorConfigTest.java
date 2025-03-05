// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class MediatorConfigTest {

    @Test
    public void testMediatorConfig_happyPath() {
        MediatorConfig mediatorConfig = new MediatorConfig(2048, null, 90);
        assertEquals(2048, mediatorConfig.ringBufferSize());
    }

    @Test
    public void testMediatorConfig_negativeRingBufferSize() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> new MediatorConfig(-1, null, 90));
        assertEquals("Mediator Ring Buffer Size must be positive", exception.getMessage());
    }

    @Test
    public void testMediatorConfig_powerOf2Values() {

        int[] powerOf2Values = IntStream.iterate(2, n -> n * 2).limit(30).toArray();

        // Test the power of 2 values
        for (int powerOf2Value : powerOf2Values) {
            MediatorConfig mediatorConfig = new MediatorConfig(powerOf2Value, null, 90);
            assertEquals(powerOf2Value, mediatorConfig.ringBufferSize());
        }

        // Test the non-power of 2 values
        for (int powerOf2Value : powerOf2Values) {
            IllegalArgumentException exception =
                    assertThrows(IllegalArgumentException.class, () -> new MediatorConfig(powerOf2Value + 1, null, 90));
            assertEquals("Mediator Ring Buffer Size must be a power of 2", exception.getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 91, 92, 93, 94, 95, 96, 97, 98, 99})
    public void testInvalidHistoricTransitionThresholdPercentage(int value) {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> new MediatorConfig(2048, null, value));
        assertEquals("Historic Transition Threshold Percentage must be between 10 and 90", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 90})
    public void testValidHistoricTransitionThresholdPercentage(int value) {
        MediatorConfig mediatorConfig = new MediatorConfig(2048, null, value);
        assertEquals(value, mediatorConfig.historicTransitionThresholdPercentage());
    }
}
