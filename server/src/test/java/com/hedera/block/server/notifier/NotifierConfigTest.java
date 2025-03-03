// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.notifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class NotifierConfigTest {

    @Test
    public void testNotifierConfig_happyPath() {
        NotifierConfig notifierConfig = new NotifierConfig(2048, 90);
        assertEquals(2048, notifierConfig.ringBufferSize());
    }

    @Test
    public void testNotifierConfig_negativeRingBufferSize() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> new NotifierConfig(-1, 90));
        assertEquals("Notifier Ring Buffer Size must be positive", exception.getMessage());
    }

    @Test
    public void testMediatorConfig_powerOf2Values() {

        int[] powerOf2Values = IntStream.iterate(2, n -> n * 2).limit(30).toArray();

        for (int powerOf2Value : powerOf2Values) {
            NotifierConfig notifierConfig = new NotifierConfig(powerOf2Value, 90);
            assertEquals(powerOf2Value, notifierConfig.ringBufferSize());
        }

        // Test the non-power of 2 values
        for (int powerOf2Value : powerOf2Values) {
            IllegalArgumentException exception =
                    assertThrows(IllegalArgumentException.class, () -> new NotifierConfig(powerOf2Value + 1, 90));
            assertEquals("Notifier Ring Buffer Size must be a power of 2", exception.getMessage());
        }
    }
}
