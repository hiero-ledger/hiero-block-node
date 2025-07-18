// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamingModeTest {

    @org.junit.jupiter.api.Test
    void fromString() {
        assertEquals(StreamingMode.CONSTANT_RATE, StreamingMode.fromString("CONSTANT_RATE"));
        assertEquals(StreamingMode.MILLIS_PER_BLOCK, StreamingMode.fromString("MILLIS_PER_BLOCK"));
        assertThrows(IllegalArgumentException.class, () -> StreamingMode.fromString("INVALID"));
    }
}
