// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.types;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class GenerationModeTest {

    @Test
    void testGenerationMode() {
        GenerationMode mode = GenerationMode.DIR;
        assertEquals(GenerationMode.DIR, mode);
        mode = GenerationMode.CRAFT;
        assertEquals(GenerationMode.CRAFT, mode);
    }
}
