// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.module;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.SemanticVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ModuleInfoAccessor} functionality.
 */
public class ModuleInfoAccessorTest {
    /**
     * This test aims to verify that the {@link ModuleInfoAccessor#getInstance(Class)}
     * returns a valid {@link ModuleInfoAccessor}.
     */
    @Test
    void testModuleInfo() {
        final ModuleInfoAccessor moduleInfoAccessor = ModuleInfoAccessor.getInstance(ModuleInfoAccessor.class);
        final SemanticVersion moduleVersion = moduleInfoAccessor.version();

        assertEquals("org.hiero.block.node.spi", moduleInfoAccessor.name());
        // ModuleInfo class does not provide any services
        assertTrue(moduleInfoAccessor.provides(ModuleInfoAccessor.class).isEmpty());
        Assertions.assertEquals(SemanticVersionUtilities.from(this.getClass()), moduleVersion);
    }
}
