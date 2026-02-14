// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.SemanticVersion;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ModuleInfo} functionality.
 */
public class ModuleInfoTest {
    /**
     * This test aims to verify that the {@link ModuleInfo#getInstance(Class)}
     * returns a valid {@link ModuleInfo}.
     */
    @Test
    void testModuleInfo() {
        final ModuleInfo moduleInfo = ModuleInfo.getInstance(ModuleInfo.class);
        final SemanticVersion moduleVersion = moduleInfo.version();

        assertEquals("org.hiero.block.common", moduleInfo.name());
        // ModuleInfo class does not provide any services
        assertTrue(moduleInfo.provides(ModuleInfo.class).isEmpty());
        assertEquals(SemanticVersionUtilities.from(this.getClass()), moduleVersion);
    }
}
