// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.realtimeblocksubscribe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Pins the plugin's config-extension contract so the discovery mechanism keeps working. */
@DisplayName("RealTimeBlockSubscribeConfigExtension")
class RealTimeBlockSubscribeConfigExtensionTest {

    @Test
    @DisplayName("exposes exactly RealTimeBlockSubscribeConfiguration")
    void exposesConfiguration() {
        final Set<Class<? extends Record>> types = new RealTimeBlockSubscribeConfigExtension().getConfigDataTypes();
        assertEquals(1, types.size());
        assertTrue(types.contains(RealTimeBlockSubscribeConfiguration.class));
    }
}
