// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.subscribeclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Pins the plugin's config-extension contract so the discovery mechanism keeps working. */
@DisplayName("SubscribeClientConfigExtension")
class SubscribeClientConfigExtensionTest {

    @Test
    @DisplayName("exposes exactly SubscribeClientConfiguration")
    void exposesConfiguration() {
        final Set<Class<? extends Record>> types = new SubscribeClientConfigExtension().getConfigDataTypes();
        assertEquals(1, types.size());
        assertTrue(types.contains(SubscribeClientConfiguration.class));
    }
}
