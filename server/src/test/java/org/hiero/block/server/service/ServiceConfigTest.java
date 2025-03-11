// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ServiceConfigTest {
    @Test
    public void testServiceConfig_happyPath() {
        ServiceConfig serviceConfig = new ServiceConfig(2000);
        assertEquals(2000, serviceConfig.shutdownDelayMillis());
    }

    @Test
    public void testServiceConfig_negativeDelayMillis() {
        assertThrows(IllegalArgumentException.class, () -> new ServiceConfig(-1));
    }
}
