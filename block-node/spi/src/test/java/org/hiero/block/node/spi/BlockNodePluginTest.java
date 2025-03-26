// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.helidon.common.Builder;
import io.helidon.webserver.Routing;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link BlockNodePlugin} interface and its default methods.
 */
public class BlockNodePluginTest {

    private static class TestBlockNodePlugin implements BlockNodePlugin {}

    @Test
    @DisplayName("Test default name method")
    void testDefaultName() {
        BlockNodePlugin plugin = new TestBlockNodePlugin();
        assertEquals("TestBlockNodePlugin", plugin.name());
    }

    @Test
    @DisplayName("Test default configDataTypes method")
    void testDefaultConfigDataTypes() {
        BlockNodePlugin plugin = new TestBlockNodePlugin();
        List<Class<? extends Record>> configDataTypes = plugin.configDataTypes();
        assertNotNull(configDataTypes);
        assertEquals(0, configDataTypes.size());
    }

    @Test
    @DisplayName("Test default init method")
    void testDefaultInit() {
        BlockNodePlugin plugin = new TestBlockNodePlugin();
        Builder<?, ? extends Routing> routingBuilder = plugin.init(null);
        assertNull(routingBuilder);
    }

    @Test
    @DisplayName("Test default start method")
    void testDefaultStart() {
        BlockNodePlugin plugin = new TestBlockNodePlugin();
        plugin.start();
        // No exception means the default implementation is a no-op
    }

    @Test
    @DisplayName("Test default stop method")
    void testDefaultStop() {
        BlockNodePlugin plugin = new TestBlockNodePlugin();
        plugin.stop();
        // No exception means the default implementation is a no-op
    }
}
