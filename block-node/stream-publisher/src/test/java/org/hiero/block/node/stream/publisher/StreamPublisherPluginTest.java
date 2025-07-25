// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import java.util.List;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link StreamPublisherPlugin}.
 */
@DisplayName("StreamPublisherPlugin Tests")
class StreamPublisherPluginTest {
    /**
     * Enable debug logging for each test.
     */
    @BeforeEach
    protected void setup() {
        enableDebugLogging();
    }

    /**
     * Test for the {@link StreamPublisherPlugin} plugin.
     */
    @Nested
    @DisplayName("Plugin Tests")
    class PluginTest extends GrpcPluginTestBase<StreamPublisherPlugin> {
        /**
         * Constructor for the plugin tests.
         */
        PluginTest() {
            final StreamPublisherPlugin toTest = new StreamPublisherPlugin();
            final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility =
                    new SimpleInMemoryHistoricalBlockFacility();
            start(toTest, toTest.methods().getFirst(), historicalBlockFacility);
        }

        /**
         * Verifies that the service interface correctly registers and exposes
         * the server status method.
         */
        @Test
        @DisplayName("Test verify correct method/s registered for StreamPublisherPlugin in test base")
        void testVerifyCorrectMethodRegistered() {
            final List<?> methods = assertThat(serviceInterface)
                    .isNotNull()
                    .extracting(ServiceInterface::methods)
                    .asInstanceOf(InstanceOfAssertFactories.LIST)
                    .hasSize(1)
                    .containsExactly(plugin.methods().getFirst())
                    .actual();
            System.out.println("Methods registered for plugin tests: " + methods);
        }
    }
}
