// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server;

import io.helidon.webserver.WebServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BlockNodeAppInjectionModuleTest {

    @Test
    void testProvideWebServerConfigBuilder() {
        WebServerConfig.Builder webServerConfigBuilder = BlockNodeAppInjectionModule.provideWebServerConfigBuilder();
        Assertions.assertNotNull(webServerConfigBuilder);
    }
}
