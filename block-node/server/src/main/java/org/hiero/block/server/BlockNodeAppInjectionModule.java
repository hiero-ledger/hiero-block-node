// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server;

import dagger.Module;
import dagger.Provides;
import io.helidon.webserver.WebServerConfig;
import javax.inject.Singleton;

/**
 * A Dagger Module for interfaces that are at the BlockNodeApp Level, should be temporary and
 * everything should be inside its own modules.
 */
@Module
public interface BlockNodeAppInjectionModule {

    /**
     * Provides a web server config builder singleton using DI.
     *
     * @return a web server config builder singleton
     */
    @Singleton
    @Provides
    static WebServerConfig.Builder provideWebServerConfigBuilder() {
        return WebServerConfig.builder();
    }
}
