// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.service;

import dagger.Binds;
import dagger.Module;
import javax.inject.Singleton;

/** A Dagger module for providing dependencies for WebServerStatus Module. */
@Module
public interface WebServerStatusInjectionModule {

    /**
     * Binds the service status to the Web Server status implementation.
     *
     * @param webServerStatus needs a service status implementation
     * @return the service status implementation
     */
    @Singleton
    @Binds
    WebServerStatus bindWebServerStatus(WebServerStatusImpl webServerStatus);
}
