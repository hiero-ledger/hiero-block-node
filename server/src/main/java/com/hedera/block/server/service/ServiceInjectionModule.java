// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.service;

import dagger.Binds;
import dagger.Module;
import javax.inject.Singleton;

/** A Dagger module for providing dependencies for Service Module. */
@Module
public interface ServiceInjectionModule {

    /**
     * Binds the service status to the service status implementation.
     *
     * @param serviceStatus needs a service status implementation
     * @return the service status implementation
     */
    @Singleton
    @Binds
    ServiceStatus bindServiceStatus(ServiceStatusImpl serviceStatus);

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
