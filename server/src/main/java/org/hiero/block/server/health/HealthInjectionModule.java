// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.health;

import dagger.Binds;
import dagger.Module;
import javax.inject.Singleton;

/**
 * A Dagger module for providing dependencies for Health Module, should we refactor to have an
 * observability module instead?.
 */
@Module
public interface HealthInjectionModule {

    /**
     * Binds the health service to the health service implementation.
     *
     * @param healthService needs a health service implementation
     * @return the health service implementation
     */
    @Singleton
    @Binds
    HealthService bindHealthService(HealthServiceImpl healthService);
}
