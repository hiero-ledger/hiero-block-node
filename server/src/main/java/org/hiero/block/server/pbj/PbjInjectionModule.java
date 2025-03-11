// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.pbj;

import dagger.Binds;
import dagger.Module;
import javax.inject.Singleton;

/**
 * A Dagger module for providing PBJ dependencies, any specific PBJ should be part of this module.
 */
@Module
public interface PbjInjectionModule {

    /**
     * Provides a block stream service singleton using DI.
     *
     * @param pbjBlockStreamServiceProxy should come from DI
     * @return a block stream service singleton
     */
    @Singleton
    @Binds
    PbjBlockStreamService bindPbjBlockStreamService(PbjBlockStreamServiceProxy pbjBlockStreamServiceProxy);

    /**
     * Provides a block access service singleton using DI.
     *
     * @param pbjBlockAccessServiceProxy should come from DI
     * @return a block access service singleton
     */
    @Singleton
    @Binds
    PbjBlockAccessService bindPbjBlockAccessService(PbjBlockAccessServiceProxy pbjBlockAccessServiceProxy);

    @Singleton
    @Binds
    PbjServerStatusService bindPbjServerStatusService(PbjServerStatusServiceProxy pbjServerStatusServiceProxy);
}
