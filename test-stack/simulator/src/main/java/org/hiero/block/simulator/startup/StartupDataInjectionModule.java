// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.startup;

import dagger.Binds;
import dagger.Module;
import javax.inject.Singleton;
import org.hiero.block.simulator.startup.impl.SimulatorStartupDataImpl;

/**
 * Injection module for the startup data.
 */
@Module
public interface StartupDataInjectionModule {
    /**
     * @param impl the implementation of the startup data
     *
     * @return valid, non-null, fully initialized singleton instance of the
     * startup data
     */
    @Singleton
    @Binds
    SimulatorStartupData bindSimulatorStartupData(final SimulatorStartupDataImpl impl);
}
