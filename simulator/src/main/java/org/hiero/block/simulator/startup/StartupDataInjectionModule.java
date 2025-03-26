// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.startup;

import dagger.Binds;
import dagger.Module;
import javax.inject.Singleton;
import org.hiero.block.simulator.startup.impl.SimulatorStartupDataImpl;

@Module
public interface StartupDataInjectionModule {
    @Singleton
    @Binds
    SimulatorStartupData bindSimulatorStartupData(final SimulatorStartupDataImpl impl);
}
