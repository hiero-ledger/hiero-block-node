// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator;

import com.swirlds.config.api.Configuration;
import dagger.BindsInstance;
import dagger.Component;
import javax.inject.Singleton;
import org.hiero.block.simulator.config.ConfigInjectionModule;
import org.hiero.block.simulator.generator.GeneratorInjectionModule;
import org.hiero.block.simulator.grpc.GrpcInjectionModule;
import org.hiero.block.simulator.metrics.MetricsInjectionModule;
import org.hiero.block.simulator.mode.SimulatorModeInjectionModule;

/** The component used to inject the block stream simulator into the application. */
@Singleton
@Component(
        modules = {
            MetricsInjectionModule.class,
            ConfigInjectionModule.class,
            GeneratorInjectionModule.class,
            GrpcInjectionModule.class,
            SimulatorModeInjectionModule.class
        })
public interface BlockStreamSimulatorInjectionComponent {

    /**
     * Gets the block stream simulator.
     *
     * @return the block stream simulator
     */
    BlockStreamSimulatorApp getBlockStreamSimulatorApp();

    /** The factory used to create the block stream simulator injection component. */
    @Component.Factory
    interface Factory {
        /**
         * Creates the block stream simulator injection component.
         *
         * @param configuration the configuration to be used by the block stream simulator
         * @return the block stream simulator injection component
         */
        BlockStreamSimulatorInjectionComponent create(@BindsInstance Configuration configuration);
    }
}
