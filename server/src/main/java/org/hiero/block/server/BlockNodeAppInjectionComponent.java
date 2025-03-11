// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server;

import com.swirlds.config.api.Configuration;
import dagger.BindsInstance;
import dagger.Component;
import javax.inject.Singleton;
import org.hiero.block.server.ack.AckHandlerInjectionModule;
import org.hiero.block.server.config.ConfigInjectionModule;
import org.hiero.block.server.health.HealthInjectionModule;
import org.hiero.block.server.mediator.MediatorInjectionModule;
import org.hiero.block.server.metrics.MetricsInjectionModule;
import org.hiero.block.server.notifier.NotifierInjectionModule;
import org.hiero.block.server.pbj.PbjInjectionModule;
import org.hiero.block.server.persistence.PersistenceInjectionModule;
import org.hiero.block.server.service.ServiceInjectionModule;
import org.hiero.block.server.verification.VerificationInjectionModule;

/** The infrastructure used to manage the instances and inject them using Dagger */
@Singleton
@Component(
        modules = {
            NotifierInjectionModule.class,
            ServiceInjectionModule.class,
            BlockNodeAppInjectionModule.class,
            HealthInjectionModule.class,
            PersistenceInjectionModule.class,
            MediatorInjectionModule.class,
            ConfigInjectionModule.class,
            MetricsInjectionModule.class,
            PbjInjectionModule.class,
            VerificationInjectionModule.class,
            AckHandlerInjectionModule.class
        })
public interface BlockNodeAppInjectionComponent {
    /**
     * Get the block node app server.
     *
     * @return the block node app server
     */
    BlockNodeApp getBlockNodeApp();

    /**
     * Factory for the block node app injection component, needs a configuration to create the
     * component and the block node app with all the wired dependencies.
     */
    @Component.Factory
    interface Factory {
        /**
         * Create the block node app injection component.
         *
         * @param configuration the configuration
         * @return the block node app injection component
         */
        BlockNodeAppInjectionComponent create(@BindsInstance Configuration configuration);
    }
}
