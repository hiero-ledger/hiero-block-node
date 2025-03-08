// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server;

import com.hedera.block.server.ack.AckHandlerInjectionModule;
import com.hedera.block.server.config.ConfigInjectionModule;
import com.hedera.block.server.health.HealthInjectionModule;
import com.hedera.block.server.mediator.MediatorInjectionModule;
import com.hedera.block.server.metrics.MetricsInjectionModule;
import com.hedera.block.server.notifier.NotifierInjectionModule;
import com.hedera.block.server.pbj.PbjInjectionModule;
import com.hedera.block.server.persistence.PersistenceInjectionModule;
import com.hedera.block.server.service.ServiceInjectionModule;
import com.hedera.block.server.service.WebServerStatusInjectionModule;
import com.hedera.block.server.verification.VerificationInjectionModule;
import com.swirlds.config.api.Configuration;
import dagger.BindsInstance;
import dagger.Component;
import javax.inject.Singleton;

/** The infrastructure used to manage the instances and inject them using Dagger */
@Singleton
@Component(
        modules = {
            NotifierInjectionModule.class,
            ServiceInjectionModule.class,
            WebServerStatusInjectionModule.class,
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
