// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.config;

import com.google.auto.service.AutoService;
import com.swirlds.common.metrics.config.MetricsConfig;
import com.swirlds.common.metrics.platform.prometheus.PrometheusConfig;
import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;
import org.hiero.block.server.consumer.ConsumerConfig;
import org.hiero.block.server.mediator.MediatorConfig;
import org.hiero.block.server.notifier.NotifierConfig;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.producer.ProducerConfig;
import org.hiero.block.server.service.ServiceConfig;
import org.hiero.block.server.verification.VerificationConfig;

/** Registers configuration types for the server. */
@AutoService(ConfigurationExtension.class)
public class BlockNodeConfigExtension implements ConfigurationExtension {

    /** Explicitly defined constructor. */
    public BlockNodeConfigExtension() {
        super();
    }

    /**
     * {@inheritDoc}
     *
     * @return Set of configuration data types for the server
     */
    @NonNull
    @Override
    public Set<Class<? extends Record>> getConfigDataTypes() {
        return Set.of(
                ServiceConfig.class,
                MediatorConfig.class,
                NotifierConfig.class,
                MetricsConfig.class,
                PrometheusConfig.class,
                ProducerConfig.class,
                ConsumerConfig.class,
                PersistenceStorageConfig.class,
                ServerConfig.class,
                VerificationConfig.class);
    }
}
