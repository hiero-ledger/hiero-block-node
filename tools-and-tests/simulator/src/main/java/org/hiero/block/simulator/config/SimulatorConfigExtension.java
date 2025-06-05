// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config;

import com.google.auto.service.AutoService;
import com.swirlds.common.metrics.config.MetricsConfig;
import com.swirlds.common.metrics.platform.prometheus.PrometheusConfig;
import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.ConsumerConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.config.data.SimulatorStartupDataConfig;
import org.hiero.block.simulator.config.data.UnorderedStreamConfig;

/** Sets up configuration for services. */
@AutoService(ConfigurationExtension.class)
public class SimulatorConfigExtension implements ConfigurationExtension {

    /** Explicitly defined constructor. */
    public SimulatorConfigExtension() {
        super();
    }

    @NonNull
    @Override
    public Set<Class<? extends Record>> getConfigDataTypes() {
        return Set.of(
                BlockStreamConfig.class,
                UnorderedStreamConfig.class,
                ConsumerConfig.class,
                GrpcConfig.class,
                BlockGeneratorConfig.class,
                SimulatorStartupDataConfig.class,
                MetricsConfig.class,
                PrometheusConfig.class);
    }
}
