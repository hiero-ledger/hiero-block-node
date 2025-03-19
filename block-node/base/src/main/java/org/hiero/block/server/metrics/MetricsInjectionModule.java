// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.metrics;

import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;

/** The module used to inject the metrics service and metrics into the application. */
@Module
public interface MetricsInjectionModule {

    /**
     * Provides the metrics service.
     *
     * @param metricsService the metrics service to be used
     * @return the metrics service
     */
    @Singleton
    @Binds
    MetricsService bindMetricsService(MetricsServiceImpl metricsService);

    /**
     * Provides the metrics.
     *
     * @param configuration the configuration to be used by the metrics
     * @return the metrics
     */
    @Singleton
    @Provides
    static Metrics provideMetrics(Configuration configuration) {
        final DefaultMetricsProvider metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();
        metricsProvider.start();
        return metrics;
    }
}
