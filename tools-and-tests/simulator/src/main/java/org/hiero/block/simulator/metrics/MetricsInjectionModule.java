// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import dagger.Binds;
import dagger.Module;
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
}
