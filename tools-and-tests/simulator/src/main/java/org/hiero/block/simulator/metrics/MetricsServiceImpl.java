// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import javax.inject.Inject;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.metrics.core.Label;
import org.hiero.metrics.core.Metric;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/**
 * Implementation of {@link MetricsService} for the Block Stream Simulator.
 *
 * <p>Wraps a {@link MetricRegistry} whose metrics are discovered via SPI (see
 * {@link SimulatorMetricsProvider}) and whose exporter (e.g. the Prometheus HTTP endpoint) is
 * discovered via SPI when enabled in configuration.
 *
 * <p>Metrics are accessed by their {@link MetricKey} through {@link #getMetric(MetricKey)} or the
 * convenience methods on {@link MetricsService}.
 */
public class MetricsServiceImpl implements MetricsService {

    private final MetricRegistry metricRegistry;

    /**
     * Creates and initializes the metrics service.
     *
     * <p>Reads the simulator mode from configuration to set the {@code instance_type} global label
     * (e.g. {@code publisher_client}, {@code consumer}), which allows Grafana dashboards to
     * distinguish between simulator instances. Builds the metric registry, registering metrics from
     * all discovered {@link org.hiero.metrics.core.MetricsRegistrationProvider}s and starting the
     * discovered metrics exporter (e.g. Prometheus HTTP) if enabled in configuration.
     *
     * @param configuration the resolved application configuration
     */
    @Inject
    public MetricsServiceImpl(@NonNull final Configuration configuration) {
        // Derive the instance_type label from the simulator mode so dashboards can filter by instance
        final String instanceType = configuration
                .getConfigData(BlockStreamConfig.class)
                .simulatorMode()
                .name()
                .toLowerCase();

        this.metricRegistry = MetricRegistry.builder()
                .discoverMetricProviders()
                .discoverMetricsExporter(configuration)
                .addGlobalLabel(new Label("instance_type", instanceType))
                .build();
    }

    @NonNull
    @Override
    public <M extends Metric> M getMetric(MetricKey<M> key) {
        return metricRegistry.getMetric(key);
    }

    /**
     * Closes the underlying metric registry, which also closes the metrics exporter (stopping the
     * Prometheus HTTP server if one was started).
     */
    @Override
    public void close() throws IOException {
        metricRegistry.close();
    }
}
