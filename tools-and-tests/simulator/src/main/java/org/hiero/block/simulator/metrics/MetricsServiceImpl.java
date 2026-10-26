// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import javax.inject.Inject;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.Label;
import org.hiero.metrics.core.Metric;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;
import org.hiero.metrics.core.MetricsExporter;

/**
 * Implementation of {@link MetricsService} for the Block Stream Simulator.
 *
 * <p>Also implements {@link MetricsExporter} to act as a composite exporter: it captures the
 * snapshot supplier from the {@link MetricRegistry} and forwards it to a discovered Prometheus
 * HTTP exporter (via SPI), enabling both internal metric reads and external scraping.
 *
 * <p>Metrics are updated by calling the appropriate method on the metric object instance. For
 * example, to increment a counter, call {@link LongCounter.Measurement#increment()}.
 */
public class MetricsServiceImpl implements MetricsService, AutoCloseable {

    private final MetricRegistry metricRegistry;

    /**
     * Creates and initializes the metrics service.
     *
     * <p>Reads the simulator mode from configuration to set the {@code instance_type} global label
     * (e.g. {@code publisher_client}, {@code consumer}), which allows Grafana dashboards to
     * distinguish between simulator instances. Discovers and starts the Prometheus HTTP exporter
     * via SPI if enabled in configuration, then registers all counters with the metric registry.
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

    /** {@inheritDoc} Stops the Prometheus HTTP server if one was started. */
    @Override
    public void close() throws IOException {
        metricRegistry.close();
    }
}
