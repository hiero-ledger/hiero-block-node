// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.Label;
import org.hiero.metrics.core.LongMeasurementSnapshot;
import org.hiero.metrics.core.MeasurementSnapshot;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;
import org.hiero.metrics.core.MetricRegistrySnapshot;
import org.hiero.metrics.core.MetricSnapshot;
import org.hiero.metrics.core.MetricsExporter;
import org.hiero.metrics.core.MetricsExporterFactory;

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
public class MetricsServiceImpl implements MetricsService, MetricsExporter {

    private static final String CATEGORY = "hiero_block_node_simulator";

    private final EnumMap<SimulatorMetricTypes.Counter, LongCounter.Measurement> counters =
            new EnumMap<>(SimulatorMetricTypes.Counter.class);
    private Supplier<MetricRegistrySnapshot> snapshotSupplier;
    /** Prometheus HTTP exporter discovered via SPI; {@code null} when the endpoint is disabled. */
    private final MetricsExporter prometheusExporter;

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
        final List<Label> globalLabels = List.of(new Label("instance_type", instanceType));

        // Discover and start the Prometheus HTTP exporter via SPI (mirrors the block node approach).
        // Returns null when metrics.exporter.openmetrics.http.enabled=false (e.g. in tests).
        prometheusExporter = ServiceLoader.load(MetricsExporterFactory.class).stream()
                .map(ServiceLoader.Provider::get)
                .map(factory -> factory.createExporter(globalLabels, configuration))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        // This instance acts as the exporter so that setSnapshotSupplier is intercepted and
        // forwarded to the Prometheus exporter, enabling both getValue() and Prometheus scraping.
        final MetricRegistry metricRegistry = MetricRegistry.builder()
                .discoverMetricProviders()
                .addGlobalLabel(globalLabels.getFirst())
                .setMetricsExporter(this)
                .build();
        for (SimulatorMetricTypes.Counter counter : SimulatorMetricTypes.Counter.values()) {
            counters.put(
                    counter,
                    metricRegistry
                            .register(LongCounter.builder(MetricKey.of(counter.grafanaLabel(), LongCounter.class)
                                            .addCategory(CATEGORY))
                                    .setDescription(counter.description()))
                            .getOrCreateNotLabeled());
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Stores the supplier for internal use by {@link #getValue} and forwards it to the
     * Prometheus exporter so the HTTP endpoint can serve metrics.
     */
    @Override
    public void setSnapshotSupplier(@NonNull Supplier<MetricRegistrySnapshot> snapshotSupplier) {
        this.snapshotSupplier = snapshotSupplier;
        if (prometheusExporter != null) {
            prometheusExporter.setSnapshotSupplier(snapshotSupplier);
        }
    }

    /** {@inheritDoc} Stops the Prometheus HTTP server if one was started. */
    @Override
    public void close() throws java.io.IOException {
        if (prometheusExporter != null) {
            prometheusExporter.close();
        }
    }

    /**
     * Use this method to get a specific counter for the given metric type.
     *
     * @param key to get a specific counter
     * @return the counter
     */
    @NonNull
    @Override
    public LongCounter.Measurement get(@NonNull SimulatorMetricTypes.Counter key) {
        return counters.get(key);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Reads the current value from the registry snapshot. The metric is looked up by its
     * fully-qualified name ({@code METRICS_CATEGORY:grafanaLabel}).
     *
     * @throws IllegalArgumentException if the metric is not found in the current snapshot
     */
    @Override
    public long getValue(@NonNull SimulatorMetricTypes.Counter key) {
        Objects.requireNonNull(snapshotSupplier, "MetricRegistry has not called setSnapshotSupplier yet");
        final String metricName = CATEGORY + ":" + key.grafanaLabel();
        for (MetricSnapshot snapshot : snapshotSupplier.get()) {
            if (snapshot.name().equals(metricName)) {
                for (MeasurementSnapshot measurement : snapshot) {
                    if (measurement instanceof LongMeasurementSnapshot lm) {
                        return lm.get();
                    }
                }
            }
        }
        throw new IllegalArgumentException("Metric not found: " + metricName);
    }
}
