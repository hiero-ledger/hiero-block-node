// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.function.Supplier;
import org.hiero.metrics.core.LongMeasurementSnapshot;
import org.hiero.metrics.core.MeasurementSnapshot;
import org.hiero.metrics.core.MetricRegistrySnapshot;
import org.hiero.metrics.core.MetricSnapshot;
import org.hiero.metrics.core.MetricsExporter;

/**
 * A test {@link MetricsExporter} that captures the registry snapshot supplier so that
 * tests can read current metric values via {@link #getMetricValue(String)}.
 *
 * <p>Usage: pass an instance to {@link org.hiero.metrics.core.MetricRegistry.Builder#setMetricsExporter}
 * when building the registry, then call {@link #getMetricValue} with a fully-qualified metric
 * name (e.g. {@code "block_node:" + shortName}) to read the current value.
 */
public class TestMetricsExporter implements MetricsExporter {

    private Supplier<MetricRegistrySnapshot> snapshotSupplier;

    @Override
    public void setSnapshotSupplier(@NonNull Supplier<MetricRegistrySnapshot> snapshotSupplier) {
        this.snapshotSupplier = snapshotSupplier;
    }

    /**
     * Returns the current long value of the named metric from the registry snapshot.
     *
     * @param metricName fully-qualified metric name
     * @return the current value
     * @throws IllegalArgumentException if the metric is not found
     */
    public long getMetricValue(@NonNull final String metricName) {
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

    /**
     * No-op: this exporter holds no I/O resources, so nothing needs to be released on close.
     */
    @Override
    public void close() {
        // Nothing to do
    }
}
