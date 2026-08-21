// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.hiero.metrics.core.LabelValues;
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
     * <p>
     * Labels are ignored: for a metric with dynamic labels this returns the first measurement found, which is one
     * arbitrary label value rather than a total across them. Only use it on metrics that have a single series in the
     * test at hand.
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
     * Returns the current long value of every series of the named metric, keyed by its first dynamic label value.
     * Use this instead of {@link #getMetricValue(String)} for a metric with dynamic labels, where the series present
     * and the label values they carry are themselves what the test is asserting.
     *
     * @param metricName fully-qualified metric name
     * @return the current value of each series, keyed by first label value, empty if the metric is not found
     */
    public Map<String, Long> getMetricValuesByLabel(@NonNull final String metricName) {
        final Map<String, Long> valuesByLabel = new LinkedHashMap<>();
        for (MetricSnapshot snapshot : snapshotSupplier.get()) {
            if (snapshot.name().equals(metricName)) {
                for (MeasurementSnapshot measurement : snapshot) {
                    if (measurement instanceof LongMeasurementSnapshot lm) {
                        final LabelValues labels = lm.getDynamicLabelValues();
                        valuesByLabel.put(labels.size() == 0 ? "" : labels.get(0), lm.get());
                    }
                }
            }
        }
        return valuesByLabel;
    }

    /**
     * No-op: this exporter holds no I/O resources, so nothing needs to be released on close.
     */
    @Override
    public void close() {
        // Nothing to do
    }
}
