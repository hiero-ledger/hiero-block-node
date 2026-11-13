// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.DoubleAccumulator;
import com.swirlds.metrics.api.DoubleGauge;
import com.swirlds.metrics.api.IntegerAccumulator;
import com.swirlds.metrics.api.IntegerGauge;
import com.swirlds.metrics.api.LongAccumulator;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metric;
import com.swirlds.metrics.api.MetricConfig;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collection;
import java.util.List;
import org.hiero.consensus.metrics.DurationGauge;
import org.hiero.consensus.metrics.FunctionGauge;
import org.hiero.consensus.metrics.IntegerPairAccumulator;
import org.hiero.consensus.metrics.PlatformMetricsFactory;
import org.hiero.consensus.metrics.RunningAverageMetric;
import org.hiero.consensus.metrics.SpeedometerMetric;
import org.hiero.consensus.metrics.StatEntry;

/// No-op `Metrics` for `VirtualMapStateLifecycleManager`. Implements every `PlatformMetricsFactory`
/// type since `VirtualMap`'s internal statistics use more than just `RunningAverageMetric`.
final class NoOpSwirldsMetrics implements Metrics {

    @Override
    public Metric getMetric(@NonNull final String category, @NonNull final String name) {
        return null;
    }

    @NonNull
    @Override
    public Collection<Metric> findMetricsByCategory(@NonNull final String category) {
        return List.of();
    }

    @NonNull
    @Override
    public Collection<Metric> getAll() {
        return List.of();
    }

    @NonNull
    @Override
    public <T extends Metric> T getOrCreate(@NonNull final MetricConfig<T, ?> config) {
        return config.create(FACTORY);
    }

    @Override
    public void remove(@NonNull final String category, @NonNull final String name) {
        // no-op: nothing is ever stored
    }

    @Override
    public void remove(@NonNull final Metric metric) {
        // no-op: nothing is ever stored
    }

    @Override
    public void remove(@NonNull final MetricConfig<?, ?> config) {
        // no-op: nothing is ever stored
    }

    @Override
    public void addUpdater(@NonNull final Runnable updater) {
        // no-op
    }

    @Override
    public void removeUpdater(@NonNull final Runnable updater) {
        // no-op
    }

    @Override
    public void start() {
        // no-op
    }

    private static final PlatformMetricsFactory FACTORY = new PlatformMetricsFactory() {
        @Override
        public @NonNull Counter createCounter(@NonNull final Counter.Config config) {
            return new NoOpCounter(config);
        }

        @Override
        public @NonNull DoubleAccumulator createDoubleAccumulator(@NonNull final DoubleAccumulator.Config config) {
            return new NoOpDoubleAccumulator(config);
        }

        @Override
        public @NonNull DoubleGauge createDoubleGauge(@NonNull final DoubleGauge.Config config) {
            return new NoOpDoubleGauge(config);
        }

        @Override
        public @NonNull IntegerAccumulator createIntegerAccumulator(@NonNull final IntegerAccumulator.Config config) {
            return new NoOpIntegerAccumulator(config);
        }

        @Override
        public @NonNull IntegerGauge createIntegerGauge(@NonNull final IntegerGauge.Config config) {
            return new NoOpIntegerGauge(config);
        }

        @Override
        public @NonNull LongAccumulator createLongAccumulator(@NonNull final LongAccumulator.Config config) {
            return new NoOpLongAccumulator(config);
        }

        @Override
        public @NonNull LongGauge createLongGauge(@NonNull final LongGauge.Config config) {
            return new NoOpLongGauge(config);
        }

        @Override
        public @NonNull DurationGauge createDurationGauge(@NonNull final DurationGauge.Config config) {
            return new NoOpDurationGauge(config);
        }

        @Override
        public <T> @NonNull FunctionGauge<T> createFunctionGauge(@NonNull final FunctionGauge.Config<T> config) {
            return new NoOpFunctionGauge<>(config);
        }

        @Override
        public <T> @NonNull IntegerPairAccumulator<T> createIntegerPairAccumulator(
                @NonNull final IntegerPairAccumulator.Config<T> config) {
            return new NoOpIntegerPairAccumulator<>(config);
        }

        @Override
        public @NonNull RunningAverageMetric createRunningAverageMetric(
                @NonNull final RunningAverageMetric.Config config) {
            return new NoOpRunningAverageMetric(config);
        }

        @Override
        public @NonNull SpeedometerMetric createSpeedometerMetric(@NonNull final SpeedometerMetric.Config config) {
            return new NoOpSpeedometerMetric(config);
        }

        @Override
        public @NonNull StatEntry createStatEntry(@NonNull final StatEntry.Config<?> config) {
            return new NoOpStatEntry(config);
        }
    };

    /// Shared no-op getters backed by the `MetricConfig`.
    private abstract static class AbstractNoOpMetric implements Metric {
        private final MetricConfig<?, ?> config;

        AbstractNoOpMetric(@NonNull final MetricConfig<?, ?> config) {
            this.config = config;
        }

        @Override
        public String getCategory() {
            return config.getCategory();
        }

        @Override
        public String getName() {
            return config.getName();
        }

        @Override
        public String getDescription() {
            return config.getDescription();
        }

        @Override
        public String getUnit() {
            return config.getUnit();
        }

        @Override
        public String getFormat() {
            return config.getFormat();
        }

        @Override
        public void reset() {
            // no-op: nothing is accumulated
        }
    }

    private static final class NoOpCounter extends AbstractNoOpMetric implements Counter {
        private volatile long value;

        NoOpCounter(@NonNull final Counter.Config config) {
            super(config);
        }

        @Override
        public long get() {
            return value;
        }

        @Override
        public void add(final long value) {
            this.value += value;
        }

        @Override
        public void increment() {
            value++;
        }
    }

    private static final class NoOpDoubleAccumulator extends AbstractNoOpMetric implements DoubleAccumulator {
        private final double initialValue;
        private volatile double value;

        NoOpDoubleAccumulator(@NonNull final DoubleAccumulator.Config config) {
            super(config);
            this.initialValue = config.getInitialValue();
            this.value = initialValue;
        }

        @Override
        public double get() {
            return value;
        }

        @Override
        public double getInitialValue() {
            return initialValue;
        }

        @Override
        public void update(final double value) {
            this.value = value;
        }
    }

    private static final class NoOpDoubleGauge extends AbstractNoOpMetric implements DoubleGauge {
        private volatile double value;

        NoOpDoubleGauge(@NonNull final DoubleGauge.Config config) {
            super(config);
        }

        @Override
        public double get() {
            return value;
        }

        @Override
        public void set(final double value) {
            this.value = value;
        }

        @Override
        public void add(final double value) {
            this.value += value;
        }
    }

    private static final class NoOpIntegerAccumulator extends AbstractNoOpMetric implements IntegerAccumulator {
        private final int initialValue;
        private volatile int value;

        NoOpIntegerAccumulator(@NonNull final IntegerAccumulator.Config config) {
            super(config);
            this.initialValue = config.getInitialValue();
            this.value = initialValue;
        }

        @Override
        public int get() {
            return value;
        }

        @Override
        public int getInitialValue() {
            return initialValue;
        }

        @Override
        public void update(final int value) {
            this.value = value;
        }
    }

    private static final class NoOpIntegerGauge extends AbstractNoOpMetric implements IntegerGauge {
        private volatile int value;

        NoOpIntegerGauge(@NonNull final IntegerGauge.Config config) {
            super(config);
        }

        @Override
        public int get() {
            return value;
        }

        @Override
        public void set(final int value) {
            this.value = value;
        }

        @Override
        public void add(final int value) {
            this.value += value;
        }
    }

    private static final class NoOpLongAccumulator extends AbstractNoOpMetric implements LongAccumulator {
        private final long initialValue;
        private volatile long value;

        NoOpLongAccumulator(@NonNull final LongAccumulator.Config config) {
            super(config);
            this.initialValue = config.getInitialValue();
            this.value = initialValue;
        }

        @Override
        public long get() {
            return value;
        }

        @Override
        public long getInitialValue() {
            return initialValue;
        }

        @Override
        public void update(final long value) {
            this.value = value;
        }
    }

    private static final class NoOpLongGauge extends AbstractNoOpMetric implements LongGauge {
        private volatile long value;

        NoOpLongGauge(@NonNull final LongGauge.Config config) {
            super(config);
        }

        @Override
        public long get() {
            return value;
        }

        @Override
        public void set(final long value) {
            this.value = value;
        }

        @Override
        public void add(final long value) {
            this.value += value;
        }
    }

    private static final class NoOpDurationGauge extends AbstractNoOpMetric implements DurationGauge {
        private volatile java.time.Duration duration = java.time.Duration.ZERO;

        NoOpDurationGauge(@NonNull final DurationGauge.Config config) {
            super(config);
        }

        @Override
        public long getNanos() {
            return duration.toNanos();
        }

        @Override
        public void set(final java.time.Duration duration) {
            this.duration = duration;
        }

        @Override
        public double get() {
            return duration.toNanos();
        }
    }

    private static final class NoOpFunctionGauge<T> extends AbstractNoOpMetric implements FunctionGauge<T> {
        private final FunctionGauge.Config<T> config;

        NoOpFunctionGauge(@NonNull final FunctionGauge.Config<T> config) {
            super(config);
            this.config = config;
        }

        @Override
        public Metric.DataType getDataType() {
            return Metric.DataType.STRING;
        }

        @Override
        public T get() {
            return config.getSupplier().get();
        }
    }

    private static final class NoOpIntegerPairAccumulator<T> extends AbstractNoOpMetric
            implements IntegerPairAccumulator<T> {
        private final IntegerPairAccumulator.Config<T> config;
        private volatile int left;
        private volatile int right;

        NoOpIntegerPairAccumulator(@NonNull final IntegerPairAccumulator.Config<T> config) {
            super(config);
            this.config = config;
        }

        @Override
        public Metric.DataType getDataType() {
            return Metric.DataType.STRING;
        }

        @Override
        public T get() {
            return config.getResultFunction().apply(left, right);
        }

        @Override
        public int getLeft() {
            return left;
        }

        @Override
        public int getRight() {
            return right;
        }

        @Override
        public void update(final int leftValue, final int rightValue) {
            this.left = leftValue;
            this.right = rightValue;
        }
    }

    private static final class NoOpRunningAverageMetric extends AbstractNoOpMetric implements RunningAverageMetric {
        private final double halfLife;
        private volatile double value;

        NoOpRunningAverageMetric(@NonNull final RunningAverageMetric.Config config) {
            super(config);
            this.halfLife = config.getHalfLife();
        }

        @Override
        public double getHalfLife() {
            return halfLife;
        }

        @Override
        public void update(final double value) {
            this.value = value;
        }

        @Override
        public double get() {
            return value;
        }

        @Override
        public Double get(@NonNull final Metric.ValueType valueType) {
            return value;
        }
    }

    private static final class NoOpSpeedometerMetric extends AbstractNoOpMetric implements SpeedometerMetric {
        private final double halfLife;
        private volatile double value;

        NoOpSpeedometerMetric(@NonNull final SpeedometerMetric.Config config) {
            super(config);
            this.halfLife = config.getHalfLife();
        }

        @Override
        public double getHalfLife() {
            return halfLife;
        }

        @Override
        public void update(final double value) {
            this.value = value;
        }

        @Override
        public void cycle() {
            // no-op
        }

        @Override
        public double get() {
            return value;
        }

        @Override
        public Double get(@NonNull final Metric.ValueType valueType) {
            return value;
        }
    }

    private static final class NoOpStatEntry extends AbstractNoOpMetric implements StatEntry {
        private final StatEntry.Config<?> config;

        NoOpStatEntry(@NonNull final StatEntry.Config<?> config) {
            super(config);
            this.config = config;
        }

        @Override
        public Metric.DataType getDataType() {
            return Metric.DataType.STRING;
        }

        @Override
        public org.hiero.consensus.metrics.statistics.StatsBuffered getBuffered() {
            return config.getBuffered();
        }

        @Override
        public java.util.function.Consumer<Double> getReset() {
            return config.getReset();
        }

        @Override
        public java.util.function.Supplier<Object> getStatsStringSupplier() {
            @SuppressWarnings("unchecked")
            final java.util.function.Supplier<Object> supplier =
                    (java.util.function.Supplier<Object>) config.getStatsStringSupplier();
            return supplier;
        }

        @Override
        public java.util.function.Supplier<Object> getResetStatsStringSupplier() {
            @SuppressWarnings("unchecked")
            final java.util.function.Supplier<Object> supplier =
                    (java.util.function.Supplier<Object>) config.getResetStatsStringSupplier();
            return supplier;
        }
    }
}
