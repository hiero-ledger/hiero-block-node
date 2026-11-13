// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.pbj.runtime.grpc.ServiceInterface.RequestOptions;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.function.Supplier;
import org.hiero.metrics.core.LongMeasurementSnapshot;
import org.hiero.metrics.core.MeasurementSnapshot;
import org.hiero.metrics.core.MetricRegistry;
import org.hiero.metrics.core.MetricRegistrySnapshot;
import org.hiero.metrics.core.MetricSnapshot;
import org.hiero.metrics.core.MetricsExporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RemoteAddressKeyExtractorTest {
    private RemoteAddressKeyExtractor extractor;
    private SnapshotReadingMetricsExporter exporter;

    @BeforeEach
    void setUp() {
        exporter = new SnapshotReadingMetricsExporter();
        final MetricRegistry metricRegistry =
                MetricRegistry.builder().setMetricsExporter(exporter).build();
        extractor = new RemoteAddressKeyExtractor(metricRegistry);
    }

    @Test
    @DisplayName("Keys by host address, ignoring the port")
    void keysByHostAddressIgnoringPort() {
        assertEquals("10.0.0.1", extractor.extractKey(optionsFor(loopbackLike("10.0.0.1"), 40840)));
        assertEquals("10.0.0.1", extractor.extractKey(optionsFor(loopbackLike("10.0.0.1"), 55555)));
    }

    @Test
    @DisplayName("A null remote address falls back to a shared key and increments the unknown-address counter")
    void nullAddressFallsBackAndIncrementsCounter() {
        assertEquals("unknown", extractor.extractKey(optionsWithRemoteAddress(null)));
        assertEquals("unknown", extractor.extractKey(optionsWithRemoteAddress(null)));

        assertEquals(2, exporter.getMetricValue(RemoteAddressKeyExtractor.METRIC_UNKNOWN_REMOTE_ADDRESS.name()));
    }

    @Test
    @DisplayName("A resolvable address never touches the unknown-address counter")
    void resolvableAddressDoesNotIncrementCounter() {
        extractor.extractKey(optionsFor(loopbackLike("10.0.0.1"), 40840));

        assertEquals(0, exporter.getMetricValue(RemoteAddressKeyExtractor.METRIC_UNKNOWN_REMOTE_ADDRESS.name()));
    }

    /// A test [MetricsExporter] that reads back a named metric's current long value from the
    /// registry snapshot.
    private static final class SnapshotReadingMetricsExporter implements MetricsExporter {
        private Supplier<MetricRegistrySnapshot> snapshotSupplier;

        @Override
        public void setSnapshotSupplier(final Supplier<MetricRegistrySnapshot> snapshotSupplier) {
            this.snapshotSupplier = snapshotSupplier;
        }

        long getMetricValue(final String metricName) {
            for (final MetricSnapshot snapshot : snapshotSupplier.get()) {
                if (snapshot.name().equals(metricName)) {
                    for (final MeasurementSnapshot measurement : snapshot) {
                        if (measurement instanceof LongMeasurementSnapshot lm) {
                            return lm.get();
                        }
                    }
                }
            }
            throw new IllegalArgumentException("Metric not found: " + metricName);
        }

        @Override
        public void close() {}
    }

    private static RequestOptions optionsFor(final InetAddress address, final int port) {
        return optionsWithRemoteAddress(new InetSocketAddress(address, port));
    }

    private static RequestOptions optionsWithRemoteAddress(final SocketAddress address) {
        return new RequestOptions() {
            @Override
            public Optional<String> authority() {
                return Optional.empty();
            }

            @Override
            public String contentType() {
                return RequestOptions.APPLICATION_GRPC_PROTO;
            }

            @Override
            public SocketAddress remoteAddress() {
                return address;
            }
        };
    }

    private static InetAddress loopbackLike(final String ipAddress) {
        try {
            return InetAddress.getByName(ipAddress);
        } catch (final UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }
}
