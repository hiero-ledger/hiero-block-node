// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import com.hedera.pbj.runtime.grpc.ServiceInterface.RequestOptions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Default [ClientKeyExtractor] that keys by the caller's remote network address, ignoring the
/// port so that multiple connections from the same client share one bucket.
///
/// Known, accepted limitation: clients behind a shared NAT/corporate gateway, or behind a
/// reverse proxy/load balancer that does not forward the original client address, will share a
/// bucket. See `docs/design/apis/api-throttling.md` for the rationale.
public final class RemoteAddressKeyExtractor implements ClientKeyExtractor {
    private static final String UNKNOWN_KEY = "unknown";

    /// Metric key for [#UNKNOWN_KEY] fallbacks — public so tests can read the registered value
    /// back by name.
    public static final MetricKey<LongCounter> METRIC_UNKNOWN_REMOTE_ADDRESS = MetricKey.of(
                    "throttle_unknown_remote_address_total", LongCounter.class)
            .addCategory(BlockNodePlugin.METRICS_CATEGORY);

    private final LongCounter.Measurement unknownAddressCounter;

    /// @param metricRegistry the registry to register this instance's metric with — construct
    ///     exactly once and share it, since registering the same metric name twice throws
    public RemoteAddressKeyExtractor(@NonNull final MetricRegistry metricRegistry) {
        unknownAddressCounter = metricRegistry
                .register(LongCounter.builder(METRIC_UNKNOWN_REMOTE_ADDRESS)
                        .setDescription("Calls with no resolvable remote address, keyed into the shared "
                                + UNKNOWN_KEY + " bucket instead of their own — a sustained non-zero rate usually"
                                + " means a proxy/load balancer isn't forwarding the original client address"))
                .getOrCreateNotLabeled();
    }

    @NonNull
    @Override
    public String extractKey(@NonNull final RequestOptions options) {
        final SocketAddress address = options.remoteAddress();
        // The interface's default remoteAddress() can return null (e.g. an unusual transport or
        // test harness that does not override it); fall back to a shared bucket rather than
        // producing a null client key.
        if (address == null) {
            unknownAddressCounter.increment();
            return UNKNOWN_KEY;
        }
        if (address instanceof InetSocketAddress inetSocketAddress) {
            return inetSocketAddress.getAddress().getHostAddress();
        }
        return address.toString();
    }
}
