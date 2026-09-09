// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import com.hedera.pbj.runtime.grpc.ServiceInterface.RequestOptions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/// Default [ClientKeyExtractor] that keys by the caller's remote network address, ignoring the
/// port so that multiple connections from the same client share one bucket.
///
/// Known, accepted limitation: clients behind a shared NAT/corporate gateway, or behind a
/// reverse proxy/load balancer that does not forward the original client address, will share a
/// bucket. See `docs/design/apis/api-throttling.md` for the rationale.
public final class RemoteAddressKeyExtractor implements ClientKeyExtractor {
    private static final String UNKNOWN_KEY = "unknown";

    @NonNull
    @Override
    public String extractKey(@NonNull final RequestOptions options) {
        final SocketAddress address = options.remoteAddress();
        // The interface's default remoteAddress() can return null (e.g. an unusual transport or
        // test harness that does not override it); fall back to a shared bucket rather than
        // producing a null client key.
        if (address == null) {
            return UNKNOWN_KEY;
        }
        if (address instanceof InetSocketAddress inetSocketAddress) {
            return inetSocketAddress.getAddress().getHostAddress();
        }
        return address.toString();
    }
}
