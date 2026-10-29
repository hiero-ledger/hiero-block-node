// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;

/// The operator-facing per-client throttle settings for one gRPC method. A plugin's per-method
/// config record (e.g. `throttle.serverStatus.*`) implements this so every throttled method
/// exposes the same keys with the same names and units, and so the plugin does not hand-copy
/// each value into a [PerClientThrottleSettings]. Defaults and the config prefix stay on each
/// implementing record, since sensible defaults differ per method.
public interface MethodThrottleConfig {
    /// @return sustained requests per second allowed for one client
    int ratePerSecond();

    /// @return how many pacing intervals early a request may arrive and still be admitted
    int burstTolerance();

    /// @return maximum concurrent in-flight calls for one client
    int maxConcurrentPerClient();

    /// @return age in seconds after which a still-held permit is force-released; `0` never reaps
    int maxPermitHoldSeconds();

    /// @return these settings in the form [ThrottleSpec] and the throttle mechanism consume
    @NonNull
    default PerClientThrottleSettings toSettings() {
        return new PerClientThrottleSettings(
                ratePerSecond(), burstTolerance(), maxConcurrentPerClient(), maxPermitHoldSeconds());
    }
}
