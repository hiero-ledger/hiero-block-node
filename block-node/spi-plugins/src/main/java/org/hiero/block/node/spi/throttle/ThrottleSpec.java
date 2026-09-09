// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;

/// Implemented by a plugin's `ServiceInterface`, alongside it, to opt that service into per-client
/// admission control without a separate registration call. `ServiceBuilder.registerGrpcService(port,
/// service)` is the only registration method a plugin ever calls; the registration point checks
/// `instanceof ThrottleSpec` on the service it's given and wraps it automatically when present,
/// resolving settings from this interface rather than from extra call-site arguments.
public interface ThrottleSpec {
    /// This service's per-client rate/concurrency settings, read from the plugin's own
    /// configuration.
    ///
    /// @return the per-client settings
    @NonNull
    PerClientThrottleSettings perClientSettings();

    /// This service's node-wide concurrency ceiling. A ceiling here represents an allocation of
    /// the node's total shared capacity across every throttled API — the plugin doesn't choose
    /// this number itself, it reads it from the centrally-owned, node-level config
    /// (`GlobalThrottleConfig` in the `app-config` module) and reports it here, the same way it
    /// already reads its own per-client settings from its own config record. See
    /// `docs/design/apis/api-throttling.md` ("Configuration ownership") for the full rationale on
    /// why this stays centrally owned rather than becoming a per-plugin config value.
    ///
    /// @return the node-wide concurrency ceiling for this service
    int globalConcurrencyCeiling();
}
