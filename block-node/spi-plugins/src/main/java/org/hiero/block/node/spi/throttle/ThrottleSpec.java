// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;

/// Implemented by a plugin's `ServiceInterface`, alongside it, to opt that service into per-client
/// admission control without a separate registration call. `ServiceBuilder.registerGrpcService(port,
/// service)` is the only registration method a plugin ever calls; the registration point checks
/// `instanceof ThrottleSpec` on the service it's given and wraps it automatically when present,
/// resolving settings from this interface rather than from extra call-site arguments.
///
/// A service with more than one method (e.g. `BlockNodeService`'s `serverStatus` and
/// `serverStatusDetail`) may configure each method independently, configure only some of them, or
/// configure none at all — see [#perClientSettings] and [#defaultPerClientSettings].
public interface ThrottleSpec {
    /// This service's per-client rate/concurrency settings, one entry per method this service
    /// wants gated individually, keyed by that method's name (e.g. `"serverStatus"`). A method
    /// with no entry here — and no [#defaultPerClientSettings] configured either — gets **no
    /// per-client gate at all**: its calls are still subject to [#globalConcurrencyCeiling] (the
    /// node-wide ceiling is mandatory and always shared, regardless of per-client configuration),
    /// but nothing tracks any individual client's rate or concurrency for it.
    ///
    /// @return the per-method settings map; may be empty
    @NonNull
    Map<String, PerClientThrottleSettings> perClientSettings();

    /// A fallback applied to any of this service's methods that [#perClientSettings] doesn't
    /// cover, instead of leaving them with no per-client gate. Empty by default, which preserves
    /// the behavior described in [#perClientSettings]: an unconfigured method is governed only by
    /// the shared [#globalConcurrencyCeiling].
    ///
    /// This exists so a plugin can later give every otherwise-unconfigured method a shared,
    /// generous default instead of skipping per-client gating entirely — a config/wiring change
    /// for that one plugin, not a change to the admission mechanism itself.
    ///
    /// @return the fallback per-client settings, or empty to leave every otherwise-unconfigured
    ///     method ungated at the per-client layer
    @NonNull
    default Optional<PerClientThrottleSettings> defaultPerClientSettings() {
        return Optional.empty();
    }

    /// This service's node-wide concurrency ceiling. A ceiling here represents an allocation of
    /// the node's total shared capacity across every throttled API — the plugin doesn't choose
    /// this number itself, it reads it from the centrally-owned, node-level config
    /// (`GlobalThrottleConfig` in the `app` module) and reports it here, the same way it already
    /// reads its own per-client settings from its own config record.
    ///
    /// This ceiling is **shared by every method on this service**, configured or not — it is not
    /// split per method. See `docs/design/apis/api-throttling.md` ("Configuration ownership") for
    /// the full rationale on why this stays centrally owned rather than becoming a per-plugin,
    /// let alone per-method, config value.
    ///
    /// @return the node-wide concurrency ceiling for this service
    int globalConcurrencyCeiling();
}
