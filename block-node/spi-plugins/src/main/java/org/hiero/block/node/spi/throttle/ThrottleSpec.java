// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;

/// Implemented by a plugin's `ServiceInterface`, alongside it, to opt that service into per-client
/// admission control without a separate registration call. `ServiceBuilder.registerGrpcService`
/// checks `instanceof ThrottleSpec` and wraps the service automatically when present, resolving
/// settings from this interface rather than extra call-site arguments.
///
/// A service with more than one method may configure each independently, only some, or none at
/// all, see [#perClientSettings] and [#defaultPerClientSettings].
public interface ThrottleSpec {
    /// This service's per-client rate/concurrency settings, one entry per method this service
    /// wants gated individually, keyed by method name (e.g. `"serverStatus"`). A method with no
    /// entry here, and no [#defaultPerClientSettings], gets only its [#globalConcurrencyCeilings]
    /// entry, if any, and no per-client gate.
    ///
    /// @return the per-method settings map; may be empty
    @NonNull
    default Map<String, PerClientThrottleSettings> perClientSettings() {
        return Map.of();
    }

    /// A fallback applied to any method [#perClientSettings] doesn't cover, instead of leaving it
    /// with no per-client gate. Empty by default.
    ///
    /// @return the fallback per-client settings, or empty to leave unconfigured methods ungated at
    ///     the per-client layer
    @NonNull
    default Optional<PerClientThrottleSettings> defaultPerClientSettings() {
        return Optional.empty();
    }

    /// This service's node-wide concurrency ceilings, keyed by method name, read from the
    /// centrally-owned node-level config (`GlobalThrottleConfig` in the `app` module) rather than
    /// chosen by the plugin. Each method has its own ceiling. Every method with a per-client gate
    /// (explicit or via [#defaultPerClientSettings]) must have an entry; a method with an entry
    /// but no per-client gate is limited by its ceiling alone. A method in neither is not
    /// throttled.
    ///
    /// @return the node-wide concurrency ceiling per method name
    @NonNull
    default Map<String, Integer> globalConcurrencyCeilings() {
        return Map.of();
    }

    /// The content-aware weigher for this service. When present, the service is wrapped in a
    /// [WeightedThrottledServiceInterface] and admission is configured through
    /// [#weightedPerClientSettings] and [#weightedGlobalConcurrencyCeilings] instead of
    /// [#perClientSettings] and [#globalConcurrencyCeilings], which are then ignored.
    ///
    /// @return the weigher, or empty (the default) for a single-tier service
    @NonNull
    default Optional<ContentAwareWeigher> weigher() {
        return Optional.empty();
    }

    /// Per-client settings for a weighted service, one entry per `(method, weight class)`. Every
    /// configured method must include a [WeightClass#STANDARD] entry, which is also the fallback
    /// for any weight class with no entry of its own. Only used when [#weigher] is present.
    ///
    /// @return the per-`(method, weight class)` settings; may be empty
    @NonNull
    default Map<MethodWeight, PerClientThrottleSettings> weightedPerClientSettings() {
        return Map.of();
    }

    /// Node-wide concurrency ceilings for a weighted service, one per weight class, scoped to the
    /// service and shared by all its methods. Must include [WeightClass#STANDARD] and every weight
    /// class with per-client settings. Only used when [#weigher] is present.
    ///
    /// @return the node-wide ceiling per weight class
    @NonNull
    default Map<WeightClass, Integer> weightedGlobalConcurrencyCeilings() {
        return Map.of();
    }
}
