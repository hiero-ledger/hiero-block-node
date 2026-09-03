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
/// A plugin with a single cost tier (e.g. `serverStatus`) returns a one-entry map keyed by
/// [WeightClass#STANDARD] and leaves [#weigher()] empty. A plugin with more than one cost tier (e.g.
/// `getBlock`'s live-vs-historical distinction) returns one entry per weight class its weigher can
/// classify into, and supplies that weigher.
public interface ThrottleSpec {
    /// This service's per-client rate/concurrency settings, one entry per weight class its
    /// [#weigher()] can classify a request into. Must contain an entry for [WeightClass#STANDARD],
    /// used as the fallback if the weigher ever returns a class with no configured entry, and as the
    /// only entry for a service with no weigher.
    ///
    /// @return the per-weight-class settings map; never empty
    @NonNull
    Map<WeightClass, PerClientThrottleSettings> perClientSettingsByWeight();

    /// This service's node-wide concurrency ceiling, one entry per key in
    /// [#perClientSettingsByWeight]. A ceiling here represents an allocation of the node's total
    /// shared capacity across every throttled API — the plugin doesn't choose this number itself, it
    /// reads it from the centrally-owned, node-level config (`GlobalThrottleConfig` in the `app`
    /// module) and reports it here, the same way it already reads its own per-client settings from
    /// its own config record. Two or more weight classes may share the same ceiling value (e.g. a
    /// standing session resource that draws from one shared budget regardless of classification) —
    /// see `docs/design/apis/api-throttling.md` ("Configuration ownership") for the full rationale
    /// on why this stays centrally owned rather than becoming a per-plugin config value.
    ///
    /// @return the per-weight-class node-wide ceiling map; never empty, same key set as
    ///     [#perClientSettingsByWeight]
    @NonNull
    Map<WeightClass, Integer> globalConcurrencyCeilings();

    /// Classifies each call's request content into a weight class, once its content is available.
    /// Empty for a service with a single cost tier — every call is then treated as
    /// [WeightClass#STANDARD], decided synchronously at admission time rather than deferred until
    /// the request body arrives (see `ThrottledServiceInterface` vs `WeightedThrottledServiceInterface`
    /// for why this distinction matters for latency on simple, single-tier calls).
    ///
    /// @return the weigher, or empty for a single-tier service
    @NonNull
    default Optional<ContentAwareWeigher> weigher() {
        return Optional.empty();
    }
}
