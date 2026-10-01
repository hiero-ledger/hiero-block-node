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
/// A plugin with a single cost tier and a single method (e.g. `serverStatus`) returns a one-entry
/// map keyed by `(that method, WeightClass#STANDARD)` and leaves [#weigher()] empty. A plugin with
/// more than one cost tier (e.g. `getBlock`'s live-vs-historical distinction) returns one entry per
/// weight class its weigher can classify into, keyed by its one method, and supplies that weigher.
///
/// A service with more than one method (e.g. `BlockNodeService`'s `serverStatus` and
/// `serverStatusDetail`) may configure each method independently, configure only some of them, or
/// configure none at all — see [#perClientSettings] and [#defaultPerClientSettings].
public interface ThrottleSpec {
    /// This service's per-client rate/concurrency settings, one entry per `(method, weight
    /// class)` combination this service wants gated individually. A method with no entry here —
    /// and no [#defaultPerClientSettings] configured either — gets **no per-client gate at all**:
    /// its calls are still subject to [#globalConcurrencyCeilings] (the node-wide ceiling is
    /// mandatory and always shared, regardless of per-client configuration), but nothing tracks
    /// any individual client's rate or concurrency for it.
    ///
    /// For a method with a [#weigher], at least one entry for that method must use
    /// [WeightClass#STANDARD], used as the fallback if the weigher ever classifies a call into a
    /// weight class with no configured entry for that method.
    ///
    /// @return the per-`(method, weight class)` settings map; may be empty
    @NonNull
    Map<MethodWeight, PerClientThrottleSettings> perClientSettings();

    /// A fallback applied to any of this service's methods that [#perClientSettings] doesn't
    /// cover, instead of leaving them with no per-client gate. Empty by default, which preserves
    /// the behavior described in [#perClientSettings]: an unconfigured method is governed only by
    /// the shared [#globalConcurrencyCeilings].
    ///
    /// This exists so a plugin can later give every otherwise-unconfigured method a shared,
    /// generous default instead of skipping per-client gating entirely — a config/wiring change
    /// for that one plugin, not a change to the admission mechanism itself. Only applies to
    /// methods with no [ContentAwareWeigher] classifying them (i.e. [WeightClass#STANDARD]); a
    /// weighed method with no entry of its own still falls through to the shared gate only, since
    /// there is no single weight class a "default" could unambiguously apply to.
    ///
    /// @return the fallback per-client settings, or empty to leave every otherwise-unconfigured
    ///     method ungated at the per-client layer
    @NonNull
    default Optional<PerClientThrottleSettings> defaultPerClientSettings() {
        return Optional.empty();
    }

    /// This service's node-wide concurrency ceiling, one entry per weight class used by any entry
    /// in [#perClientSettings] (plus [WeightClass#STANDARD] for a service with no weigher). A
    /// ceiling here represents an allocation of the node's total shared capacity across every
    /// throttled API — the plugin doesn't choose this number itself, it reads it from the
    /// centrally-owned, node-level config (`GlobalThrottleConfig` in the `app` module) and reports
    /// it here, the same way it already reads its own per-client settings from its own config
    /// record.
    ///
    /// This ceiling is **shared by every method at that weight class**, configured or not — it is
    /// not split per method. See `docs/design/apis/api-throttling.md` ("Configuration ownership")
    /// for the full rationale on why this stays centrally owned rather than becoming a per-plugin,
    /// let alone per-method, config value.
    ///
    /// @return the per-weight-class node-wide ceiling map; never empty
    @NonNull
    Map<WeightClass, Integer> globalConcurrencyCeilings();

    /// Classifies each call's request content into a weight class, once its content is available.
    /// Empty for a service with no method needing more than one cost tier — every call to such a
    /// method is then treated as [WeightClass#STANDARD], decided synchronously at admission time
    /// rather than deferred until the request body arrives (see `ThrottledServiceInterface` vs
    /// `WeightedThrottledServiceInterface` for why this distinction matters for latency on
    /// simple, single-tier calls).
    ///
    /// @return the weigher, or empty for a service with no method needing one
    @NonNull
    default Optional<ContentAwareWeigher> weigher() {
        return Optional.empty();
    }
}
