// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;

/// Identifies one `(method, weight class)` combination a [ThrottleSpec] declares per-client
/// settings for, see [ThrottleSpec#weightedPerClientSettings].
///
/// @param method the gRPC method name this entry governs, matching one of the throttled service's
///     own `ServiceInterface.Method#name()` values
/// @param weightClass the weight class this entry governs
public record MethodWeight(@NonNull String method, @NonNull WeightClass weightClass) {}
