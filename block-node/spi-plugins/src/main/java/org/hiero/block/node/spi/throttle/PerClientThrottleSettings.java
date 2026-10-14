// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

/// Per-client throttle settings a plugin declares for one gRPC method. Excludes the node-wide
/// concurrency ceiling, which is a separate, centrally-owned concern (see
/// [GlobalConcurrencyGate]).
///
/// @param ratePerSecond sustained requests (or, for streaming APIs, new session opens) per second
///     allowed for one client
/// @param burstTolerance how far ahead of the even-pacing schedule a client's request may arrive
///     and still be admitted, expressed as a multiple of the pacing interval
/// @param maxConcurrentPerClient maximum concurrent in-flight calls/sessions for one client on
///     this method
public record PerClientThrottleSettings(int ratePerSecond, int burstTolerance, int maxConcurrentPerClient) {}
