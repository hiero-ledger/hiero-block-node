// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

/// The per-client throttle settings a plugin declares for one gRPC method, in that plugin's own
/// configuration. Does **not** include the node-wide concurrency ceiling: that is a node-level
/// capacity-allocation concern, resolved separately and merged in by the service-registration
/// point into a full [ThrottlePolicy]. See `docs/design/apis/api-throttling.md` ("Configuration
/// ownership") for the rationale behind this split.
///
/// @param ratePerSecond sustained requests (or, for streaming APIs, new session opens) per second
///     allowed for one client
/// @param burstTolerance how far ahead of the even-pacing schedule a client's request may arrive
///     and still be admitted, expressed as a multiple of the pacing interval
/// @param maxConcurrentPerClient maximum concurrent in-flight calls/sessions for one client on
///     this method
public record PerClientThrottleSettings(int ratePerSecond, int burstTolerance, int maxConcurrentPerClient) {}
