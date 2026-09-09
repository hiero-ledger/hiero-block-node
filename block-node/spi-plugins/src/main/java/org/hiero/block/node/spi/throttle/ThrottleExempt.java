// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

/// Implemented by a plugin's `ServiceInterface`, alongside it, to declare that this gRPC service is
/// deliberately not admission-controlled — as opposed to a service that simply forgot to implement
/// [ThrottleSpec]. Registration logs every gRPC service as throttled, exempt, or neither; a service
/// that is neither logs a warning, since an untagged, unthrottled service is far more likely to be
/// an oversight than an intentional choice. A service should implement this only when there's a
/// real reason not to admission-control it (e.g. an ingest path protected by a different mechanism
/// than the per-client rate/concurrency model this package implements).
public interface ThrottleExempt {}
