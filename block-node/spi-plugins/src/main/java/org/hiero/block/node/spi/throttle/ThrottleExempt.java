// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

/// Implemented by a plugin's `ServiceInterface`, alongside it, to declare that this gRPC service is
/// deliberately not admission-controlled, as opposed to one that simply forgot to implement
/// [ThrottleSpec]. Registration logs every service as throttled, exempt, or neither; an untagged
/// service logs a warning since that's more likely an oversight than an intentional choice.
public interface ThrottleExempt {}
