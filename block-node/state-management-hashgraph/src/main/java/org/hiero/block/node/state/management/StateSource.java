// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

/// Selects which version of state a read targets.
///
/// The plugin keeps more than one version of state alive at once. The gRPC
/// query API only ever reads `IMMUTABLE`; the other sources exist so that
/// in-process callers (future plugin-to-plugin reads via a `BinaryStateReader`
/// SPI) can opt into freshness or, later, point reads at a persisted snapshot.
public enum StateSource {
    /// The latest network-attested immutable state — the state confirmed by a
    /// block footer. This is the only source the gRPC query API serves, and the
    /// safe default for any consumer that must not act on unattested state.
    IMMUTABLE,

    /// The latest applied-but-not-yet-attested state. Lets an in-process caller
    /// read the freshest state ahead of footer attestation, accepting that the
    /// network has not yet confirmed it. Never served over gRPC.
    MUTABLE,

    /// A historical state recovered from a persisted snapshot (e.g. a recent
    /// snapshot directory). Not yet supported — reads against this source throw
    /// until the snapshot-backed read path is implemented (see the BinaryStateReader
    /// SPI follow-up ticket).
    HISTORICAL
}
