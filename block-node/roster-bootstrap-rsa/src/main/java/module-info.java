// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.roster.bootstrap.rsa.RsaRosterBootstrapPlugin;

/// RSA Roster Bootstrap Plugin module.
///
/// Loads the consensus node roster (node IDs and RSA public keys) at Block Node startup and
/// makes it available to the proof verification layer via `BlockNodeContext`. The roster is
/// required to verify `SignedRecordFileProof` block proofs carried by Wrapped Record Blocks
/// (WRBs) during Phase 2a of the Hiero network upgrade.
///
/// On first startup the roster is loaded from `rsa-bootstrap-roster.pb` when present.
/// If absent, the plugin queries the Mirror Node REST API, persists the result, and makes the
/// roster available. If neither source succeeds the BN fails fast.
///
/// See `docs/design/wrb-streaming/bootstrap-roster-plugin.md` for the full design.
module org.hiero.block.node.roster.bootstrap.rsa {
    // Export configuration classes to the config and app modules
    exports org.hiero.block.node.roster.bootstrap.rsa to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires org.hiero.block.node.base;
    requires org.hiero.block.protobuf.pbj;
    requires org.hiero.metrics;
    requires com.google.gson;
    requires java.net.http;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            RsaRosterBootstrapPlugin;
}
