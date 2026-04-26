// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.roster.bootstrap.rsa.RsaRosterBootstrapPlugin;

/**
 * RSA Roster Bootstrap Plugin module.
 *
 * <p>Loads the consensus node roster (node IDs and RSA public keys) at Block Node startup and
 * makes it available to the proof verification layer via {@code BlockNodeContext}. The roster is
 * required to verify {@code SignedRecordFileProof} block proofs carried by Wrapped Record Blocks
 * (WRBs) during Phase 2a of the Hiero network upgrade.
 *
 * <p>On first startup the roster is loaded from {@code rsa-bootstrap-roster.pb} when present.
 * If absent, the plugin queries the Mirror Node REST API, persists the result, and makes the
 * roster available. If neither source succeeds the BN fails fast.
 *
 * <p>See {@code docs/design/wrb-streaming/bootstrap-roster-plugin.md} for the full design.
 */
module org.hiero.block.node.roster.bootstrap.rsa {
    // Export configuration classes to the config and app modules
    exports org.hiero.block.node.roster.bootstrap.rsa to
            com.swirlds.config.impl,
            com.swirlds.config.extensions,
            org.hiero.block.node.app;

    requires transitive org.hiero.block.node.spi;
    requires org.hiero.block.node.base;
    requires com.hedera.pbj.runtime;
    requires java.net.http;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            RsaRosterBootstrapPlugin;
}
