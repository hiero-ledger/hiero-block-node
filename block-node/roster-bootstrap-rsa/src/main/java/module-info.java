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
 * <p>On first startup the roster is fetched from the configured {@code RosterSource} (default:
 * Hedera Mirror Node REST API) and persisted as a local JSON bootstrap file. Subsequent startups
 * load directly from that file, avoiding a network call.
 *
 * <p>See {@code docs/design/wrb-streaming/bootstrap-roster-plugin.md} for the full design.
 */
module org.hiero.block.node.roster.bootstrap.rsa {
    requires transitive org.hiero.block.node.spi;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            RsaRosterBootstrapPlugin;
}
