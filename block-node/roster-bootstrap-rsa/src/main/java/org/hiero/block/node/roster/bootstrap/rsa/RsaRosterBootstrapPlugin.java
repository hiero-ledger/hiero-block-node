// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

/**
 * Loads the consensus node RSA roster at Block Node startup and makes it available to the
 * proof verification layer via {@link BlockNodeContext}.
 *
 * <p>This plugin supports Phase 2a of the Hiero network upgrade (WRB streaming), during which
 * Consensus Nodes produce Wrapped Record Blocks (WRBs) carrying {@code SignedRecordFileProof}
 * proofs — a set of gossiped RSA signatures from every node in the current roster. The Block Node
 * must verify these proofs using RSA public keys for each signing node.
 *
 * <h2>Startup sequence</h2>
 * <ol>
 *   <li>If a local bootstrap file exists at the configured path, load the roster from it.</li>
 *   <li>Otherwise, fetch the roster from the configured {@code RosterSource} (default: Hedera
 *       Mirror Node {@code GET /api/v1/network/nodes}), persist the result to the bootstrap file
 *       for future startups, then make the roster available in context.</li>
 *   <li>If neither source succeeds and {@code roster.bootstrap.failOnFetchError=true} (default),
 *       fail BN startup immediately with a clear error message.</li>
 * </ol>
 *
 * <h2>No mid-instance reload</h2>
 * <p>The roster is loaded once during {@link #init} and does not change for the lifetime of the
 * BN instance. Roster updates require a BN restart with a refreshed bootstrap file. Runtime
 * reload support is tracked separately.
 *
 * <h2>RosterSource abstraction</h2>
 * <p>The mechanism used to fetch the roster when no local file is present is abstracted behind a
 * {@code RosterSource} interface. The initial implementation queries the Mirror Node REST API.
 * Alternative sources (on-chain state, static file) can be added by providing a new
 * implementation and updating the {@code roster.bootstrap.rosterSource} configuration property.
 *
 * <p>See {@code docs/design/wrb-streaming/bootstrap-roster-plugin.md} for the full design.
 */
public class RsaRosterBootstrapPlugin implements BlockNodePlugin {

    /** {@inheritDoc} */
    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        // todo: read BootstrapRosterConfig from context.configuration()
        // todo: check if local bootstrap file exists at config.filePath()
        //         if yes  → parse JSON → validate schema → build RsaRoster → update context
        //         if no   → build RosterSource (MirrorNodeRosterSource by default)
        //                 → fetch roster entries (paginated)
        //                 → write result to bootstrap file (non-fatal on write failure)
        //                 → build RsaRoster → update context
        // todo: if roster is empty and failOnFetchError=true → throw to abort BN startup
        // todo: update BlockNodeContext with the loaded RsaRoster
        // todo: emit roster_entries_loaded gauge metric (labelled by source: file | mirror_node)
    }
}
