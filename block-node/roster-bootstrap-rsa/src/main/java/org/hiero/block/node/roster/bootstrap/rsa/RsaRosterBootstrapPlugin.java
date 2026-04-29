// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

/// Loads the consensus node RSA roster at Block Node startup and makes it available to the
/// proof verification layer via {@link BlockNodeContext}.
///
/// This plugin supports Phase 2a of the Hiero network upgrade (WRB streaming), during which
/// Consensus Nodes produce Wrapped Record Blocks (WRBs) carrying {@code SignedRecordFileProof}
/// proofs — a set of gossiped RSA signatures from every node in the current roster. The Block Node
/// must verify these proofs using RSA public keys for each signing node.
///
/// ## Startup sequence
///
/// 1. If a local bootstrap file exists at the configured path, load the roster from it.
/// 2. Otherwise, fetch the roster from the configured {@code RosterSource} (default: Hedera
///    Mirror Node {@code GET /api/v1/network/nodes}), then pass the result to
///    {@code ApplicationStateFacility.updateAddressBook()}, which handles file persistence and
///    notifies all plugins via {@code onContextUpdate}.
/// 3. If neither source succeeds and {@code roster.bootstrap.failOnFetchError=true} (default),
///    fail BN startup immediately with a clear error message.
///
/// ## No mid-instance reload
///
/// The roster is loaded once during {@link #start} and does not change for the lifetime of the
/// BN instance. Roster updates require a BN restart with a refreshed bootstrap file. Runtime
/// reload support is tracked separately.
///
/// ## RosterSource abstraction
///
/// The mechanism used to fetch the roster when no local file is present is abstracted behind a
/// {@code RosterSource} interface. The initial implementation queries the Mirror Node REST API.
/// Alternative sources (on-chain state, static file) can be added by providing a new
/// implementation and updating the {@code roster.bootstrap.rosterSource} configuration property.
///
/// See {@code docs/design/wrb-streaming/bootstrap-roster-plugin.md} for the full design.
public class RsaRosterBootstrapPlugin implements BlockNodePlugin {

    /// {@inheritDoc}
    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        // todo: store applicationStateFacility reference from context.applicationStateFacility()
        // todo: store BootstrapRosterConfig from context.configuration()
    }

    /// {@inheritDoc}
    @Override
    public void start() {
        // todo: check if local bootstrap file exists at config.filePath()
        //         if yes  → parse binary protobuf → validate ≥1 entry with RSA_PubKey + nodeId
        //                 → build NodeAddressBook
        //                 → call applicationStateFacility.updateAddressBook(book)
        //         if no   → build RosterSource (MirrorNodeRosterSource by default)
        //                 → fetch roster entries (paginated)
        //                 → build NodeAddressBook
        //                 → call applicationStateFacility.updateAddressBook(book)
        //                   (ApplicationStateFacility handles file persistence and onContextUpdate broadcast)
        // todo: if roster is empty and failOnFetchError=true → throw to abort BN startup
        // todo: emit roster_entries_loaded gauge metric (labelled by source: file | mirror_node)
    }
}
