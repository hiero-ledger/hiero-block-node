# RSA Roster Bootstrap Plugin

**Module:** `org.hiero.block.node.roster.bootstrap.rsa`
**Plugin class:** `org.hiero.block.node.roster.bootstrap.rsa.RsaRosterBootstrapPlugin`
**Design doc:** [`docs/design/wrb-streaming/bootstrap-roster-plugin.md`](../../docs/design/wrb-streaming/bootstrap-roster-plugin.md)
**Implements:** [#2561](https://github.com/hiero-ledger/hiero-block-node/issues/2561)
**Part of epic:** [#2509](https://github.com/hiero-ledger/hiero-block-node/issues/2509) — Phase 2a WRB Streaming

---

## Purpose

This plugin loads the consensus node roster — a mapping of `node_id → RSA public key` — at Block
Node startup. The loaded roster is made available to all plugins ensuring the verification plugin
has the details to verify Wrapped Record Blocks (WRBs) carrying `SignedRecordFileProof` block proofs.

WRBs are produced by Consensus Nodes during Phase 2a of the Hiero network upgrade. They carry a
set of gossiped RSA signatures from every node in the current roster as their block proof. Without
a populated roster the Block Node cannot accept any WRB, so the plugin fails startup fast when
the roster cannot be loaded.

---

## How it works

1. **File-first:** On startup `BlockNodeApp.loadApplicationState()` checks for a local bootstrap file at
   `app.state.rsaBootstrapFilePath` (default `/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json`). If found,
   the roster is parsed and made available in `BlockNodeContext` before any plugin is started.
2. **Mirror Node fallback:** If no local file is present, the plugin queries the Hedera Mirror Node REST API
   (`GET /api/v1/network/nodes`, paginated, `order=desc`). The result is written to the bootstrap file via
   `ApplicationStateFacility.updateAddressBook()` for future restarts.
3. **Fail fast on Mirror Node error:** If `roster.bootstrap.rsa.mirrorNodeBaseUrl` is configured but the Mirror Node
   is unreachable after 3 retries, startup is aborted with a clear error log. If `mirrorNodeBaseUrl` is left blank
   and no file is present, a WARNING is logged and the plugin exits without failing startup.
4. **No runtime reload:** The roster is loaded once and does not change for the lifetime of the
   BN instance. An address-book change requires a restart with a refreshed bootstrap file.

---

## Bootstrap file format

The bootstrap file is a **JSON serialization** of the `NodeAddressBook` protobuf message from
`basic_types.proto` (`hiero-consensus-node`), written and read via `NodeAddressBook.JSON`.

Only two fields from each `NodeAddress` entry are populated:

|    Field     | Proto field # |   Type   |                            Content                            |
|--------------|---------------|----------|---------------------------------------------------------------|
| `nodeId`     | 5             | `int64`  | Numeric node identifier                                       |
| `RSA_PubKey` | 4             | `string` | Raw hex-encoded DER X.509 RSA public key — **no** `0x` prefix |

No metadata fields (network name, generation timestamp, schema version) are embedded.
Operators wishing to annotate the file should maintain a separate sidecar.

**Default file path:** `/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json`
(Configured via `app.state.rsaBootstrapFilePath`.)

Generate this file before Phase 2a cutover using the operator script:

```bash
tools-and-tests/scripts/node-operations/generate-roster-bootstrap.sh \
  --network mainnet \
  --output /opt/hiero/block-node/application-state/rsa-bootstrap-roster.json
```

---

## Configuration

Bootstrap file path is configured in the `app.state` namespace (shared with other application state):

|             Property             |                               Default                               |                              Description                              |
|----------------------------------|---------------------------------------------------------------------|-----------------------------------------------------------------------|
| `app.state.rsaBootstrapFilePath` | `/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json` | Path to the local bootstrap file (JSON-serialized `NodeAddressBook`). |

Mirror Node fallback is configured in the `roster.bootstrap.rsa` namespace:

|                        Property                        |  Default  |                        Description                        |
|--------------------------------------------------------|-----------|-----------------------------------------------------------|
| `roster.bootstrap.rsa.mirrorNodeBaseUrl`               | *(blank)* | Mirror Node base URL. Leave blank to disable MN fallback. |
| `roster.bootstrap.rsa.mirrorNodeConnectTimeoutSeconds` | `5`       | TCP connect timeout for Mirror Node calls.                |
| `roster.bootstrap.rsa.mirrorNodeReadTimeoutSeconds`    | `10`      | Read timeout per Mirror Node request.                     |
| `roster.bootstrap.rsa.mirrorNodePageSize`              | `100`     | Nodes per page for paginated Mirror Node calls (max 100). |

---

## Relationship to `RosterBootstrapTssPlugin`

This plugin parallels `RosterBootstrapTssPlugin` in structure. Both plugins:

- Implement `BlockNodePlugin` and perform all work in `start()`.
- Use a JSON bootstrap file under the path configured in `app.state` managed by Application State Facility.
- Populate a field on `BlockNodeContext` for downstream consumers.

The RSA roster plugin handles Phase 2a verification. The TSS bootstrap plugin handles Phase 2b
(hinTS/BLS) verification. They are independent and can be deployed side-by-side during the
transition window.

---

## Follow-on tickets

|                                Ticket                                 |                                                                                        Work                                                                                        |
|-----------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [#2561](https://github.com/hiero-ledger/hiero-block-node/issues/2561) | Implement `RsaRosterBootstrapPlugin` and `RsaRosterBootstrapConfig`; extend `ApplicationStateFacility` with `updateAddressBook()` and `BlockNodeContext` with `nodeAddressBook()`. |
| [#2562](https://github.com/hiero-ledger/hiero-block-node/issues/2562) | Implement RSA `SignedRecordFileProof` verification in `BlockVerificationService`.                                                                                                  |
| [#2660](https://github.com/hiero-ledger/hiero-block-node/issues/2660) | Implement `generate-roster-bootstrap.sh` operator script and Phase 2a cutover runbook.                                                                                             |
