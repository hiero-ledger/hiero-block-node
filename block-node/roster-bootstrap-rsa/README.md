# RSA Roster Bootstrap Plugin

**Module:** `org.hiero.block.node.roster.bootstrap.rsa`
**Plugin class:** `org.hiero.block.node.roster.bootstrap.rsa.RsaRosterBootstrapPlugin`
**Design doc:** [`docs/design/wrb-streaming/bootstrap-roster-plugin.md`](../../docs/design/wrb-streaming/bootstrap-roster-plugin.md)
**Implements:** [#2561](https://github.com/hiero-ledger/hiero-block-node/issues/2561)
**Part of epic:** [#2509](https://github.com/hiero-ledger/hiero-block-node/issues/2509) - Phase 2a WRB Streaming

---

## Purpose

This plugin loads the consensus node roster - a mapping of `node_id → RSA public key` - at Block
Node startup. The loaded roster is made available to all plugins ensuring the verification plugin
has the details to verify Wrapped Record Blocks (WRBs) carrying `SignedRecordFileProof` block proofs.

WRBs are produced by Consensus Nodes during **Phase 2a** of the Hiero network upgrade - the
transition period where Consensus Nodes emit RSA-signed record files alongside the block stream.
See the [design doc](../../docs/design/wrb-streaming/bootstrap-roster-plugin.md) for full context.
WRBs carry a set of gossiped RSA signatures from every node in the current roster as their block
proof. Without a populated roster the Block Node cannot accept any WRB, so the plugin fails
startup fast when the roster cannot be loaded.

---

## How it works

The plugin loads the roster through the following priority sequence:

1. **File-first:** On startup `BlockNodeApp.loadApplicationState()` checks for a local bootstrap file at
   `app.state.rsaBootstrapFilePath` (default `/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json`). If found,
   the roster is parsed and made available in `BlockNodeContext` before any plugin is started.
2. **Peer Block Node query:** If `roster.bootstrap.rsa.blockNodeSourcesPath` is set and the file exists,
   the plugin queries a peer Block Node via gRPC to retrieve the address book.
3. **Mirror Node fallback:** If no local file or peer query result is available and
   `roster.bootstrap.rsa.mirrorNodeBaseUrl` is set, the plugin queries the Hedera Mirror Node REST API
   (`GET /api/v1/network/nodes`, paginated, `order=desc`). The result is registered via
   `ApplicationStateFacility.updateAddressBookHistory()` for future restarts.
4. **Fail fast:** If `mirrorNodeBaseUrl` is configured but the Mirror Node is unreachable, startup is
   aborted with a clear error log. If both `mirrorNodeBaseUrl` and `blockNodeSourcesPath` are blank
   and no file is present, a WARNING is logged and the plugin exits without failing startup.
5. **No runtime reload:** The roster is loaded once and does not change for the lifetime of the
   BN instance. An address-book change requires a restart with a refreshed bootstrap file.

---

## Bootstrap file format

The bootstrap file is a **JSON serialization** of the `NodeAddressBook` protobuf message from
the Hedera API (`com.hedera.hapi.node.base.NodeAddressBook`), written and read via `NodeAddressBook.JSON`.

Only two fields from each `NodeAddress` entry are populated:

|    Field     | Proto field # |   Type   |                            Content                            |
|--------------|---------------|----------|---------------------------------------------------------------|
| `nodeId`     | 5             | `int64`  | Numeric node identifier                                       |
| `RSA_PubKey` | 4             | `string` | Raw hex-encoded DER X.509 RSA public key - **no** `0x` prefix |

No metadata fields (network name, generation timestamp, schema version) are embedded.
Operators wishing to annotate the file should maintain a separate sidecar.

**Default file path:** `/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json`
(Configured via `app.state.rsaBootstrapFilePath`.)

Generate this file before Phase 2a cutover using the operator script:

```bash
tools-and-tests/scripts/node-operations/generate-rsa-roster-bootstrap.sh \
  --network mainnet \
  --output /opt/hiero/block-node/application-state/rsa-bootstrap-roster.json
```

---

## Configuration

Bootstrap file path is configured in the `app.state` namespace (shared with other application state):

|             Property             |                               Default                               |                              Description                              |
|----------------------------------|---------------------------------------------------------------------|-----------------------------------------------------------------------|
| `app.state.rsaBootstrapFilePath` | `/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json` | Path to the local bootstrap file (JSON-serialized `NodeAddressBook`). |

Peer Block Node query is configured in the `roster.bootstrap.rsa` namespace:

|                        Property                        |   Default   |                                     Description                                      |
|--------------------------------------------------------|-------------|--------------------------------------------------------------------------------------|
| `roster.bootstrap.rsa.blockNodeSourcesPath`            | *(blank)*   | Path to a JSON file listing peer Block Nodes to query via gRPC. Leave blank to skip. |
| `roster.bootstrap.rsa.bnInitialQueryIntervalMillis`    | `5000`      | Interval (ms) between peer queries until an address book is found.                   |
| `roster.bootstrap.rsa.bnSubsequentQueryIntervalMillis` | `60000`     | Interval (ms) between peer queries after an address book is found.                   |
| `roster.bootstrap.rsa.enableTLS`                       | `false`     | Whether to enable TLS for peer gRPC connections.                                     |
| `roster.bootstrap.rsa.grpcOverallTimeout`              | `60000`     | Overall timeout (ms) for peer gRPC connections.                                      |
| `roster.bootstrap.rsa.maxIncomingBufferSize`           | `104857600` | Maximum incoming gRPC message buffer size in bytes (min 100 MiB, max 300 MiB).       |

Mirror Node fallback is configured in the `roster.bootstrap.rsa` namespace:

|                        Property                        |  Default  |                              Description                               |
|--------------------------------------------------------|-----------|------------------------------------------------------------------------|
| `roster.bootstrap.rsa.mirrorNodeBaseUrl`               | *(blank)* | Mirror Node base URL. Leave blank to disable MN fallback.              |
| `roster.bootstrap.rsa.mnInitialQueryIntervalMillis`    | `5000`    | Interval (ms) between queries to Mirror Node until address book found. |
| `roster.bootstrap.rsa.mnSubsequentQueryIntervalMillis` | `60000`   | Interval (ms) between queries to Mirror Node after address book found. |
| `roster.bootstrap.rsa.mirrorNodeConnectTimeoutSeconds` | `5`       | TCP connect timeout for Mirror Node calls.                             |
| `roster.bootstrap.rsa.mirrorNodeReadTimeoutSeconds`    | `10`      | Read timeout per Mirror Node request.                                  |
| `roster.bootstrap.rsa.mirrorNodePageSize`              | `100`     | Nodes per page for paginated Mirror Node calls (max 100).              |

---

## Relationship to `RosterBootstrapTssPlugin`

This plugin parallels `RosterBootstrapTssPlugin` in structure. Both plugins:

- Implement `BlockNodePlugin` and perform all work in `start()`.
- Use a JSON bootstrap file under the path configured in `app.state` managed by Application State Facility.
- Populate a field on `BlockNodeContext` for downstream consumers.

The RSA roster plugin handles Phase 2a verification. The TSS bootstrap plugin handles Phase 2b
(hinTS/BLS) verification. They are independent and can be deployed side-by-side during the
transition window.
