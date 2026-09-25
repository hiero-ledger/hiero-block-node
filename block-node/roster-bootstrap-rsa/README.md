# RSA Roster Bootstrap Plugin

**Module:** `org.hiero.block.node.roster.bootstrap.rsa`
**Plugin class:** `org.hiero.block.node.roster.bootstrap.rsa.RsaRosterBootstrapPlugin`
**Design doc:** [`docs/design/wrb-streaming/bootstrap-roster-plugin.md`](../../docs/design/wrb-streaming/bootstrap-roster-plugin.md)

---

## Purpose

This plugin loads the consensus node roster - a mapping of `node_id → RSA public key` - at Block
Node startup. The loaded roster is made available to all plugins ensuring the verification plugin
has the details to verify Wrapped Record Blocks (WRBs) carrying `SignedRecordFileProof` block proofs.

WRBs are produced by Consensus Nodes during Phase 2a of the Hiero network upgrade. They carry a
set of gossiped RSA signatures from every node in the current roster as their block proof. Without
a populated roster the Block Node cannot verify any WRB. If no roster source is available (no
bootstrap file, no peer block node, and no Mirror Node configured), the plugin logs an INFO message
and continues without failing startup - operators must ensure at least one source is configured.

---

## How it works

1. **History file (pre-loaded by `BlockNodeApp`):** On startup `BlockNodeApp.loadApplicationState()` checks for a
   local bootstrap file at `app.state.rsaBootstrapFilePath`. If a full address-book history is found, the plugin
   records metrics and returns - no further fetching is scheduled.
2. **Legacy single-book file (pre-loaded by `BlockNodeApp`):** If a legacy single-book file is found instead, the
   plugin records metrics and schedules periodic peer block node and Mirror Node refreshes (see steps 3 and 4
   below).
3. **Peer block node gRPC query:** If `roster.bootstrap.rsa.blockNodeSourcesPath` is configured, the plugin queries
   peer block nodes concurrently at `bnInitialQueryIntervalMillis` intervals until a valid address book is received.
   On success, queries switch to `bnSubsequentQueryIntervalMillis` for periodic refresh.
4. **Mirror Node fallback:** If `roster.bootstrap.rsa.mirrorNodeBaseUrl` is configured, the plugin queries the
   Mirror Node REST API (`GET /api/v1/network/nodes`, paginated, `order=desc`) at `mnInitialQueryIntervalMillis`
   intervals concurrently with peer queries. On each failure an error is logged and the query is retried - the
   plugin does **not** abort startup. On success, queries switch to `mnSubsequentQueryIntervalMillis` for periodic
   refresh.
5. **Neither source configured:** If both `blockNodeSourcesPath` and `mirrorNodeBaseUrl` are blank and no file is
   present, an INFO message is logged and the plugin continues without failing startup.

---

## Bootstrap file format

The bootstrap file is a **JSON serialization** of the `NodeAddressBook` protobuf message from
`basic_types.proto` (`hiero-consensus-node`), written and read via `NodeAddressBook.JSON`.

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

Mirror Node fallback is configured in the `roster.bootstrap.rsa` namespace:

|                        Property                        |  Default  |                             Description                              |
|--------------------------------------------------------|-----------|----------------------------------------------------------------------|
| `roster.bootstrap.rsa.mirrorNodeBaseUrl`               | *(blank)* | Mirror Node base URL. Leave blank to disable MN fallback.            |
| `roster.bootstrap.rsa.mnInitialQueryIntervalMillis`    | `5000`    | Poll interval (ms) until the first address book is received from MN. |
| `roster.bootstrap.rsa.mnSubsequentQueryIntervalMillis` | `60000`   | Poll interval (ms) for periodic MN refreshes after initial success.  |
| `roster.bootstrap.rsa.mirrorNodeConnectTimeoutSeconds` | `5`       | TCP connect timeout for Mirror Node calls.                           |
| `roster.bootstrap.rsa.mirrorNodeReadTimeoutSeconds`    | `10`      | Read timeout per Mirror Node request.                                |
| `roster.bootstrap.rsa.mirrorNodePageSize`              | `100`     | Nodes per page for paginated Mirror Node calls (max 100).            |

Peer block node fallback is also configured in the `roster.bootstrap.rsa` namespace:

|                        Property                        |   Default   |                                 Description                                 |
|--------------------------------------------------------|-------------|-----------------------------------------------------------------------------|
| `roster.bootstrap.rsa.blockNodeSourcesPath`            | *(blank)*   | Path to a JSON file listing peer block nodes to query. Leave blank to skip. |
| `roster.bootstrap.rsa.bnInitialQueryIntervalMillis`    | `5000`      | Poll interval (ms) until the first address book is received from a peer BN. |
| `roster.bootstrap.rsa.bnSubsequentQueryIntervalMillis` | `60000`     | Poll interval (ms) for periodic peer BN refreshes after initial success.    |
| `roster.bootstrap.rsa.enableTLS`                       | `false`     | Whether to use TLS for peer gRPC connections.                               |
| `roster.bootstrap.rsa.grpcOverallTimeout`              | `60000`     | Overall timeout (ms) for peer gRPC calls.                                   |
| `roster.bootstrap.rsa.maxIncomingBufferSize`           | `104857600` | Maximum incoming gRPC message buffer size in bytes (min/default 100 MB).    |

---

## Relationship to `RosterBootstrapTssPlugin`

This plugin parallels `RosterBootstrapTssPlugin` in structure. Both plugins:

- Implement `BlockNodePlugin` and perform all work in `start()`.
- Can query peer block nodes via gRPC for bootstrap data (configured via `blockNodeSourcesPath` in their respective namespaces).
- Populate application state via `ApplicationStateFacility` for downstream consumers.

The RSA roster plugin additionally uses a local JSON bootstrap file (`app.state.rsaBootstrapFilePath`)
and a Mirror Node fallback; the TSS plugin reads TSS data from peer block nodes only.

The RSA roster plugin handles Phase 2a verification. The TSS bootstrap plugin handles Phase 2b
verification. They are independent and can be deployed side-by-side during the transition window.
