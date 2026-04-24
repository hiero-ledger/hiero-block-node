# RSA Roster Bootstrap Plugin

**Module:** `org.hiero.block.node.roster.bootstrap.rsa`
**Plugin class:** `org.hiero.block.node.roster.bootstrap.rsa.RsaRosterBootstrapPlugin`
**Design doc:** [`docs/design/wrb-streaming/bootstrap-roster-plugin.md`](../../docs/design/wrb-streaming/bootstrap-roster-plugin.md)
**Implements:** [#2561](https://github.com/hiero-ledger/hiero-block-node/issues/2561)
**Part of epic:** [#2509](https://github.com/hiero-ledger/hiero-block-node/issues/2509) — Phase 2a WRB Streaming

---

## Purpose

This plugin loads the consensus node roster — a mapping of `node_id → RSA public key` — at Block
Node startup. The loaded roster is made available to the proof verification layer so that Wrapped
Record Blocks (WRBs) carrying `SignedRecordFileProof` block proofs can be verified.

WRBs are produced by Consensus Nodes during Phase 2a of the Hiero network upgrade. They carry a
set of gossiped RSA signatures from every node in the current roster as their block proof. Without
a populated roster the Block Node cannot accept any WRB, so the plugin fails startup fast when
the roster cannot be loaded.

---

## How it works

1. **File-first:** On startup the plugin checks for a local bootstrap file at the configured path
   (default `data/config/rsa-bootstrap-roster.pb`). If found, the roster is loaded from that file.
2. **Mirror Node fallback:** If no local file is present, the roster is fetched from the Hedera
   Mirror Node REST API (`GET /api/v1/network/nodes`, paginated). The result is written to the
   bootstrap file for future restarts.
3. **Fail fast:** If neither source succeeds and `roster.bootstrap.failOnFetchError=true`
   (the default), startup is aborted with a clear error message. Set to `false` only in test
   environments.
4. **No runtime reload:** The roster is loaded once and does not change for the lifetime of the
   BN instance. An address-book change requires a restart with a refreshed bootstrap file.

---

## Bootstrap file format

The bootstrap file is a **binary protobuf serialization** of the `NodeAddressBook` message from
`basic_types.proto` (`hiero-consensus-node`). This reuses the on-chain address book format stored
at Hedera file `0.0.102`, avoiding a bespoke schema.

Only two fields from each `NodeAddress` entry are populated:

| Field | Proto field # | Type | Content |
|---|---|---|---|
| `nodeId` | 5 | `int64` | Numeric node identifier |
| `RSA_PubKey` | 4 | `string` | Raw hex-encoded DER X.509 RSA public key — **no** `0x` prefix |

No metadata fields (network name, generation timestamp, schema version) are embedded; the file is
a direct binary protobuf of `NodeAddressBook`. Operators wishing to annotate the file should
maintain a separate sidecar.

**Default file path:** `data/config/rsa-bootstrap-roster.pb`

Generate this file before Phase 2a cutover using the operator script:

```bash
tools-and-tests/scripts/node-operations/generate-roster-bootstrap.sh \
  --network mainnet \
  --output data/config/rsa-bootstrap-roster.pb
```

---

## Configuration

|                      Property                       |                    Default                     |                 Description                  |
|-----------------------------------------------------|------------------------------------------------|----------------------------------------------|
| `roster.bootstrap.enabled`                          | `true`                                         | Enable/disable the plugin.                        |
| `roster.bootstrap.filePath`                         | `data/config/rsa-bootstrap-rosterp.pb`              | Path to the local bootstrap file (binary protobuf). |
| `roster.bootstrap.proofMode`                        | `RSA`                                          | Accepted proof type: `RSA`, `TSS`, or `ANY`.      |
| `roster.bootstrap.rosterSource`                     | `MIRROR_NODE`                                  | Source when no local file exists.                 |
| `roster.bootstrap.failOnFetchError`                 | `true`                                         | Fail startup if roster cannot be loaded.          |
| `roster.bootstrap.mirrorNode.baseUrl`               | `https://mainnet-public.mirrornode.hedera.com` | Mirror Node base URL.                             |
| `roster.bootstrap.mirrorNode.connectTimeoutSeconds` | `5`                                            | Connect timeout for Mirror Node calls.            |
| `roster.bootstrap.mirrorNode.readTimeoutSeconds`    | `10`                                           | Read timeout for Mirror Node calls.               |
| `roster.bootstrap.mirrorNode.pageSize`              | `100`                                          | Nodes per page for paginated Mirror Node calls (max 100). |

---

## Relationship to `TssBootstrapPlugin`

This plugin parallels `TssBootstrapPlugin` in structure. Both plugins:

- Implement `BlockNodePlugin` and perform all work in `init()`.
- Use a binary protobuf bootstrap file in `data/config/`.
- Populate a field on `BlockNodeContext` for downstream consumers.

The RSA roster plugin handles Phase 2a verification. The TSS bootstrap plugin handles Phase 2b
(hinTS/BLS) verification. They are independent and can be deployed side-by-side during the
transition window.

---

## Follow-on tickets

|                                Ticket                                 |                                                                   Work                                                                   |
|-----------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| [#2561](https://github.com/hiero-ledger/hiero-block-node/issues/2561) | Implement `RsaRosterBootstrapPlugin`, `MirrorNodeRosterSource`, `BootstrapRosterConfig`, and extend `BlockNodeContext` with `RsaRoster`. |
| [#2562](https://github.com/hiero-ledger/hiero-block-node/issues/2562) | Implement RSA `SignedRecordFileProof` verification in `BlockVerificationService`.                                                        |
| [#2563](https://github.com/hiero-ledger/hiero-block-node/issues/2563) | Implement `generate-roster-bootstrap.sh` operator script and Phase 2a cutover runbook.                                                   |
