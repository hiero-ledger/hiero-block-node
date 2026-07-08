# Block Node Integration FAQ

Common integration and protocol questions for Consensus Node, Block Node, and Mirror
Node developers.

For term definitions, see the [Glossary](./glossary.md).
For operator questions, see the [Operator FAQ](./operator-faq.md).

---

## Publish stream protocol (CN → BN)

> **CN multi-BN routing:** When a stream closes due to an `EndOfStream` response, the
> CN does not necessarily reconnect to the same Block Node. If other Block Nodes are
> configured and eligible, the CN may route subsequent blocks to a higher-priority BN.
> A Block Node that generates repeated `EndOfStream` responses in a short window may be
> temporarily deprioritized by the CN before it is eligible to receive streams again.

### How does a Consensus Node initiate a publish stream to a Block Node?

The CN opens a **bidirectional gRPC stream** to
`BlockStreamPublishService.publishBlockStream`. The **first message in the stream must
be a `BlockHeader`** for the next expected block. The BN inspects the block number and
responds:

- If the block number is the next expected block → streaming continues normally.
- If the block number ≤ last verified block → BN responds with `DUPLICATE_BLOCK`.
- If the block number > next expected → BN responds with `BehindPublisher`.

The CN is always the client (initiator). The BN never dials a CN.

> See [Publish Block Stream](../design/communication-protocol/publish-block-stream.md) for more details.

### What does the Block Node send back during streaming?

The BN can send five response types during an active stream:

|        Response        |                    When sent                    |                What the CN should do                |
|------------------------|-------------------------------------------------|-----------------------------------------------------|
| `BlockAcknowledgement` | After block proof received and verified         | Continue streaming the next block                   |
| `SkipBlock`            | Another publisher already delivered this block  | Skip to the block indicated and continue            |
| `ResendBlock`          | BN needs the CN to resend from a specific block | Resend from the block number specified              |
| `BehindPublisher`      | BN is behind — CN is too far ahead              | Start a new stream from the block number specified  |
| `EndOfStream`          | Terminal — stream is closed                     | See the status code and take the appropriate action |

### What does `DUPLICATE_BLOCK` mean and what should the CN do?

`DUPLICATE_BLOCK` (code 5 in `EndOfStream`) means the block header the CN sent
corresponds to a block that is already stored and verified by the BN. The CN closes the
stream and resumes streaming — to this BN or a different available Block Node — beginning
with the block after the last persisted and verified block.

### What does `TOO_FAR_BEHIND` mean and what should the CN do?

`TOO_FAR_BEHIND` (code 4 in `EndOfStream`) means the BN is too far behind the CN to
catch up by streaming alone. The CN should:

1. Switch streaming to a different Block Node.
2. Include `earliest_block` and `latest_block` in the `EndStream` message so the
   falling-behind BN can attempt to backfill from a peer.
3. Resume streaming to this BN later once it has caught up via backfill.

### What does `PERSISTENCE_FAILED` mean and is the block lost?

`PERSISTENCE_FAILED` (code 7 in `EndOfStream`) means the BN failed to durably store
the block. **The block is not necessarily lost** — the CN still has it. The CN must:

1. Start a new stream and resend the block to this BN or a different reliable BN.
2. **Not discard the block** until it has been acknowledged as persisted and verified by
   at least one BN.

### What does `BAD_BLOCK_PROOF` mean?

`BAD_BLOCK_PROOF` (code 6 in `EndOfStream`) means a `BlockProof` item could not be
validated. The CN closes the stream and resumes streaming — to this BN or a different
available Block Node — from before the failed block.

### When does the Block Node send `TIMEOUT` to the Consensus Node?

`TIMEOUT` (code 4 in `EndOfStream`) is sent when the delay between stream items
exceeds the BN's configured timeout — the CN did not deliver the next item within the
allowed window. The CN closes the stream and resumes streaming — to this BN or a
different available Block Node — from before the timed-out block.

### Does the Block Node reconnect to the Consensus Node if the stream drops?

**No.** The Block Node is the server; it does not initiate connections. When a stream
closes (either orderly with `RESET`/`SUCCESS` or with an error code), the BN sends
`EndOfStream` and waits. The CN is responsible for opening a new stream.

---

## Subscribe stream protocol (MN → BN)

### How does a Mirror Node subscribe to the block stream?

The MN opens a **server-streaming gRPC call** to
`BlockStreamSubscribeService.subscribeBlockStream` with a `SubscribeStreamRequest`:

```protobuf
message SubscribeStreamRequest {
  uint64 start_block_number = 1;
  uint64 end_block_number   = 2;   // set to uint64 max for an indefinite stream
}
```

The BN responds with a stream of `SubscribeStreamResponse` messages containing
`block_items`, `end_of_block`, and finally a terminal `status` code when the stream
closes.

> See [Mirror Node Integration](./mirror-node-integration.md) for more details.

### What is the difference between `INVALID_START_BLOCK_NUMBER` and `NOT_AVAILABLE`?

|               Code               |                                                           Meaning                                                           |                              What to do                               |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| `INVALID_START_BLOCK_NUMBER` (4) | `start_block_number` is structurally invalid — negative, below `first_available_block`, or beyond the future-request window | Fix the request parameters; do not retry with the same value          |
| `NOT_AVAILABLE` (6)              | The block is not available on this BN at this time (e.g. pruned, or node is still backfilling)                              | Retry later with exponential backoff, or query a different Block Node |

### What should a subscriber do on `NOT_AVAILABLE`?

The subscriber **may retry with exponential backoff** against the same BN, or
**fall over to a lower-priority BN** that covers the requested block range. Check
`serverStatus` on available BNs to find one whose range includes the needed block.

Detect gaps client-side by comparing `end_of_block.block_number` against
`(last_committed_block + 1)`. Do not rely on the BN to signal gaps — the BN feeds
slow subscribers from history, not from the live stream, and does not produce gaps.

> See [Mirror Node Integration](./mirror-node-integration.md) for more details.

### How does the Block Node handle slow subscribers?

The BN **silently switches a slow subscriber session back to history** and streams from
the historical block store until the subscriber catches up to the live stream. Gaps are
never introduced by the BN — they can only come from the upstream unverified CN→BN
stream.

Blocks delivered from the live queue are unverified and may contain errors, be
repeated, or be incomplete. Subscribers are responsible for verifying each block using
its `BlockProof`.

> See [Mirror Node Integration](./mirror-node-integration.md) for more details.

---

## Block Access API

### What is the difference between `NOT_FOUND` and `NOT_AVAILABLE` in `getBlock`?

|        Code         |                                                           Meaning                                                            |
|---------------------|------------------------------------------------------------------------------------------------------------------------------|
| `NOT_FOUND` (4)     | The block **should** be available on this BN (within its managed range) but was not found — may be a transient storage issue |
| `NOT_AVAILABLE` (5) | The block is **outside** this BN's managed range — the BN has never held it or it has been pruned                            |

For `NOT_AVAILABLE`, call `serverStatus` to determine the BN's `firstAvailableBlock`
and `lastAvailableBlock`, then query a BN that covers the needed range.

### Is `stateSnapshot` implemented?

**No.** The `StateService.stateSnapshot` RPC is defined in the proto but not
implemented. The proto itself contains a REVIEW NOTE: *"This is TEMPORARY. We have not
yet designed how state snapshots may be sent."* There is no Java source plugin or
service implementation in the Block Node codebase, and neither the Mirror Node nor the
Consensus Node has any client code that calls this RPC.

Document any dependency on `stateSnapshot` as a future requirement.

---

## CN → BN wiring

### What is `block-nodes.json` and where does it live?

`block-nodes.json` is a JSON configuration file on the Consensus Node that identifies
which Block Node(s) to stream to. Location:

- Directory configured by `blockNode.blockNodeConnectionFileDir` in
  `application.properties` (default: `data/config`), relative to the CN working
  directory.
- File name **must** be exactly `block-nodes.json`.

> See [Configure Consensus Node Streaming](./operations/consensus-node-to-block-node-configuration.md) for more details.

### What values go in `block-nodes.json`?

```json
{
  "nodes": [
    {
      "address": "bn.example.com",
      "streamingPort": 40840,
      "servicePort": 40840,
      "priority": 0,
      "messageSizeSoftLimitBytes": 2097152,
      "messageSizeHardLimitBytes": 131072000
    }
  ]
}
```

|            Field            | Required |                                    Description                                    |
|-----------------------------|----------|-----------------------------------------------------------------------------------|
| `address`                   | ✅        | Hostname or IP of the Block Node                                                  |
| `streamingPort`             | ✅        | Port for block stream publishing (default 40840)                                  |
| `servicePort`               | ✅        | Port for service APIs like `serverStatus` (defaults to `streamingPort`)           |
| `priority`                  | ✅        | Lower value = higher priority; nodes with the same priority are selected randomly |
| `messageSizeSoftLimitBytes` | ❌        | Soft limit on per-request payload size; default 2,097,152 bytes (2 MB)            |
| `messageSizeHardLimitBytes` | ❌        | Hard limit on per-item payload size; default 131,072,000 bytes (125 MB)           |

> See [Configure Consensus Node Streaming](./operations/consensus-node-to-block-node-configuration.md) for more details.

### What is `writerMode` and when do I need `FILE_AND_GRPC` vs `GRPC`?

`blockStream.writerMode` in CN `application.properties` controls where the CN writes
block data:

|      Mode       |                                     Behaviour                                      |
|-----------------|------------------------------------------------------------------------------------|
| `FILE`          | Write to local disk only; Block Nodes receive nothing                              |
| `FILE_AND_GRPC` | Write to local disk **and** stream to configured Block Nodes (recommended default) |
| `GRPC`          | Stream to Block Nodes only; no local files written                                 |

Use `GRPC` when the CN should not retain local block files and all history is owned by
Block Nodes. Use `FILE_AND_GRPC` to keep a local backup while streaming.

> See [Configure Consensus Node Streaming](./operations/consensus-node-to-block-node-configuration.md) for more details.

### Does `block-nodes.json` require a CN restart to take effect?

**No.** The CN watches `block-nodes.json` for create, modify, and delete events and
reloads it live. Changes take effect immediately — existing connections are shut down
cleanly and new connections are established with the updated configuration. If the file
is missing or fails to parse, the CN logs a warning and stops establishing new
connections until a valid file is present.

> See [Configure Consensus Node Streaming](./operations/consensus-node-to-block-node-configuration.md) for more details.

### How do I use `solo block node add-external` instead of editing `block-nodes.json`?

For test and load-testing deployments, Solo can wire a temporary CN network to an
external Block Node before the CNs start:

```bash
solo block node add-external \
  --deployment <deployment-name> \
  --address <BN_IP>:<PORT>
```

Run this command after the network is initialised but **before** starting the Consensus
Nodes — this ensures the BN receives every block from block 0 onwards.

> See [Load Testing with Solo and NLG](./operations/load-testing-a-deployed-block-node-using-solo-and-nlg.md) for more details.

---

## On-chain registration (HIP-1137)

### How do I generate an `admin_key` for Block Node registration?

The `admin_key` is a **Hiero network key — not a generic EVM key or Hedera wallet key**.

**Key type:** Ed25519 (standard recommendation). Generated via `PrivateKey.generateED25519()` in any of the seven official Hiero SDKs (Java, JavaScript, Go, Rust, Swift, C++, Python). Use an HSM or KMS if your operational policy requires hardware-backed key custody.

**Critical rules:**
- The `admin_key` must **not** be tied to any network account. Entangling it with a treasury or transaction-signing account increases blast radius if the operator account is compromised.
- For **production**, use a `KeyList` or `ThresholdKey` (e.g. `ThresholdKey(2-of-3, three operator-controlled Ed25519 keys)`). Single-key control is acceptable for dev/test only.
- **If the `admin_key` is lost, there is no recovery mechanism.** Every create, update, and delete on a registered node must be signed by that node's `admin_key`. The most the network's governance structure can do is sign a `Delete` transaction to remove the orphaned registration, after which the operator must create a new one with a fresh `registered_node_id`.

**Example (JavaScript SDK):**

```javascript
const adminKey = PrivateKey.generateED25519();
// Store the private key securely — loss is unrecoverable
```

> See [Block Node On-Chain Registration](./block-node-on-chain-registration.md) for the
> full registration workflow, key rotation procedure, and production custody recommendations.

---

## WRB cutover and migration

### What changes for Mirror Nodes when the network transitions from record streams to block streams?

Mirror Nodes transition from **polling record files from cloud storage** to **subscribing
to a long-lived gRPC stream from a Block Node**:

|    Dimension     |         Before cutover (record stream)         |                                  After cutover (block stream)                                  |
|------------------|------------------------------------------------|------------------------------------------------------------------------------------------------|
| Transport        | Poll GCS / S3 buckets                          | Subscribe to Block Node via gRPC                                                               |
| Encoding         | `.rcd` + sidecar + signature files             | `BlockItem` messages in a single stream                                                        |
| Verification     | Per-node RSA signatures, majority verification | Single `BlockProof` per block                                                                  |
| Source discovery | Operator-configured bucket URL                 | On-chain registry ([HIP-1137](https://hips.hedera.com/hip/hip-1137)) or operator-supplied list |
| Switch trigger   | Manual reconfiguration                         | Automatic on HAPI version ≥ 0.76.0 (Mirror Node v0.155+)                                       |

> See [Record Stream to Block Stream Migration](./record-stream-to-block-stream-migration.md) for more details.

### What preparation steps does a Block Node need before WRB cutover?

1. Run all common pre-upgrade checks (health, storage, provisioner version, artifact
   backup).
2. **Clear the block store** — reset removes preview blocks incompatible with the WRB
   format.
3. Enable the `roster-bootstrap-rsa` plugin.
4. Configure the RSA bootstrap roster (via Mirror Node auto-fetch, peer BN query, or
   manual JSON file).
5. Configure backfill sources pointing at the Record Block History (RBH) Block Node.
6. Enable greedy backfill (`BACKFILL_GREEDY=true`) to pre-load WRB history before
   cutover.
7. Verify all readiness checks pass and `lastAvailableBlock` is advancing.

> See [Preparing for WRB Cutover](./operations/preparing-your-block-node-for-wrb-cutover.md) for more details.

---

## Versioning and compatibility

### Which Block Node version supports which Consensus Node version?

The Block Node is designed with long-term forward compatibility as a core goal — a given BN version is intended to remain
compatible with future CN versions for as long as practical. In practice this means the
minimum compatible BN version for a given CN release is expected to change very slowly
(for example, BN 1.0.0 might remain the minimum compatible version across many CN
releases).

That said, **always run the latest available Block Node release** — the minimum
compatible version is a floor, not a recommendation to stay pinned.

The current BN release is built and tested against the CN version pinned in
`hiero-dependency-versions/build.gradle.kts`:

```bash
# Find the CN version the current BN was built and tested against
grep "hederaVersion" hiero-dependency-versions/build.gradle.kts
# → val hederaVersion = "0.76.0-rc.1"
```

The E2E test suite (`solo-e2e-test.yml`) runs against the latest available version of
each component dynamically rather than pinned pairs. For production compatibility
guidance, consult your Hashgraph PoC.

---
