# Network Latency Scenarios

Long-form companion to the [README's Network Chaos section](../README.md#network-chaos--latency-tests). Use this when you want to know *why* each test exists, *what* the thresholds are, and *how* to add new scenarios.

The solo-e2e-test harness can inject configurable network latency in three dimensions:

| Dimension | Source selector | Target selector | Affects |
|-----------|-----------------|-----------------|---------|
| **CN ↔ CN** | `solo.hedera.com/type=network-node` | `solo.hedera.com/type=network-node`     | Gossip / consensus rounds |
| **BN ↔ BN** | `block-node.hiero.com/type=block-node` | `block-node.hiero.com/type=block-node` | Peer backfill mesh |
| **CN ↔ BN** | `solo.hedera.com/type=network-node` | `block-node.hiero.com/type=block-node`  | Live block-publish stream |

> Why two label namespaces? CN pods carry `solo.hedera.com/*` labels emitted by Solo CLI. BN pods carry `block-node.hiero.com/*` labels emitted by the BN Helm chart. See `agent/proposals/solo-chaos-spike/findings/002-bn-label-namespace-mismatch.md`.

## How the tests are shaped

Each latency test follows the same shape and runs in ~7 minutes:

```yaml
events:
  - load-start                    (t=0)
  - inject-latency                (t=30,  starts the chaos window)
  - clear-latency                 (t=330, ends it after 5 min)
  - load-stop                     (t=360)
assertions:
  - all-healthy                   (every BN pod is Running)
  - all-have-blocks               (every BN has block 0..N)
  - blocks-increasing             (every BN observes Δblocks > 0)
  - block-rate-floor              (Δblocks/Δtime ≥ 0.3 blocks/s)
  - no-stream-errors  (cn-cn only — sample showing the metric-threshold primitive)
```

The 0.3 blocks/s floor is intentionally conservative. On the paired-3 topology used during Phase 2 validation, healthy block production averaged ~0.47 blocks/s under modest NLG load, so 0.3 is a comfortable margin — high enough to catch a real stall (the assertion correctly reports 0.000/s on a stalled cluster) and low enough to absorb chaos-induced micro-slowdowns.

## The four scenarios

### `tests/latency-cn-to-cn.yaml`

**Simulates:** CNs gossiping across a slow inter-region link.

**Parameters:**
- Latency: 100 ms ± 20 ms
- Direction: bidirectional (`direction: both` on the NetworkChaos)
- Chaos window: 5 min
- Selectors: `solo.hedera.com/type=network-node` on both sides

**Expected behavior:** Consensus rounds take longer, but the network still produces blocks. BNs continue receiving blocks at roughly the same cadence as their upstream CN can publish — typically 0.4–0.5 blocks/s after the chaos clears.

**Assertions:**
- `node-healthy`, `block-available`, `blocks-increasing` (existing)
- `block-rate-floor` ≥ 0.3 blocks/s over 30 s
- `metric-threshold blocknode_publisher_stream_errors_total == 0` (exemplifies the generic primitive)

**Pass evidence from Phase 2 validation (paired-3):**
- BN-1: 465 → 479 (+14 in 30 s)
- BN-2: 479 → 494 (+15 in 30 s)
- BN-3: 494 → 508 (+14 in 30 s)

### `tests/latency-bn-to-bn.yaml`

**Simulates:** Block Nodes in different regions communicating over a slow peer mesh.

**Parameters:**
- Latency: 200 ms ± 40 ms
- Direction: bidirectional
- Chaos window: 5 min
- Selectors: `block-node.hiero.com/type=block-node` on both sides

**Expected behavior:** Live CN-to-BN streaming is **unaffected** (CN egress doesn't pass through any BN-to-BN rule). BN peer backfill is slowed. On paired-3, each BN already receives its blocks directly from a paired CN so backfill is not the critical path — blocks continue to land at the same rate.

**Assertions:** same set as CN↔CN, minus `no-stream-errors` (the CN-publisher stream is independent of BN-to-BN traffic).

**Pass evidence (paired-3):**
- BN-1: 957 → 972 (+15 in 30 s)

**Note:** solo-chaos does not ship a built-in BN↔BN template. This test relies on the parametric template at `scripts/chaos-templates/network-latency.yaml.tmpl`. See `agent/proposals/solo-chaos-spike/issues/001-bn-to-bn-netem-template.md` for the upstream feature request.

### `tests/latency-cn-to-bn.yaml`

**Simulates:** The production-realistic case — CNs and BNs sitting in different regions, with the gRPC block-publish stream crossing a slow link.

**Parameters:**
- Latency: 150 ms ± 30 ms
- Direction: bidirectional
- Chaos window: 5 min
- Selectors: `solo.hedera.com/type=network-node` → `block-node.hiero.com/type=block-node`

**Expected behavior:** Each block round-trip through the publisher gRPC stream pays the latency cost both ways. The BN's ACK takes ~300 ms longer than baseline, which can affect publisher flow control. Blocks still flow but the publisher gauge `blocknode_publisher_open_connections` should stay non-zero throughout.

**Assertions:** same set as CN↔CN minus `no-stream-errors` (the CN-publisher stream may transiently reconnect under high latency; making this strict could flake).

**Pass evidence (paired-3):**
- BN-1: 1184 → 1198 (+14 in 30 s)

### `tests/latency-all-three.yaml`

**Simulates:** Worst-case "everything is in different regions" — three concurrent NetworkChaos rules layered on the same cluster.

**Parameters:**
- CN↔CN: 100 ms ± 20 ms
- BN↔BN: 200 ms ± 40 ms
- CN↔BN: 150 ms ± 30 ms
- All bidirectional
- Chaos window: 5 min

**Expected behavior:** Each pod accumulates the netem rules that apply to its labels. The cluster's egress paths are simultaneously throttled along every dimension. This is the strongest assertion that the foundation's coexistence guarantee holds — Chaos Mesh applies all three rules without interference.

**Assertions:** same set as CN↔CN minus `no-stream-errors`.

**Pass evidence (paired-3):**
- BN-1: 1541 → 1555 (+14 in 30 s) with three concurrent rules active

## Known limitations

- **Post-chaos measurement only.** The runner runs assertions *after* all events complete, so `block-rate-floor` measures recovery, not under-chaos rate. To measure under-chaos rate we'd need an event-time assertion. Tracked as a future enhancement.
- **Tests are predicated on a healthy cluster.** The `paired-3` cluster has been observed to occasionally enter a stalled state where consensus stops producing blocks (one observed cause: CN-1 restart; another: fresh cluster with consensus already wedged at a low block).
  - When that happens, `block-rate-floor` reports `0.000/s` — that's the assertion correctly detecting the upstream problem, not a test flake.
  - Before chalking a latency-test failure up to the test itself, sanity-check the cluster: `task verify`, or `kubectl exec -n solo-network network-node1-0 -c root-container -- …` for HAPI logs.
  - Reliable signal we have today: 8 of 8 latency tests pass back-to-back on a healthy cluster (`iter1` + `iter2` of a 3× sweep). The cluster-stability issue is upstream of the chaos work and is being investigated separately.
- **NLG lock contention between runs.** Solo CLI's lock can persist across consecutive test runs in the same cluster session. `load-stop` recovers via direct Helm uninstall, but the following `load-start` may fail. Tests still pass because consensus produces blocks via heartbeats. See `agent/proposals/solo-chaos-spike/findings/003-nlg-solo-cli-lock-contention.md`.
- **Histogram metrics absent on the BN.** True p99 of block-arrival interval isn't computable from the metrics the BN exports today. `block-rate-floor` is the rate-based proxy that ships. See `agent/proposals/solo-chaos-spike/findings/004-bn-lacks-histogram-metrics-for-block-arrival.md`.
- **TSS coupling.** `TSS_ENABLED=true` (the harness default) requires CN ≥ v0.74.0-0. Until `CN_VERSION=latest` resolves to a TSS-capable tag, the latency tests need the override `TSS_ENABLED=false` on `task up`. Drop the override once your default CN tag is TSS-capable. See `findings/001` for context.

## Extending — adding a new latency scenario

A new scenario is typically <30 lines of YAML. Use one of the existing tests as a starting point:

```yaml
name: latency-my-scenario
description: "Brief description of what this exercises"

events:
  - id: load-start
    type: load-start
    delay: 0
    args: { concurrency: 5, accounts: 10, duration: 420 }

  - id: inject
    type: inject-latency
    delay: 30
    args:
      name: my-rule                      # used to build the NetworkChaos resource name
      source: { kind: network-node }     # or block-node
      target: { kind: block-node }
      latency: 75ms                      # any netem-compatible duration
      jitter: 15ms
      bidirectional: true
      # optional: loss: 1%               # adds netem packet-loss to the rule

  - id: clear
    type: clear-latency
    delay: 330
    args: { name: my-rule }

  - id: load-stop
    type: load-stop
    delay: 360

assertions:
  - id: all-healthy
    type: node-healthy
    target: all

  - id: blocks-increasing
    type: blocks-increasing
    target: all
    args: { wait_seconds: 30, max_attempts: 3 }

  - id: block-rate-floor
    type: block-rate-floor
    target: all
    args: { min_rate_per_sec: 0.3, window_seconds: 30 }
```

Then validate (no cluster required):

```bash
task test:validate TEST_FILE=tests/latency-my-scenario.yaml
```

And run (requires a running cluster, chaos-mesh installed, and `CHAOS_ENABLED=true`):

```bash
CHAOS_ENABLED=true TOPOLOGY=paired-3 task test:run TEST_FILE=tests/latency-my-scenario.yaml
```

## Pointers

- Foundation design: `agent/proposals/solo-chaos-spike/00-proposal.md`
- Phase 0 spike results: `agent/proposals/solo-chaos-spike/02-phase0-spike-results.md`
- Upstream Chaos Mesh wrapper this builds on: [solo-chaos](https://github.com/hashgraph/solo-chaos)
- Chaos Mesh NetworkChaos reference: https://chaos-mesh.org/docs/define-chaos-experiment-scope/
