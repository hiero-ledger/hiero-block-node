# Network Latency Scenarios

Long-form companion to the [README's Network Chaos section](../README.md#network-chaos--latency-tests). Use this when you want to know *why* each test exists, *what* the thresholds are, *how to choose a profile*, and *how* to add new scenarios.

The solo-e2e-test harness can inject configurable network latency in three dimensions:

|  Dimension  |            Source selector             |            Target selector             |          Affects          |
|-------------|----------------------------------------|----------------------------------------|---------------------------|
| **CN ↔ CN** | `solo.hedera.com/type=network-node`    | `solo.hedera.com/type=network-node`    | Gossip / consensus rounds |
| **BN ↔ BN** | `block-node.hiero.com/type=block-node` | `block-node.hiero.com/type=block-node` | Peer backfill mesh        |
| **CN ↔ BN** | `solo.hedera.com/type=network-node`    | `block-node.hiero.com/type=block-node` | Live block-publish stream |

> **Two label namespaces:** CN pods carry `solo.hedera.com/*` labels (emitted by Solo CLI); BN pods carry `block-node.hiero.com/*` labels (emitted by the BN Helm chart). Each side of a rule must select with the matching namespace.

## Choosing a latency profile (baseline / stress / severe)

There are three tiers. They share the same event shape and assertions vocabulary; they differ in the **magnitude** of injected latency and in what the assertions are checking for.

|   Profile    |                                  Test file(s)                                   |         Latency (CN↔CN / CN↔BN / BN↔BN)          |                                  What it answers                                   |                                                                Assertion intent                                                                 |
|--------------|---------------------------------------------------------------------------------|--------------------------------------------------|------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| **baseline** | `latency-cn-to-cn`, `latency-cn-to-bn`, `latency-bn-to-bn`, `latency-all-three` | 100±20 / 150±30 / 200±40 ms                      | Does the network tolerate *normal* cross-AZ/region latency with no visible impact? | Steady block production holds: `block-rate-floor ≥ 0.3 blk/s`, healthy, increasing.                                                             |
| **stress**   | `latency-stress`                                                                | 300±80 / 450±120 / 600±150 ms, correlation 50%   | Does it **degrade gracefully and recover**?                                        | Backfill is exercised (`backfill-triggered`), then block production resumes; reduced floor `≥ 0.2 blk/s`.                                       |
| **severe**   | `latency-severe`                                                                | 600±150 / 800±200 / 1000±250 ms, correlation 75% | Survival / recovery near the breaking point.                                       | Survives + **recovers** after latency clears (`backfill-triggered`, `blocks-increasing`); low floor `≥ 0.1 blk/s` (recovery, not steady-state). |

**Pick a profile by selecting its test name.** All profiles require the `paired-3` topology and run with **TSS on** (the default; see "Pre-requisites" below).

Run locally (Taskfile):

```bash
# baseline (one dimension, or all three at once)
CHAOS_ENABLED=true TOPOLOGY=paired-3 task test:run TEST_FILE=tests/latency-cn-to-cn.yaml
CHAOS_ENABLED=true TOPOLOGY=paired-3 task test:run TEST_FILE=tests/latency-all-three.yaml
# heavier tiers
CHAOS_ENABLED=true TOPOLOGY=paired-3 task test:run TEST_FILE=tests/latency-stress.yaml
CHAOS_ENABLED=true TOPOLOGY=paired-3 task test:run TEST_FILE=tests/latency-severe.yaml
```

Run in CI (`solo-e2e-test` workflow) — pass the test name (without `.yaml`) as `test-definition`:

```bash
gh workflow run solo-e2e-test.yml --ref <branch> -f topology=paired-3 -f tss-enabled=true -f test-definition=latency-stress
```

> **Concurrency gotcha:** the workflow uses `concurrency: solo-network-<topology>` with `cancel-in-progress: true`. Two runs of the **same topology** (e.g. two `paired-3` chaos tests) cannot run at once — the newer dispatch **cancels** the older. Run chaos tests one at a time, or stagger them. A canceled run shows `kind/solo: command not found` in its cleanup steps — that's cancellation noise, not a real failure.

### Tuning latency yourself

Each `inject-latency` event accepts:

|              Key              |                                              Meaning                                               |        Example         |
|-------------------------------|----------------------------------------------------------------------------------------------------|------------------------|
| `source.kind` / `target.kind` | `network-node` (CN) or `block-node` (BN)                                                           | `{ kind: block-node }` |
| `latency`                     | base one-way delay (any netem duration)                                                            | `200ms`                |
| `jitter`                      | ± variation around `latency`                                                                       | `40ms`                 |
| `correlation`                 | % correlation between successive delays (burstiness); default `0`                                  | `"50"`                 |
| `bidirectional`               | shape both directions (`direction: both`)                                                          | `true`                 |
| `loss`                        | netem packet loss — wired end-to-end (runner + template), but no shipped scenario exercises it yet | `"1%"`                 |

To make a custom heavier/lighter variant, copy a profile and adjust `latency`/`jitter`/`correlation`. Bump `block-rate-floor.min_rate_per_sec` down as you increase latency (the assertions run after the latency clears, so they verify recovery).

## How the tests are shaped

Each latency test follows the same shape and runs in ~7–8 minutes:

```yaml
events:
  - load-start                    (t=0)
  - inject-latency                (t=30,  starts the chaos window)
  - clear-latency                 (t=330, ends it after ~5 min)
  - load-stop                     (t=360)
assertions:
  - all-healthy                   (every BN pod is Running)
  - all-have-blocks               (every BN has block 0..N)
  - blocks-increasing             (every BN observes Δblocks > 0)
  - block-rate-floor              (Δblocks/Δtime ≥ floor)
  - backfill-triggered            (stress/severe — BNs caught up via backfill)
```

The 0.3 blocks/s baseline floor is intentionally conservative. On the `paired-3` topology, healthy block production averaged ~0.47 blocks/s under modest NLG load, so 0.3 is a comfortable margin — high enough to catch a real stall (the assertion reports 0.000/s on a stalled cluster) and low enough to absorb chaos-induced micro-slowdowns. The stress/severe tiers lower this floor because they intentionally push the system into temporary degradation and check that it *recovers*.

## Baseline scenarios (per dimension)

### `tests/latency-cn-to-cn.yaml`

**Simulates:** CNs gossiping across a slow inter-region link.

**Parameters:** 100 ms ± 20 ms, bidirectional, 5-min window, `solo.hedera.com/type=network-node` on both sides.

**Expected behavior:** Consensus rounds take longer, but the network still produces blocks. BNs continue receiving blocks at roughly the same cadence — typically 0.4–0.5 blocks/s after the chaos clears.

**Assertions:** `node-healthy`, `block-available`, `blocks-increasing`, `block-rate-floor` ≥ 0.3 blocks/s, `metric-threshold blocknode_publisher_stream_errors_total == 0` (exemplifies the generic metric primitive), and `signature-transition` (Schnorr→WRAPS, proving TSS is active under latency).

### `tests/latency-bn-to-bn.yaml`

**Simulates:** Block Nodes in different regions over a slow peer mesh.

**Parameters:** 200 ms ± 40 ms, bidirectional, 5-min window, `block-node.hiero.com/type=block-node` both sides.

**Expected behavior:** Live CN-to-BN streaming is **unaffected** (CN egress doesn't pass through any BN-to-BN rule). BN peer backfill is slowed; on paired-3 each BN already receives blocks directly from a paired CN, so backfill is not the critical path — blocks continue to land.

**Assertions:** same as CN↔CN minus the stream-error check (the CN-publisher stream is independent of BN-to-BN traffic).

> solo-chaos ships no built-in BN↔BN template; this test uses the harness's own parametric template at `scripts/chaos-templates/network-latency.yaml.tmpl` (a candidate upstream contribution).

### `tests/latency-cn-to-bn.yaml`

**Simulates:** The production-realistic case — CNs and BNs in different regions, with the gRPC block-publish stream crossing a slow link.

**Parameters:** 150 ms ± 30 ms, bidirectional, 5-min window, `network-node` → `block-node`.

**Expected behavior:** Each block round-trip through the publisher gRPC stream pays the latency both ways; the BN's ACK takes ~300 ms longer, which can affect publisher flow control. Blocks still flow and `blocknode_publisher_open_connections` should stay non-zero.

**Assertions:** same as CN↔CN minus the strict stream-error check (the publisher stream may transiently reconnect under high latency, which would flake a strict check).

### `tests/latency-all-three.yaml`

**Simulates:** "Everything is in different regions" — three concurrent NetworkChaos rules layered on the same cluster.

**Parameters:** CN↔CN 100±20, BN↔BN 200±40, CN↔BN 150±30 ms, all bidirectional, 5-min window.

**Expected behavior:** Each pod accumulates the netem rules matching its labels; all egress paths are throttled simultaneously. This is the strongest check that Chaos Mesh applies all three rules without interference.

**Assertions:** same as CN↔CN minus the stream-error check.

## Heavy scenarios

### `tests/latency-stress.yaml`

**Simulates:** A degraded WAN — ~3× baseline with bursty (correlated) delays. CN↔CN 300±80, CN↔BN 450±120, BN↔BN 600±150 ms, correlation 50%.

**Expected behavior:** BNs fall behind during the chaos window and **catch up via backfill**, then resume normal production once latency clears.

**Assertions:** `node-healthy`, `block-available`, `backfill-triggered`, `blocks-increasing` (recovery), `block-rate-floor` ≥ 0.2 blk/s.

### `tests/latency-severe.yaml`

**Simulates:** Breaking-point latency — ~5–6× baseline, high correlation. CN↔CN 600±150, CN↔BN 800±200, BN↔BN 1000±250 ms, correlation 75%.

**Expected behavior:** A resilience/recovery probe (not a throughput test). BNs are expected to fall well behind; the test verifies survival + clean recovery after latency clears.

**Assertions:** `node-healthy`, `block-available`, `backfill-triggered`, `blocks-increasing` (recovery), `block-rate-floor` ≥ 0.1 blk/s (recovery, not steady-state).

## Pre-requisites

- **Topology:** `paired-3` (3 CN + 3 BN) — the latency dimensions need multiple nodes per type.
- **TSS:** on by default (the supported mode) — confirmed working under latency (the `tss-signature-transition` check sees a real Schnorr→WRAPS transition during a chaos run). `--wraps` requires CN ≥ v0.74.0-0; `CN_VERSION=latest` is min-enforced by `resolve-versions.sh` to a TSS-capable tag (currently `0.75.0-rc.4`), so it deploys cleanly. Only set `TSS_ENABLED=false` if you pin a CN below the floor.
- **Chaos Mesh:** installed in the cluster (`CHAOS_ENABLED=true task chaos:install` locally; the CI workflow installs it automatically when a `chaos`/`latency` test is selected).

## Known limitations

- **Post-chaos measurement only.** Assertions run *after* all events complete, so `block-rate-floor` measures recovery, not under-chaos rate. Measuring under-chaos rate would need an event-time assertion (future enhancement).
- **Tests assume a healthy cluster.** If consensus stalls (an upstream issue, independent of the chaos rules), `block-rate-floor` reports `0.000/s` — that's the assertion correctly detecting the stall, not a flake. Sanity-check the cluster with `task verify` before attributing a failure to a latency test.
- **NLG lock contention between runs.** Solo CLI's NLG lock can persist across consecutive runs in the same cluster session; `load-stop` recovers via a direct Helm uninstall, but the following `load-start` may fail. Tests still pass because consensus produces blocks via heartbeats.
- **Histogram metrics absent on the BN.** The BN doesn't export block-arrival histograms today, so a true p99 of block-arrival interval isn't computable; `block-rate-floor` is the rate-based proxy that ships.

## Extending — adding a new latency scenario

A new scenario is typically <30 lines of YAML. Copy an existing test as a starting point:

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
      correlation: "0"                   # % burst correlation (optional)
      bidirectional: true

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

Validate (no cluster required), then run:

```bash
task test:validate TEST_FILE=tests/latency-my-scenario.yaml
CHAOS_ENABLED=true TOPOLOGY=paired-3 task test:run TEST_FILE=tests/latency-my-scenario.yaml
```

## Pointers

- Upstream Chaos Mesh wrapper this builds on: [solo-chaos](https://github.com/hashgraph/solo-chaos)
- Chaos Mesh NetworkChaos reference: https://chaos-mesh.org/docs/define-chaos-experiment-scope/
