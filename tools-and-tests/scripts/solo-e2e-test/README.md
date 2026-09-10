# Solo E2E Test - Local Development Helper

Deploy Hiero networks locally for development and testing using [Solo CLI](https://github.com/hashgraph/solo) and [Kind](https://kind.sigs.k8s.io/).

## Table of Contents

- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Commands](#commands)
- [Manual Testing](#manual-testing)
- [Configuration](#configuration)
- [Topologies](#topologies)
- [Load Generation](#load-generation)
- [TCK-SDK Tests](#tck-sdk-tests)
- [Test Framework](#test-framework)
- [Network Chaos / Latency Tests](#network-chaos--latency-tests)
- [CN-BN Priority Routing](#cn-bn-priority-routing)
- [CI Integration](#ci-integration)
- [Endpoints](#endpoints)
- [Troubleshooting](#troubleshooting)
- [Scheduled Runs](#scheduled-runs)

## Why This Exists

The CI workflow (`.github/workflows/solo-e2e-test.yml`) deploys Hiero networks for end-to-end testing. This directory is **self-contained** with all scripts and topologies needed for both local development and CI.

```text
+---------------------------------------------------------------------+
|                     solo-e2e-test/                                  |
|  (Self-contained test environment)                                  |
|                                                                     |
|  +-- scripts/                                                       |
|  |   +-- resolve-versions.sh    (resolve 'latest' -> actual tags)   |
|  |   +-- solo-setup-cluster.sh  (create cluster, init Solo)         |
|  |   +-- solo-deploy-network.sh (deploy BN, CN, MN, Relay, Explorer)|
|  |   +-- solo-load-generate.sh  (NLG load generation)               |
|  |   +-- solo-port-forward.sh   (kubectl port forwards)             |
|  |   +-- solo-network-status.sh (network health summary)            |
|  |   +-- solo-metrics-summary.sh(Block Node metrics)                |
|  |   +-- solo-test-runner.sh    (YAML test framework runner)        |
|  |                                                                  |
|  +-- topologies/                                                    |
|  |   +-- single.yaml          (1 CN, 1 BN)                          |
|  |   +-- paired-3.yaml        (3 CN, 3 BN)                          |
|  |   +-- fan-out-3cn-2bn.yaml (3 CN, 2 BN)                          |
|  |   +-- 3cn-1bn.yaml         (3 CN, 1 BN)                          |
|  |   +-- minimal.yaml         (1 CN, 1 BN, no mirror/relay)         |
|  |   +-- 7cn-3bn-distributed.yaml (7 CN, 3 BN)                      |
|  |                                                                  |
|  +-- tests/                   (test definitions)                    |
|  |   +-- smoke-test.yaml      (quick validation)                    |
|  |   +-- basic-load.yaml      (load test with metrics)              |
|  |   +-- node-restart-resilience.yaml (restart recovery)            |
|  |                                                                  |
|  +-- Taskfile.yml  (local dev interface)                            |
+---------------------------------------------------------------------+
           ^                                    ^
           |                                    |
    +------+------+                    +--------+--------+
    |  Taskfile   |                    |  CI Workflow    |
    |  (local)    |                    |  (GitHub)       |
    |  task up    |                    |  workflow_      |
    |             |                    |  dispatch       |
    +-------------+                    +-----------------+
```

**Benefits:**
- Test locally before pushing to CI
- Same scripts = same behavior
- Debug deployment issues locally
- Faster iteration than waiting for CI

## Quick Start

```bash
# 1. Check prerequisites
task check

# 2. Deploy network
task up

# 3. Verify it's working (see "Manual Testing" below)

# 4. Tear down when done
task down
```

## Prerequisites

|        Tool        |                                      Installation                                       |
|--------------------|-----------------------------------------------------------------------------------------|
| Docker             | [docker.com](https://docs.docker.com/get-docker/)                                       |
| kubectl            | [kubernetes.io](https://kubernetes.io/docs/tasks/tools/)                                |
| Helm               | [helm.sh](https://helm.sh/docs/intro/install/)                                          |
| Kind               | [kind.sigs.k8s.io](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)        |
| Solo CLI           | `npm i @hashgraph/solo -g`                                                              |
| Task               | [taskfile.dev](https://taskfile.dev/installation/)                                      |
| yq                 | [github.com/mikefarah/yq](https://github.com/mikefarah/yq#install)                      |
| grpcurl (optional) | [github.com/fullstorydev/grpcurl](https://github.com/fullstorydev/grpcurl#installation) |

```bash
task check  # Verify all installed
```

## Commands

### Core

|   Command    |                  Description                   |
|--------------|------------------------------------------------|
| `task up`    | Full setup (cluster + network + port-forwards) |
| `task down`  | Tear down everything                           |
| `task check` | Check prerequisites are installed              |

### Topologies

|              Command               |                  Description                  |
|------------------------------------|-----------------------------------------------|
| `task up`                          | Deploy single topology (1 CN, 1 BN) - default |
| `task up TOPOLOGY=paired-3`        | Deploy paired-3 (3 CN, 3 BN)                  |
| `task up TOPOLOGY=fan-out-3cn-2bn` | Deploy fan-out (3 CN, 2 BN)                   |
| `task up TOPOLOGY=<name>`          | Deploy any topology by name                   |

### Cluster Management

|                       Command                       |                  Description                   |
|-----------------------------------------------------|------------------------------------------------|
| `task cluster:create`                               | Create Kind cluster with Solo initialization   |
| `task cluster:destroy`                              | Destroy Kind cluster and clean up Solo config  |
| `task cluster:init CONTEXT=<ctx> CLUSTER_REF=<ref>` | Initialize Solo for external cluster (no Kind) |

### Verification

|     Command     |                     Description                      |
|-----------------|------------------------------------------------------|
| `task verify`   | Check Block Node via gRPC (NODE=n for specific node) |
| `task status`   | Show network status for all nodes                    |
| `task metrics`  | Show Block Node metrics (NODE=n or NODE=all)         |
| `task logs:bn`  | Stream Block Node logs (NODE=n for specific node)    |
| `task bn:reset` | Reset BN verification state and restart (NODE=n)     |

### Load Generation

|                    Command                    |                Description                 |
|-----------------------------------------------|--------------------------------------------|
| `task load:up`                                | Run NLG with defaults (-c 5 -a 10 -tt 300) |
| `task load:up NLG_ARGS="-c 10 -a 20 -tt 600"` | Run with custom settings                   |
| `task load:down`                              | Stop/cleanup load generation               |

### Utilities

|         Command          |         Description          |
|--------------------------|------------------------------|
| `task port-forward`      | Set up/restart port forwards |
| `task port-forward:stop` | Stop all port forwards       |

## Manual Testing

After `task up` completes, verify the network is working:

### 1. Check All Pods Are Running

```bash
kubectl get pods -n solo-network

# Expected: All pods showing 1/1 or 2/2 READY, STATUS=Running
# NAME                          READY   STATUS
# block-node-1-0                1/1     Running
# network-node1-0               5/5     Running
# mirror-1-importer-xxx         1/1     Running
# ...
```

### 2. Check Block Node Is Receiving Blocks

```bash
# View Block Node logs - should show blocks being processed
kubectl logs -n solo-network -l app.kubernetes.io/name=block-node-1 --tail=20

# Look for: "Forwarding batch for block=XXX"
```

### 3. Check Mirror Node Is Importing

```bash
# View Mirror importer logs
kubectl logs -n solo-network -l app.kubernetes.io/component=importer --tail=10

# Look for: "Successfully processed X items from..."
```

### 4. Query Block Node Status (requires grpcurl)

```bash
task port-forward  # Ensure port forwards are active

grpcurl -plaintext localhost:40840 \
  org.hiero.block.api.BlockNodeService/serverStatus
```

### 5. Query Mirror Node REST API

```bash
curl -s http://localhost:5551/api/v1/blocks?limit=3 | jq .
```

## Configuration

### Environment Variables

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

|         Variable         |         Default          |                             Description                              |
|--------------------------|--------------------------|----------------------------------------------------------------------|
| `TOPOLOGY`               | `single`                 | Network topology to deploy                                           |
| `CLUSTER_NAME`           | `solo-cluster`           | Kind cluster name                                                    |
| `NAMESPACE`              | `solo-network`           | Kubernetes namespace                                                 |
| `DEPLOYMENT`             | `deployment-solo`        | Solo deployment name                                                 |
| `SOLO_VERSION`           | `latest`                 | Solo CLI version (CI pins to `0.79.0`)                               |
| `SOLO_SOURCE`            | `npm`                    | Solo install source: `npm` or `git` (see Custom Solo Build)          |
| `SOLO_GIT_REPO`          | (empty)                  | Fork `owner/repo` when `SOLO_SOURCE=git` (must be approved)          |
| `SOLO_GIT_REF`           | (empty)                  | Branch/tag/SHA when `SOLO_SOURCE=git`                                |
| `CN_VERSION`             | `main`                   | Consensus Node version (requires `CN_LOCAL_BUILD_PATH`)              |
| `CN_LOCAL_BUILD_PATH`    | (empty)                  | Built CN `hedera-node/data` dir; required when `CN_VERSION=main`     |
| `MN_VERSION`             | `latest`                 | Mirror Node version                                                  |
| `BN_VERSION`             | `latest`                 | Block Node version                                                   |
| `RELAY_VERSION`          | `latest`                 | Relay version                                                        |
| `TCK_VERSION`            | `latest`                 | TCK-SDK version                                                      |
| `TSS_ENABLED`            | `true`                   | Enable TSS (hinTS) on consensus nodes (requires CN ≥ v0.72.0)        |
| `NLG_TEST_TYPE`          | `CryptoTransferLoadTest` | NLG test class                                                       |
| `NLG_ARGS`               | `-c 5 -a 10 -tt 300`     | NLG arguments (-c concurrency, -a accounts, -tt duration)            |
| `NLG_MAX_TPS`            | (empty)                  | Optional max transactions per second                                 |
| `MIRROR_NODE_PINGER_TPS` | `5`                      | Mirror Node pinger TPS (0 to disable, CI only)                       |
| `ENABLE_LOCAL_METRICS`   | `false`                  | Enable Prometheus+Grafana stack locally                              |
| `TEST_FILE`              | `none`                   | Test definition file for `task test:run`                             |
| `TCK_SDK_DIR`            | `sdk-tck`                | Directory for TCK-SDK repositories                                   |
| `TCK_TEST_FILE`          | (transfer + contract)    | TCK test file(s) to run (space-separated, verifies CN + Mirror Node) |

### Version Keywords

| Keyword  |                   Resolves To                   |               Notes                |
|----------|-------------------------------------------------|------------------------------------|
| `latest` | Latest GA release from GitHub                   | All components                     |
| `main`   | Current development snapshot                    | Block Node; CN needs a local build |
| `rc`     | Latest Release Candidate (tag containing `-rc`) | All components                     |
| `v0.x.y` | Specific version tag                            | All components                     |

> **Note:** `main` resolves straight through for Block Node, which publishes SNAPSHOT images. Mirror Node, Relay and TCK do **not** publish `main` snapshots at all. Consensus Node needs the extra step below.

### Consensus Node from `main` (local build)

**`CN_VERSION` defaults to `main` locally**, because no published CN tag yet carries the fixed
16-slot block-root hashing rework — a released tag fails verification against a current Block Node
and Mirror Node. That means a local `task up` requires `CN_LOCAL_BUILD_PATH`; the deploy fails fast
if it is unset. Pin a released tag (`v0.79.0-alpha.1`, `v0.78.0-rc.2`) if you want to skip the build
and don't need the new hashing.

CI does the same thing automatically. When the resolved CN version ends in `-SNAPSHOT`,
`solo-e2e-test.yml` checks out `hiero-ledger/hiero-consensus-node` at `main`, runs
`./gradlew assemble`, and passes the result as `--cn-local-build-path`. Released tags skip
the build entirely and fetch the published zip as before, so only SNAPSHOT runs pay the
build cost.

Solo does not pull the Consensus Node as a container image. `solo consensus node setup` downloads a
platform build zip from `builds.hedera.com/node/software/<vMAJOR.MINOR>/build-<tag>.zip` and unpacks it
into the pod's `root-container`. **Only tagged releases are published there**, so `CN_VERSION=main`
(which resolves to e.g. `0.79.0-SNAPSHOT` from the CN branch's `version.txt`) has no artifact to fetch.

To run an unreleased CN commit, build it locally and point the deploy at the build output. Solo's
`--local-build-path` skips the download and uploads your jars instead:

```bash
cd <cn-repo>
git fetch upstream && git checkout upstream/main
./gradlew assemble          # populates hedera-node/data/{apps,lib}

cd -                        # back to solo-e2e-test
task up CN_VERSION=main CN_LOCAL_BUILD_PATH=<cn-repo>/hedera-node/data
```

`CN_LOCAL_BUILD_PATH` points at the `data` directory itself — Solo validates that it contains `apps/`
and `lib/`. The release tag is still needed (staging-dir naming, Solo's TSS capability gate) but no
longer has to match a published build; Solo explicitly tolerates the mismatch for local builds.

`solo-deploy-network.sh` fails fast on both mistakes — a `-SNAPSHOT` `CN_VERSION` with no
`CN_LOCAL_BUILD_PATH`, and a `CN_LOCAL_BUILD_PATH` that hasn't been built — rather than letting the
404 surface after the cluster and Block Nodes are already up.

> **Named CI tags don't work.** Tags like `sdpt-pass-00380` on the CN repo are git-only markers: no
> build zip is published for them, and Solo's `Templates.prepareReleasePrefix` rejects any tag that
> isn't dotted semver. Use a real pre-release tag (`v0.79.0-alpha.1`, `v0.78.0-rc.2`) or a local build.

### Command-Line Overrides

Variables can be overridden on the command line for one-off runs. Command-line values take precedence over `.env` file settings:

```bash
task up TOPOLOGY=paired-3 BN_VERSION=v0.24.0
task load:up NLG_ARGS="-c 10 -a 20 -tt 600"
```

## Custom Solo Build (fork/branch)

By default Solo is installed from npm (`npm i @hashgraph/solo -g`). When a fix isn't released yet, you can build Solo from an **approved fork/branch** instead and install it globally.

Set `SOLO_SOURCE=git` and point at the fork:

|    Variable     |                      Purpose                      |
|-----------------|---------------------------------------------------|
| `SOLO_SOURCE`   | `git` to build from source (`npm` is the default) |
| `SOLO_GIT_REPO` | `owner/repo` — must be in the approved allowlist  |
| `SOLO_GIT_REF`  | Branch, tag, or commit SHA to build               |

**Approved repositories** (allowlist enforced by `scripts/solo-install.sh`): `hiero-ledger/solo`, `hashgraph/solo`, `AlfredoG87/solo`. Any other repo is rejected with a hard failure.

The build mirrors Solo's own `build:compile` (`npm ci` → `npx tsc` → `node resources/post-build-script.js`) then `npm i -g .` from the clone. A bare `npm i github:owner/repo#ref -g` does **not** work — Solo's `prepare` script is not a build, so `dist/` would be missing.

**Local:**

```bash
# Install a custom build, then deploy as usual
task solo:install SOLO_SOURCE=git SOLO_GIT_REPO=AlfredoG87/solo SOLO_GIT_REF=fix/bn-health-port-5283
task up TOPOLOGY=single
```

The clone lands in `.solo-build/` (gitignored). The minimum-version check in the setup/deploy scripts is skipped for git builds, since a fork may be based on older code.

**CI:** dispatch the `Solo E2E Test` workflow with `solo-source=git`, `solo-git-repo=<approved fork>`, `solo-git-ref=<ref>`.

> ⚠️ CI executes the cloned code (`npm ci` runs lifecycle scripts). The allowlist is the security boundary — keep it short and trusted.

## Load Generation

Both the CI workflow and local Taskfile support configurable transaction load generation using Solo's Network Load Generator (NLG).

### Local Load Generation

```bash
# Deploy first, then start load separately
task up
task load:up NLG_ARGS="-c 10 -a 20 -tt 600"

# Stop load generation
task load:down
```

### CI Workflow Inputs

|       Input       |         Default          |                                       Description                                        |
|-------------------|--------------------------|------------------------------------------------------------------------------------------|
| `nlg-enabled`     | `false`                  | Enable NLG load generation                                                               |
| `nlg-test-type`   | `CryptoTransferLoadTest` | Test class (dropdown in UI)                                                              |
| `nlg-args`        | `-c 5 -a 10 -tt 300`     | NLG args: `-c <clients>` `-a <accounts>` `-tt <duration>`                                |
| `nlg-max-tps`     | (empty)                  | Rate limit TPS (optional, uses RateLimitedQueue)                                         |
| `test-definition` | `none`                   | Test definitions: smoke-test, basic-load, node-restart-resilience, full-history-backfill |

### NLG Parameters Reference

| Parameter  |                         Description                          |        Used By        |
|------------|--------------------------------------------------------------|-----------------------|
| `-c <num>` | Concurrent clients/threads                                   | All tests             |
| `-a <num>` | Number of test accounts to create                            | All tests             |
| `-t/-tt`   | Duration: seconds (300), minutes (5m), or hours (1h)         | All tests             |
| `-n <num>` | Topics (HCSLoadTest) or NFTs per token (NftTransferLoadTest) | HCS, NFT tests        |
| `-T <num>` | Number of tokens to create                                   | NFT, Token tests      |
| `-A <num>` | Associations per account                                     | TokenTransferLoadTest |

> **Note:** NLG doesn't have direct TPS control. Use `nlg-max-tps` to rate limit, otherwise concurrency and accounts determine actual throughput.

### Available Test Classes

|        Test Class        |                 Description                  |
|--------------------------|----------------------------------------------|
| `CryptoTransferLoadTest` | HBAR transfers between accounts (default)    |
| `HCSLoadTest`            | Hedera Consensus Service message submissions |
| `TokenTransferLoadTest`  | HTS fungible token transfers                 |
| `NftTransferLoadTest`    | NFT minting and transfers                    |

### Example: Running High Load Test

Via GitHub Actions workflow dispatch:

1. Go to Actions → "Solo E2E Test" → "Run workflow"
2. Set parameters:
   - `nlg-enabled`: `true`
   - `nlg-test-type`: `CryptoTransferLoadTest`
   - `nlg-args`: `-c 32 -a 100 -tt 600`
   - `nlg-max-tps`: `5000` (optional rate limit)

### How It Works

The load generator:

1. Deploys the NLG pod into the cluster via `solo rapid-fire load start`
2. Creates test accounts based on the specified account count
3. Generates transactions using the specified concurrency and accounts
4. Runs for the specified duration in seconds
5. Cleans up via `solo rapid-fire load stop`

**NLG Parameters** (passed via `--args`):
- `-c` = concurrency (parallel clients)
- `-a` = accounts (test accounts to create)
- `-tt` = time in seconds (or `-t` with units like `5m`, `1h`)

## TCK-SDK Tests

Run TCK-SDK regression tests against the deployed network. These tests validate SDK compatibility with the network.

### Commands

|       Command       |                   Description                   |
|---------------------|-------------------------------------------------|
| `task tck:clone`    | Clone TCK and JS-SDK repos at latest tags       |
| `task tck:check`    | Check if TCK-SDK dependencies are installed     |
| `task tck:install`  | Install TCK-SDK dependencies (runs after clone) |
| `task test:tck-sdk` | Run TCK-SDK regression tests                    |

### Configuration

|    Variable     |              Default              |              Description              |
|-----------------|-----------------------------------|---------------------------------------|
| `TCK_SDK_DIR`   | `sdk-tck`                         | Directory for TCK/SDK repositories    |
| `TCK_TEST_FILE` | (transfer-HBAR + contract-delete) | Test file(s) to run (space-separated) |

### Usage

```bash
# First time setup
task tck:clone
task tck:install

# Run tests (auto-clones/installs if needed)
task test:tck-sdk

# Run specific test file
task test:tck-sdk TCK_TEST_FILE="src/tests/token-service/test-token-create.ts"
```

## CN-BN Priority Routing

The topology file's `consensus_nodes` section controls which Block Nodes each Consensus Node streams to:

```yaml
consensus_nodes:
  node1:
    block_nodes: [block-node-1]               # Only streams to BN-1
  node2:
    block_nodes: [block-node-2, block-node-1] # BN-2 primary, BN-1 fallback
  node3:
    block_nodes: [block-node-1, block-node-2] # BN-1 primary, BN-2 fallback
```

**Priority**: The position in the `block_nodes` array determines priority (1-indexed).
First = highest priority (primary), subsequent entries are fallbacks.
CNs not listing a BN will not stream to that BN.

### How It Works

The deploy script generates BN-centric priority mappings from the topology using `--priority-mapping`
on `Block Node add`. This specifies which CNs should route to each BN with their priorities.

Example for BN-1: `node1=1,node2=2,node3=1` means node1 and node3 have priority 1 (primary),
and node2 has priority 2 (fallback).

## Topologies

Topologies define network configuration. Located in `./topologies/`.

|         Name          | CN | BN | MN | Relay | Explorer |                      Use Case                       |
|-----------------------|----|----|:--:|:-----:|:--------:|-----------------------------------------------------|
| `single`              | 1  | 1  | 1  |   0   |    0     | Basic testing, fastest startup                      |
| `paired-3`            | 3  | 3  | 1  |   0   |    0     | Multi-node testing, each CN->BN pair                |
| `fan-out-3cn-2bn`     | 3  | 2  | 1  |   0   |    0     | Redundancy testing, all CNs->all BNs                |
| `3cn-1bn`             | 3  | 1  | 1  |   0   |    0     | Single BN receiving from multiple CNs               |
| `minimal`             | 1  | 1  | 0  |   0   |    0     | CN+BN only, no mirror/relay/explorer                |
| `2cn-2bn-backfill`    | 2  | 2  | 1  |   0   |    0     | Backfill testing, BN recovery after data loss       |
| `7cn-3bn-distributed` | 7  | 3  | 1  |   0   |    0     | Distributed streaming, grouped CN->BN with backfill |
| `single-wrb-rsa`      | 1  | 1  | 1  |   0   |    0     | WRB (wrapped record blocks) verified via RSA roster |
| `3cn-2bn-wrb-rsa`     | 3  | 2  | 1  |   0   |    0     | WRB fan-out verified via RSA roster                 |

See `../network-topology-tool/README.md` for topology schema details.

### Enabling Optional Components

Relay and Explorer are **off in every bundled topology** — none of them define a
`relay_nodes` or `explorer_nodes` entry. A component is deployed only when its section
lists at least one node:

```yaml
relay_nodes:
  relay-1: {}       # deploys the JSON-RPC Relay

explorer_nodes: {}  # empty (or absent) -> not deployed
```

> The Relay is off by default because its startup probe (`GET :7546/health/readiness`)
> has been failing on recent Relay images, leaving the pod at `0/1 Running`. Since
> `solo relay node add` waits for readiness, a deployed-but-unhealthy Relay aborts
> `task up` **before** it reaches `task port-forward`, which looks like "port forwards
>
>> are broken". Enable the Relay only if you actually need JSON-RPC.

### WRB + RSA Verification Topologies

The `*-wrb-rsa` topologies are "special": instead of TSS-signed blocks they exercise the Phase-2a
path where Consensus Nodes stream **Wrapped Record Blocks (WRB)** carrying gossiped RSA signatures,
and Block Nodes verify them against an **RSA roster**. They are selected by a top-level
`verification_mode: rsa-wrb` marker in the topology file (default is `tss`). When the marker is
present the deploy script:

- enables `blockStream.streamWrappedRecordBlocks=true` on the Consensus Nodes and **disables TSS**;
- enables the `roster-bootstrap-rsa` Block Node plugin, pointing it at the in-cluster Mirror Node
  REST service (`ROSTER_BOOTSTRAP_RSA_MIRROR_NODE_BASE_URL`);
- configures the Mirror Node importer for the WRB cutover (`block.cutover.enabled`,
  `firstStage.enabled`, `DISABLE_IMPORTER_SPRING_PROFILES=true`).

Deployment order stays the standard **BN -> CN -> MN** (so Solo wires the Block Nodes as
stream sources on the Consensus Nodes). The `roster-bootstrap-rsa` plugin polls the Mirror Node
indefinitely until it is reachable, so the Block Node tolerates the Mirror Node starting later.

These topologies do not run the `tss-signature-transition` test; they run `rsa-roster-verification`
instead.

## CI Integration

The CI workflow (`.github/workflows/solo-e2e-test.yml`) uses the same scripts as local development.

### Workflow Inputs

|           Input            |         Default          |                     Description                     |
|----------------------------|--------------------------|-----------------------------------------------------|
| `topology`                 | `single`                 | Network topology to deploy                          |
| `block-node-version`       | `latest`                 | BN version (`latest`, `main`, `rc` or specific tag) |
| `consensus-node-version`   | `latest`                 | CN version (`latest`, `rc` or specific tag)         |
| `mirror-node-version`      | `latest`                 | MN version (`latest`, `rc` or specific tag)         |
| `relay-version`            | `latest`                 | Relay version (`latest` or specific tag)            |
| `tck-version`              | `latest`                 | TCK-SDK version (`latest` or specific tag)          |
| `solo-version`             | `0.63.0`                 | Solo CLI version                                    |
| `tss-enabled`              | `true`                   | Enable TSS on consensus nodes                       |
| `nlg-enabled`              | `false`                  | Enable NLG load generation                          |
| `nlg-test-type`            | `CryptoTransferLoadTest` | NLG test class                                      |
| `nlg-args`                 | `-c 5 -a 10 -tt 300`     | NLG arguments (combined `-c`, `-a`, `-tt`)          |
| `nlg-max-tps`              | (empty)                  | Optional TPS rate limit                             |
| `mirror-node-pinger-tps`   | `5`                      | Mirror Node pinger TPS (0 to disable)               |
| `test-definition`          | `none`                   | Test definitions (comma-separated)                  |
| `run-tck-regression-tests` | `false`                  | Run TCK-SDK regression tests                        |

### Script Flow

```
task up
  |
  +-> task cluster:create
  |     +-> scripts/solo-setup-cluster.sh
  |           +-- Create Kind cluster
  |           +-- solo init
  |           +-- solo cluster-ref config connect
  |           +-- solo deployment config create
  |           +-- solo deployment cluster attach
  |
  +-> task network:deploy
  |     +-> scripts/resolve-versions.sh (latest -> v0.x.y)
  |     +-> scripts/solo-deploy-network.sh
  |           +-- Load topology from topologies/
  |           +-- solo block node add (xBN_COUNT)
  |           +-- solo keys consensus generate
  |           +-- solo consensus network deploy
  |           +-- solo consensus node setup
  |           +-- solo consensus node start
  |           +-- solo mirror node add
  |           +-- solo relay node add
  |           +-- solo explorer node add
  |
  +-> task port-forward
        +-> scripts/solo-port-forward.sh
              +-- kubectl port-forward (multiple services)
```

### Local vs CI Equivalence

|         Taskfile          |                 CI Workflow                 |
|---------------------------|---------------------------------------------|
| `task up TOPOLOGY=single` | `workflow_dispatch` with `topology: single` |
| `TOPOLOGY` variable       | `inputs.topology`                           |
| `CN_VERSION=v0.68.6`      | `inputs.consensus-node-version`             |
| `.env` file               | Workflow `env:` block                       |

The CI workflow calls the same scripts:

```yaml
# CI workflow excerpt
- name: Setup cluster
  run: ./tools-and-tests/scripts/solo-e2e-test/scripts/solo-setup-cluster.sh ...

- name: Deploy network
  run: ./tools-and-tests/scripts/solo-e2e-test/scripts/solo-deploy-network.sh ...
```

## Endpoints

After deployment with port-forwards active:

|       Service       | Base Port |           Multi-Node           |
|---------------------|-----------|--------------------------------|
| Consensus Node gRPC | 50211     | -                              |
| Block Node gRPC     | 40840     | +1 per node (40841, 40842..)   |
| Block Node Metrics  | 16007     | +1 per node (16008, 16009..)   |
| Mirror REST API     | 5551      | +1 per node (5552, 5553..)     |
| Mirror Monitor      | 5600      | -                              |
| Mirror REST Java    | 8084      | -                              |
| Relay JSON-RPC      | 7546      | +1 per node (7547, 7548..)     |
| Explorer            | 8080      | -                              |
| Grafana             | 3000      | If `ENABLE_LOCAL_METRICS=true` |

**Multi-node example:**

```bash
task verify NODE=2        # Check Block Node 2 on port 40841
task logs:bn NODE=2       # View Block Node 2 logs
curl localhost:5552/api/v1/blocks  # Mirror Node 2
```

## Test Framework

The test framework provides YAML-driven test definitions for structured E2E testing with sequential event execution. Test definitions specify:
- Timed events (commands, node operations, load generation)
- Assertions to validate test outcomes

Events execute sequentially in delay order, with sleeps between them.

### Quick Start

```bash
# List available tests
task test:list

# Run a test
task test:run TEST_FILE=tests/smoke-test.yaml

# Validate a test definition (syntax check only)
task test:validate TEST_FILE=tests/basic-load.yaml
```

### Available Tests

|              Test File               |                     Description                     |
|--------------------------------------|-----------------------------------------------------|
| `tests/smoke-test.yaml`              | Quick validation of network functionality           |
| `tests/basic-load.yaml`              | Basic load test (1000 TPS cap)                      |
| `tests/high-load.yaml`               | High load test (5000 TPS cap)                       |
| `tests/node-restart-resilience.yaml` | BN recovery after restart during load               |
| `tests/full-history-backfill.yaml`   | BN backfills history while ingesting live blocks    |
| `tests/rsa-roster-verification.yaml` | Blocks verified via the RSA roster (WRB topologies) |

### Test Definition Schema

Test files are YAML with the following structure:

```yaml
name: my-test                    # Test identifier
description: "What this tests"   # Human-readable description
topology: single                 # Required topology (must be deployed)

events:                          # Events execute sequentially by delay
  - id: start-load
    type: load-start
    description: "Start load generation"
    delay: 5                     # Seconds from test start
    args:
      test_class: CryptoTransferLoadTest
      concurrency: 5
      accounts: 10
      duration: 90
      max_tps: 1000              # Optional TPS cap

assertions:                      # Validations to run after all events
  - id: bn-has-blocks
    type: block-available
    target: block-node-1
    args:
      min_block: 0
      max_block_gte: 10
```

### Event Types

|            Type            |                                           Description                                           |                                             Arguments                                             |
|----------------------------|-------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| `command`                  | Run arbitrary script                                                                            | `script`                                                                                          |
| `node-down`                | Scale node to 0 replicas                                                                        | `target`                                                                                          |
| `node-up`                  | Scale node to 1 replica                                                                         | `target`                                                                                          |
| `scale-down`               | Scale down (alias for node-down)                                                                | `target`                                                                                          |
| `scale-up`                 | Scale up (alias for node-up)                                                                    | `target`                                                                                          |
| `restart`                  | Rollout restart node                                                                            | `target`                                                                                          |
| `load-start`               | Start NLG load                                                                                  | `test_class`, `concurrency`, `accounts`, `duration`, `max_tps`                                    |
| `load-stop`                | Stop NLG load                                                                                   | `test_class`                                                                                      |
| `print-metrics`            | Print metrics summary                                                                           | `target` (node name or "all")                                                                     |
| `network-status`           | Print network status                                                                            | (none)                                                                                            |
| `sleep`                    | Pause execution                                                                                 | `seconds`                                                                                         |
| `port-forward`             | Refresh port forwards                                                                           | (none)                                                                                            |
| `clear-block-storage`      | Clear all block data on node (live, archive, verification, and the persisted block-range state) | `target`                                                                                          |
| `deploy-block-node`        | Deploy new Block Node                                                                           | `name`, `backfill_sources`, `greedy`, `chart_version`, `archive_backend`                          |
| `archive-files-exist`      | Count objects in an S3 bucket                                                                   | `bucket`, `min_files`, `min_increase`, `record_baseline`                                          |
| `reconfigure-cn-streaming` | Update CN block-nodes.json                                                                      | `consensus_node`, `block_nodes`                                                                   |
| `inject-latency`           | Apply a NetworkChaos rule                                                                       | `name`, `source.kind`, `target.kind`, `latency`, `jitter`, `correlation`, `bidirectional`, `loss` |
| `clear-latency`            | Remove a NetworkChaos rule                                                                      | `name`                                                                                            |

### Assertion Types

|            Type            |                           Description                            |                         Arguments                          |
|----------------------------|------------------------------------------------------------------|------------------------------------------------------------|
| `block-available`          | Verify BN has blocks in range                                    | `min_block`, `max_block_gte`                               |
| `node-healthy`             | Verify pod is Running                                            | `target`                                                   |
| `no-errors`                | Verify no verification errors                                    | `target`                                                   |
| `blocks-increasing`        | Verify blocks are actively flowing                               | `wait_seconds`, `max_attempts`                             |
| `rsa-roster-verification`  | Verify blocks accepted via the RSA roster (WRB), no RSA failures | `min_rsa_success`                                          |
| `metric-threshold`         | Compare any BN Prometheus metric                                 | `metric`, `comparator`, `value`, `samples`, `wait_seconds` |
| `block-rate-floor`         | Assert Δblocks/Δtime ≥ floor                                     | `min_rate_per_sec`, `window_seconds`                       |
| `backfill-triggered`       | Assert backfill log marker observed                              | `grep` (default `"backfill"`), `since_seconds`             |
| `log-match`                | Generic log-substring check                                      | `grep`, `since_seconds`                                    |
| `mirror-blocks-increasing` | Verify Mirror Node is importing new blocks                       | `wait_seconds`, `max_attempts`                             |
| `mirror-lag`               | Verify Mirror Node is not falling behind its paired Block Node   | `max_blocks_behind` (default 30)                           |
| `archive-files-exist`      | Verify an S3 archive bucket has (or gained) objects              | `bucket`, `min_files`, `min_increase`                      |
| `archive-contiguous`       | Verify an archive bucket has no gap in its object run            | `bucket`                                                   |

**Note:** `archive-files-exist` with `min_increase: N` asserts "at least N more objects than the recorded baseline" instead of an absolute count. The baseline is written only by an earlier `archive-files-exist` **event** carrying `record_baseline: true`; with no baseline recorded the assertion fails rather than falling back to an absolute count. `archive-contiguous` derives the object-key segment width from the archive grouping level in `scripts/lib/cloud-storage-overlay.sh`, so it takes no `pad` argument.

**Note:** The `blocks-increasing` assertion verifies a Block Node is actively receiving blocks. It measures baseline, waits `wait_seconds` (default: 60), verifies increase, retrying up to `max_attempts` (default: 3) times.

**Note:** Entries under `assertions:` all run *after* every event, so they cannot check anything that is only true partway through a run. Time-sensitive checks belong in a `command` event instead — a failing `command` event fails the test just as a failing assertion does. `scripts/backfill/assert-backfill-during-live-stream.sh` and the `scripts/wrb-distribution/assert-*.sh` scripts follow this pattern.

### Backfill With Live Tail (`full-history-backfill`)

`tests/full-history-backfill.yaml` wipes BN1's data and checks that it refills its history from BN2 *while* its Consensus Node keeps streaming live blocks into it, rather than staying blocked until backfill reaches the chain head. Supporting scripts live in `scripts/backfill/`:

|                 Script                  |                                                                       Purpose                                                                       |
|-----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `wait-for-network-height.sh`            | Blocks until the network reaches `MIN_HEIGHT` (default 600) so there is a recoverable gap; returns at once if already past it                       |
| `configure-bn-live-tail-backfill.sh`    | Raises the BN's `earliestManagedBlock` to `<chain head> + EMB_OFFSET` and throttles backfill, while the BN is scaled to zero                        |
| `assert-backfill-during-live-stream.sh` | Samples `serverStatusDetail` + `publisher_open_connections` and requires the historical range and the live range to both advance in the same window |

The `earliestManagedBlock` is the hinge. On a store-less start the publisher treats it as its next expected block: left at the default `0` the Block Node answers every offer from the Consensus Node with `BlockNodeBehind`, so live streaming only resumes once backfill has walked the whole chain. Raised above the chain head, the first offered block is accepted instead and everything below it becomes a `HISTORICAL` backfill gap.

If the assertion times out reporting no publisher connection, the network most likely produced blocks past the chosen `earliestManagedBlock` before the pod was ready — re-run with a larger `EMB_OFFSET`.

### CI Integration

Run tests via GitHub Actions workflow dispatch:

1. Go to Actions → "Solo E2E Test" → "Run workflow"
2. Select a test from the `test-definition` dropdown (e.g., `basic-load`)
3. The test results will appear in the workflow summary

### Writing Custom Tests

1. Create a new YAML file in `tests/`
2. Define events with appropriate delays
3. Add assertions to validate outcomes
4. Validate with `task test:validate TEST_FILE=tests/my-test.yaml`
5. Run with `task test:run TEST_FILE=tests/my-test.yaml`

The harness's own assertion logic has fixture-based unit tests that need no cluster: `task test:unit` (or `scripts/test/run-all.sh`). They also run in CI ahead of the cluster build.

See `test-schema.yaml` for the complete schema documentation.

## Network Chaos / Latency Tests

Inject configurable network latency between Consensus Nodes and Block Nodes during a test run, to exercise behavior under realistic cross-region conditions. Built on top of [Chaos Mesh](https://chaos-mesh.org/) v2.7.2. **Opt-in only** — the default workflow is unaffected.

The framework supports three latency dimensions:

- **CN ↔ CN** — gossip / consensus traffic
- **BN ↔ BN** — peer backfill mesh
- **CN ↔ BN** — live block-publish stream

For per-scenario details, thresholds, and how to add a new scenario, see [`docs/latency-scenarios.md`](docs/latency-scenarios.md).

### Prerequisites

In addition to the usual prereqs:

- **Solo CLI ≥ 0.63.0** (for the block-node label set Chaos Mesh selects on)
- **Privileged Kubernetes** — Chaos Mesh's daemon runs `privileged=true`, `hostPID=true`, `mountHostLibModules=true`. Local Kind clusters allow this by default; hardened CI clusters may not.

### First-time setup (per cluster session)

```bash
# 1. Bring up a cluster (TSS is on by default — see note below).
task up TOPOLOGY=paired-3

# 2. Install Chaos Mesh (opt-in)
CHAOS_ENABLED=true task chaos:install
```

`task chaos:install` is idempotent — re-running upgrades in place rather than failing.

> **TSS is on by default** (the supported mode). Solo CLI's `--wraps` flag requires CN ≥ v0.74.0-0; `CN_VERSION=latest` is min-enforced by `resolve-versions.sh` to a TSS-capable tag (currently `0.75.0-rc.4`), so `--wraps` deploys cleanly and TSS signatures verify under latency (confirmed: Schnorr→WRAPS transition). Only set `TSS_ENABLED=false` if you pin a `CN_VERSION` below the floor.

### Running a latency test

```bash
CHAOS_ENABLED=true TOPOLOGY=paired-3 task test:run TEST_FILE=tests/latency-cn-to-bn.yaml
```

Available latency tests, grouped by **profile** (see [`docs/latency-scenarios.md`](docs/latency-scenarios.md#choosing-a-latency-profile-baseline--stress--severe) for how to choose):

|              Test File              | Profile  |                Description                 |
|-------------------------------------|----------|--------------------------------------------|
| `tests/chaos-foundation-smoke.yaml` | —        | Plumbing check (inject → confirm → clear)  |
| `tests/latency-cn-to-cn.yaml`       | baseline | 100 ms ± 20 ms between Consensus Nodes     |
| `tests/latency-bn-to-bn.yaml`       | baseline | 200 ms ± 40 ms between Block Nodes         |
| `tests/latency-cn-to-bn.yaml`       | baseline | 150 ms ± 30 ms between CNs and BNs         |
| `tests/latency-all-three.yaml`      | baseline | All three baseline rules concurrently      |
| `tests/latency-stress.yaml`         | stress   | ~3× baseline, bursty — degrade & recover   |
| `tests/latency-severe.yaml`         | severe   | ~5–6× baseline — survival & recovery probe |

- **baseline** — does the network tolerate normal latency with no visible impact? (steady block-rate floor holds)
- **stress** — does it degrade gracefully and recover via backfill? (reduced floor)
- **severe** — does it survive and recover near the breaking point? (recovery-only assertions)

> **CI concurrency:** the `solo-e2e-test` workflow uses `concurrency: solo-network-<topology>` with `cancel-in-progress: true`, so two `paired-3` chaos runs cannot run at once — the newer dispatch cancels the older. Run them one at a time. (A canceled run logs `kind/solo: command not found` in cleanup — that's cancellation noise, not a failure.)

### Inspecting active chaos

```bash
task chaos:status      # active NetworkChaos / PodChaos + Chaos Mesh component health
```

### Cleanup

`task down` automatically invokes `task chaos:cleanup`, so chaos rules do not leak between test cluster lifetimes. To clear rules without tearing down the cluster:

```bash
task chaos:cleanup     # deletes all NetworkChaos / PodChaos resources
```

To fully uninstall Chaos Mesh from the cluster:

```bash
task chaos:uninstall   # helm uninstall + delete the chaos-mesh namespace
```

### Troubleshooting

|                 Symptom                  |                                                       Cause / fix                                                       |
|------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| `task chaos:install` skipped silently    | `CHAOS_ENABLED` is not `true`. Re-run with `CHAOS_ENABLED=true task chaos:install`.                                     |
| `ERROR: Chaos Mesh CRDs not present`     | Chaos Mesh isn't installed in this cluster. Run `CHAOS_ENABLED=true task chaos:install`.                                |
| `source(...) matched 0 pods`             | Selector found no matching pods. Verify `kubectl get pod --show-labels` and the kind ↔ label-key mapping.               |
| Stale NetworkChaos after test crash      | The runner's trap should clean up; if not, `task chaos:cleanup` (or `kubectl delete networkchaos --all -n chaos-mesh`). |
| Test deploy fails on hardened CI cluster | The chaos daemon needs `privileged=true`. Hardened clusters won't allow this. Skip with `CHAOS_ENABLED=false`.          |

### Pointers

- Profiles, per-scenario detail, and how to add a new one: [`docs/latency-scenarios.md`](docs/latency-scenarios.md)
- Upstream Chaos Mesh wrapper this builds on: [solo-chaos](https://github.com/hashgraph/solo-chaos) (for multi-region simulation)

## Troubleshooting

### Full Reset

```bash
task down    # Destroys cluster and cleans Solo config via CLI
task up
```

If `task down` doesn't fully clean up (e.g., Solo CLI errors), manually clean Solo config:

```bash
solo deployment config delete -d deployment-solo -q
solo cluster-ref config disconnect -c kind-solo-cluster -q
# Or nuclear option: rm -f ~/.solo/local-config.yaml
```

### Common Issues

|               Issue                |        Cause         |                   Solution                    |
|------------------------------------|----------------------|-----------------------------------------------|
| Mirror Importer `CrashLoopBackOff` | Waiting for Postgres | Wait 2-3 minutes, recovers automatically      |
| "context deadline exceeded"        | Helm repo timeout    | Retry: `task network:deploy`                  |
| Solo CLI errors                    | Version mismatch     | `npm i @hashgraph/solo@0.63.0 -g`             |
| Port already in use                | Stale port-forwards  | `task port-forward:stop && task port-forward` |

### Debugging

```bash
# Check pod status
kubectl get pods -n solo-network

# Describe pod for events/errors
kubectl describe pod <pod-name> -n solo-network

# View pod logs
kubectl logs <pod-name> -n solo-network
```

### Backfill Not Working

If backfill shows "Unable to reach node" errors but pods are running:

1. **Test TCP connectivity** from the failing pod:

   ```bash
   # Exec into the pod that can't connect
   kubectl exec -it -n solo-network block-node-2-0 -- /bin/bash

   # Test TCP to target (curl is usually available, nc may not be)
   curl -v telnet://block-node-1.solo-network.svc.cluster.local:40840
   # Success: "Connected to block-node-1..."
   # Failure: "Could not resolve host" or "Connection refused"
   ```
2. **Check service exists**:

   ```bash
   kubectl get svc -n solo-network | grep block-node
   ```

## Scheduled Runs

The `solo-e2e-scheduler.yml` workflow runs tests automatically:

|  Run Type   |      Trigger      | Deployments |         Versions          |
|-------------|-------------------|-------------|---------------------------|
| **Daily**   | Mon-Fri 6 AM UTC  | 2           | BN=`main`, CN/MN=`latest` |
| **Weekend** | Saturday 2 AM UTC | 6           | BN=`main`, CN/MN=`latest` |
| **RC**      | Sunday 2 AM UTC   | 6           | BN=`main`, CN/MN=`rc`     |
| **TAG**     | Push `v*` tag     | 6           | BN=tag, CN/MN=`latest`    |

### Test Matrix

Tests are validated against topologies before execution. The matrix defines which tests run on each topology:

|       Topology        |                               Tests                                |
|-----------------------|--------------------------------------------------------------------|
| `single`              | `smoke-test`, `basic-load`, `high-load`, `node-restart-resilience` |
| `paired-3`            | `smoke-test`, `basic-load`, `high-load`                            |
| `3cn-1bn`             | `smoke-test`                                                       |
| `fan-out-3cn-2bn`     | `smoke-test`                                                       |
| `2cn-2bn-backfill`    | `full-history-backfill`                                            |
| `7cn-3bn-distributed` | `smoke-test`                                                       |
| `single-wrb-rsa`      | `smoke-test`, `rsa-roster-verification`                            |
| `3cn-2bn-wrb-rsa`     | `smoke-test`, `rsa-roster-verification`                            |

Multiple tests run sequentially on the same deployment, reducing CI time.

Up to **3 deployments run in parallel** within a scheduler run (`max-parallel: 3`). Each matrix
entry gets its own runner and its own Kind cluster, so they do not contend with one another;
the cap limits how many runners — and how many concurrent Consensus Node builds, when the
resolved CN version is a `-SNAPSHOT` — a single run consumes.

> Every topology appears at most once per matrix, which matters: `solo-e2e-test.yml` keys its
> concurrency group on `topology` with `cancel-in-progress: true`, so two parallel jobs sharing
> a topology would cancel each other. Keep topologies unique per matrix when adding entries.

Manual trigger: Actions -> "Solo E2E Scheduler" -> "Run workflow"
