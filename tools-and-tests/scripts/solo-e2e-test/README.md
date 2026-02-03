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
|  |   +-- solo-deploy-network.sh (deploy BN, CN, MN, Relay)          |
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

|         Variable         |         Default          |                        Description                        |
|--------------------------|--------------------------|-----------------------------------------------------------|
| `TOPOLOGY`               | `single`                 | Network topology to deploy                                |
| `CLUSTER_NAME`           | `solo-cluster`           | Kind cluster name                                         |
| `NAMESPACE`              | `solo-network`           | Kubernetes namespace                                      |
| `DEPLOYMENT`             | `deployment-solo`        | Solo deployment name                                      |
| `SOLO_VERSION`           | `0.54.0`                 | Solo CLI version (CI default)                             |
| `CN_VERSION`             | `latest`                 | Consensus Node version                                    |
| `MN_VERSION`             | `latest`                 | Mirror Node version                                       |
| `BN_VERSION`             | `latest`                 | Block Node version                                        |
| `NLG_TEST_TYPE`          | `CryptoTransferLoadTest` | NLG test class                                            |
| `NLG_ARGS`               | `-c 5 -a 10 -tt 300`     | NLG arguments (-c concurrency, -a accounts, -tt duration) |
| `NLG_MAX_TPS`            | (empty)                  | Optional max transactions per second                      |
| `MIRROR_NODE_PINGER_TPS` | `5`                      | Mirror Node pinger TPS (0 to disable, CI only)            |
| `ENABLE_LOCAL_METRICS`   | `false`                  | Enable Prometheus+Grafana stack locally                   |
| `TEST_FILE`              | `none`                   | Test definition file for `task test:run`                  |
| `TCK_SDK_DIR`            | `sdk-tck`                | Directory for TCK-SDK repositories                        |
| `TCK_TEST_FILE`          | (crypto transfer test)   | TCK test file to run                                      |

### Version Keywords

| Keyword  |                   Resolves To                   |        Notes        |
|----------|-------------------------------------------------|---------------------|
| `latest` | Latest GA release from GitHub                   | All components      |
| `main`   | Current development snapshot                    | **Block Node only** |
| `rc`     | Latest Release Candidate (tag containing `-rc`) | All components      |
| `v0.x.y` | Specific version tag                            | All components      |

> **Note:** The `main` keyword only works for Block Node. Consensus Node and Mirror Node do not publish `main` snapshots.

### Command-Line Overrides

Variables can be overridden on the command line for one-off runs. Command-line values take precedence over `.env` file settings:

```bash
task up TOPOLOGY=paired-3 BN_VERSION=v0.24.0
task load:up NLG_ARGS="-c 10 -a 20 -tt 600"
```

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

|    Variable     |                         Default                         |            Description             |
|-----------------|---------------------------------------------------------|------------------------------------|
| `TCK_SDK_DIR`   | `sdk-tck`                                               | Directory for TCK/SDK repositories |
| `TCK_TEST_FILE` | `src/tests/crypto-service/test-transfer-transaction.ts` | Test file to run                   |

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

|         Name          | CN | BN |                      Use Case                       |
|-----------------------|----|----|-----------------------------------------------------|
| `single`              | 1  | 1  | Basic testing, fastest startup                      |
| `paired-3`            | 3  | 3  | Multi-node testing, each CN->BN pair                |
| `fan-out-3cn-2bn`     | 3  | 2  | Redundancy testing, all CNs->all BNs                |
| `3cn-1bn`             | 3  | 1  | Single BN receiving from multiple CNs               |
| `minimal`             | 1  | 1  | CN+BN only, no mirror/relay/explorer                |
| `2cn-2bn-backfill`    | 2  | 2  | Backfill testing, BN recovery after data loss       |
| `7cn-3bn-distributed` | 7  | 3  | Distributed streaming, grouped CN->BN with backfill |

See `../network-topology-tool/README.md` for topology schema details.

## CI Integration

The CI workflow (`.github/workflows/solo-e2e-test.yml`) uses the same scripts as local development.

### Workflow Inputs

|           Input            |         Default          |                     Description                      |
|----------------------------|--------------------------|------------------------------------------------------|
| `topology`                 | `single`                 | Network topology to deploy                           |
| `block-node-version`       | `latest`                 | BN version (`latest`, `main`, `rc`, or specific tag) |
| `consensus-node-version`   | `latest`                 | CN version (`latest`, `rc`, or specific tag)         |
| `mirror-node-version`      | `latest`                 | MN version (`latest`, `rc`, or specific tag)         |
| `solo-version`             | `0.54.0`                 | Solo CLI version                                     |
| `nlg-enabled`              | `false`                  | Enable NLG load generation                           |
| `nlg-test-type`            | `CryptoTransferLoadTest` | NLG test class                                       |
| `nlg-args`                 | `-c 5 -a 10 -tt 300`     | NLG arguments (combined `-c`, `-a`, `-tt`)           |
| `nlg-max-tps`              | (empty)                  | Optional TPS rate limit                              |
| `mirror-node-pinger-tps`   | `5`                      | Mirror Node pinger TPS (0 to disable)                |
| `test-definition`          | `none`                   | Test definitions (comma-separated)                   |
| `run-tck-regression-tests` | `false`                  | Run TCK-SDK regression tests                         |

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

|              Test File               |                   Description                   |
|--------------------------------------|-------------------------------------------------|
| `tests/smoke-test.yaml`              | Quick validation of network functionality       |
| `tests/basic-load.yaml`              | Basic load test (1000 TPS cap)                  |
| `tests/high-load.yaml`               | High load test (5000 TPS cap)                   |
| `tests/node-restart-resilience.yaml` | BN recovery after restart during load           |
| `tests/full-history-backfill.yaml`   | BN recovery via backfill after simulated outage |

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

|            Type            |           Description            |                           Arguments                            |
|----------------------------|----------------------------------|----------------------------------------------------------------|
| `command`                  | Run arbitrary script             | `script`                                                       |
| `node-down`                | Scale node to 0 replicas         | `target`                                                       |
| `node-up`                  | Scale node to 1 replica          | `target`                                                       |
| `scale-down`               | Scale down (alias for node-down) | `target`                                                       |
| `scale-up`                 | Scale up (alias for node-up)     | `target`                                                       |
| `restart`                  | Rollout restart node             | `target`                                                       |
| `load-start`               | Start NLG load                   | `test_class`, `concurrency`, `accounts`, `duration`, `max_tps` |
| `load-stop`                | Stop NLG load                    | `test_class`                                                   |
| `print-metrics`            | Print metrics summary            | `target` (node name or "all")                                  |
| `network-status`           | Print network status             | (none)                                                         |
| `sleep`                    | Pause execution                  | `seconds`                                                      |
| `port-forward`             | Refresh port forwards            | (none)                                                         |
| `clear-block-storage`      | Clear all block data on node     | `target`                                                       |
| `deploy-block-node`        | Deploy new Block Node            | `name`, `backfill_sources`, `greedy`, `chart_version`          |
| `reconfigure-cn-streaming` | Update CN block-nodes.json       | `consensus_node`, `block_nodes`                                |

### Assertion Types

|        Type         |            Description             |           Arguments            |
|---------------------|------------------------------------|--------------------------------|
| `block-available`   | Verify BN has blocks in range      | `min_block`, `max_block_gte`   |
| `node-healthy`      | Verify pod is Running              | `target`                       |
| `no-errors`         | Verify no verification errors      | `target`                       |
| `blocks-increasing` | Verify blocks are actively flowing | `wait_seconds`, `max_attempts` |

**Note:** The `blocks-increasing` assertion verifies a Block Node is actively receiving blocks. It measures baseline, waits `wait_seconds` (default: 60), verifies increase, retrying up to `max_attempts` (default: 3) times.

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

See `test-schema.yaml` for the complete schema documentation.

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
| Solo CLI errors                    | Version mismatch     | `npm i @hashgraph/solo@0.54.0 -g`             |
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

|       Topology        |                         Tests                         |
|-----------------------|-------------------------------------------------------|
| `single`              | `smoke-test`, `basic-load`, `node-restart-resilience` |
| `paired-3`            | `smoke-test`, `basic-load`                            |
| `3cn-1bn`             | `smoke-test`                                          |
| `fan-out-3cn-2bn`     | `smoke-test`                                          |
| `2cn-2bn-backfill`    | `full-history-backfill`                               |
| `7cn-3bn-distributed` | `smoke-test`                                          |

Multiple tests run sequentially on the same deployment, reducing CI time.

Manual trigger: Actions -> "Solo E2E Scheduler" -> "Run workflow"
