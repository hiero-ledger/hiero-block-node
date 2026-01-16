# Solo E2E Test - Local Development Helper

Deploy Hiero networks locally for development and testing using [Solo CLI](https://github.com/hashgraph/solo) and [Kind](https://kind.sigs.k8s.io/).

## Why This Exists

The CI workflow (`.github/workflows/solo-e2e-test.yml`) deploys Hiero networks for end-to-end testing. This Taskfile provides **the same deployment locally** by calling the same shared scripts that CI uses.

```
+---------------------------------------------------------------------+
|                     Shared Scripts                                  |
|  .github/workflows/support/scripts/                                 |
|  +-- resolve-versions.sh    (resolve 'latest' -> actual tags)       |
|  +-- solo-setup-cluster.sh  (create cluster, init Solo)             |
|  +-- solo-deploy-network.sh (deploy BN, CN, MN, Relay)              |
+---------------------------------------------------------------------+
           ^                                    ^
           |                                    |
    +------+------+                    +--------+--------+
    |  Taskfile   |                    |  CI Workflow    |
    |  (local)    |                    |  (GitHub)       |
    |  task up    |                    |  workflow_      |
    |             |                    |  dispatch       |
    +-------------+                    +-----------------+

+---------------------------------------------------------------------+
|                     Topology Files                                  |
|  tools-and-tests/scripts/network-topology-tool/topologies/          |
|  +-- single.yaml          (1 CN, 1 BN)                              |
|  +-- paired-3.yaml        (3 CN, 3 BN)                              |
|  +-- fan-out-3cn-2bn.yaml (3 CN, 2 BN)                              |
+---------------------------------------------------------------------+
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

```bash
task up                    # Full setup (cluster + network + port-forwards)
task down                  # Tear down everything
task restart               # down + up
```

### Topologies

```bash
task up                    # single (1 CN, 1 BN) - default
task up:paired             # paired-3 (3 CN, 3 BN)
task up:fan-out            # fan-out-3cn-2bn (3 CN, 2 BN)
task up TOPOLOGY=<name>    # any topology by name
task topologies            # list available topologies
```

### Verification

```bash
task verify                # Check Block Node via gRPC
task verify:mirror         # Check Mirror Node REST API
task cluster:status        # Show all pods and services
task logs:bn               # Stream Block Node logs
task logs:cn               # Stream Consensus Node logs
task logs:mn               # Stream Mirror Node logs
```

### Load Generation

```bash
task load:start            # Start NLG with defaults (10 TPS, 5m)
task load:start TPS=100 DURATION=10m  # Custom TPS and duration
task load:start TEST_CLASS=HCSLoadTest  # Use different test class
task load:status           # Check load generator logs, TODO, fix this, is not showing anything but it works :)
task load:stop             # Stop load generation
```

Combined convenience task:

```bash
task up:load               # Deploy network AND start load generation
task up:load TPS=100 DURATION=10m  # With custom settings
```

### Utilities

```bash
task solo:install          # Install/update Solo CLI
task resolve-versions      # Show resolved GA versions
task port-forward          # Restart port forwards
```

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

|    Variable    |    Default     |        Description         |
|----------------|----------------|----------------------------|
| `TOPOLOGY`     | `single`       | Network topology to deploy |
| `CLUSTER_NAME` | `solo-cluster` | Kind cluster name          |
| `NAMESPACE`    | `solo-network` | Kubernetes namespace       |
| `CN_VERSION`   | `latest`       | Consensus Node version     |
| `MN_VERSION`   | `latest`       | Mirror Node version        |
| `BN_VERSION`   | `latest`       | Block Node version         |
| `SOLO_VERSION` | `latest`       | Solo CLI version           |

### Version Keywords

| Keyword  |          Resolves To          |
|----------|-------------------------------|
| `latest` | Latest GA release from GitHub |
| `main`   | Current development snapshot  |
| `v0.x.y` | Specific version tag          |

### Command-Line Overrides

```bash
task up TOPOLOGY=paired-3 BN_VERSION=v0.24.0
```

## Load Generation

Both the CI workflow and local Taskfile support configurable transaction load generation using Solo's Network Load Generator (NLG). This allows testing at various TPS levels from 1 to 20,000.

### Local Load Generation

```bash
# Deploy network AND start load in one command
task up:load TPS=100 DURATION=10m

# Or deploy first, then start load separately
task up
task load:start TPS=100 DURATION=10m

# Check load generator status
task load:status

# Stop load generation
task load:stop
```

### CI Workflow Inputs

|      Input       |        Default         |                      Description                      |
|------------------|------------------------|-------------------------------------------------------|
| `load-tps`       | `10`                   | Approx TPS (1-20000, 0 to disable). Maps to concurrency/accounts. |
| `load-duration`  | `5m`                   | Duration (e.g., `5m`, `1h`, `30s`, or plain seconds like `300`) |
| `load-test-class`| `CryptoTransferLoadTest` | NLG test class to run                               |

> **Note:** NLG doesn't have direct TPS control. The TPS value is used to calculate appropriate concurrency and account counts. Actual throughput depends on network capacity and test class.

### Available Test Classes

|       Test Class        |                  Description                  |
|-------------------------|-----------------------------------------------|
| `CryptoTransferLoadTest`| HBAR transfers between accounts (default)     |
| `HCSLoadTest`           | Hedera Consensus Service message submissions  |
| `TokenTransferLoadTest` | HTS token transfers                           |

### Example: Running High TPS Test

Via GitHub Actions workflow dispatch:

1. Go to Actions → "Solo E2E Test" → "Run workflow"
2. Set parameters:
   - `load-tps`: `1000`
   - `load-duration`: `10m`
   - `load-test-class`: `CryptoTransferLoadTest`

### How It Works

The load generator:

1. Deploys the NLG pod into the cluster via `solo rapid-fire load start`
2. Creates test accounts based on TPS (scales automatically)
3. Generates transactions using calculated concurrency and accounts
4. Runs for the specified duration (converted to seconds internally)
5. Cleans up via `solo rapid-fire load stop`

**NLG Parameters** (passed via `--args`):
- `-c` = concurrency (parallel clients)
- `-a` = accounts (test accounts to create)
- `-t` = time in seconds

### NLG Parameters (Auto-Calculated)

| Parameter   |                  Calculation                  |
|-------------|-----------------------------------------------|
| Accounts    | `TPS < 100 ? 10 : TPS / 10`                   |
| Concurrency | `TPS < 50 ? 5 : (TPS < 500 ? 10 : 32)`        |

## Topologies

Topologies define network configuration. Located in `../network-topology-tool/topologies/`.

|       Name        | CN | BN |               Use Case               |
|-------------------|----|----|--------------------------------------|
| `single`          | 1  | 1  | Basic testing, fastest startup       |
| `paired-3`        | 3  | 3  | Multi-node testing, each CN->BN pair |
| `fan-out-3cn-2bn` | 3  | 2  | Redundancy testing, all CNs->all BNs |

See `../network-topology-tool/README.md` for topology schema details.

## How It Works

### Script Flow

```
task up
  |
  +-> task cluster:create
  |     +-> solo-setup-cluster.sh
  |           +-- Create Kind cluster
  |           +-- solo init
  |           +-- solo cluster-ref config connect
  |           +-- solo deployment config create
  |           +-- solo deployment cluster attach
  |
  +-> task network:deploy
  |     +-> resolve-versions.sh (latest -> v0.x.y)
  |     +-> solo-deploy-network.sh
  |           +-- Load topology YAML
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
        +-- kubectl port-forward (multiple services)
```

### CI Workflow Equivalence

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
  run: .github/workflows/support/scripts/solo-setup-cluster.sh ...

- name: Deploy network
  run: .github/workflows/support/scripts/solo-deploy-network.sh ...
```

## Endpoints

After deployment with port-forwards active:

|       Service       |        Endpoint         |
|---------------------|-------------------------|
| Consensus Node gRPC | `localhost:50211`       |
| Block Node gRPC     | `localhost:40840`       |
| Mirror REST API     | `http://localhost:5551` |
| Mirror Monitor      | `http://localhost:5600` |
| Mirror REST Java    | `http://localhost:8084` |

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

### Mirror Importer CrashLoopBackOff

During initial deployment, `mirror-1-importer` may show `CrashLoopBackOff` status. This is **expected** - it's waiting for Postgres to be ready. It will recover automatically within 2-3 minutes.

### Network Timeouts

Helm repo operations may timeout with "context deadline exceeded". Retry the deployment:

```bash
task network:deploy
```

### Solo Version Issues

```bash
task solo:install SOLO_VERSION=0.52.0
```

### Check What's Running

```bash
task cluster:status
kubectl describe pod <pod-name> -n solo-network
kubectl logs <pod-name> -n solo-network
```

### Backfill Not Working Between Block Nodes

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

### Multi-Node Port Forwarding

When using multi-node topologies, ports are assigned incrementally:

|   Service   | Node 1 | Node 2 | Node 3 |
|-------------|--------|--------|--------|
| Block Node  | 40840  | 40841  | 40842  |
| Mirror REST | 5551   | 5552   | 5553   |
| Relay       | 7546   | 7547   | 7548   |

```bash
task verify NODE=2        # Check Block Node 2 on port 40841
task logs:bn NODE=2       # View Block Node 2 logs
```
