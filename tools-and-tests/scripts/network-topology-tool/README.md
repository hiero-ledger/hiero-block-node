# Network Topology Tool

Define and validate Hiero network topologies for testing and deployment.

## Schema

Topologies are defined in YAML following `network-topology.schema.yaml`. The schema supports:

|       Section       | Required |                           Description                            |
|---------------------|----------|------------------------------------------------------------------|
| `name`              | Yes      | Topology identifier                                              |
| `description`       | No       | Human-readable description                                       |
| `block_nodes`       | Yes      | Block nodes that receive streams from consensus nodes            |
| `consensus_nodes`   | Yes      | Consensus nodes that produce block streams                       |
| `mirror_nodes`      | No       | Mirror nodes that subscribe to block nodes                       |
| `relay_nodes`       | No       | JSON-RPC relay nodes that connect to mirror nodes                |
| `explorer_nodes`    | No       | Explorer UI nodes that display data from mirror nodes            |
| `s3_nodes`          | No       | S3-compatible storage (RustFS) that archiving BNs upload to      |
| `verification_mode` | No       | `tss` (default) or `rsa-wrb` for Wrapped Record Block topologies |

### Block Node Fields

Each entry under `block_nodes` accepts:

|          Field           | Required |                                              Description                                              |
|--------------------------|----------|-------------------------------------------------------------------------------------------------------|
| `address`                | Yes      | Service name used for in-cluster DNS                                                                  |
| `port`                   | Yes      | gRPC port (40840 in every bundled topology)                                                           |
| `peers`                  | No       | Other BNs this node backfills from, in priority order                                                 |
| `greedy`                 | No       | Greedy backfill mode, i.e. pull all history rather than only fill gaps (default false)                |
| `backfill_scan_interval` | No       | Gap-scan interval in ms (default 15000)                                                               |
| `grpc_tuning`            | No       | Web-client tuning read off the **peer** entry by whoever backfills from it (see below)                |
| `plugin_ports`           | No       | Per-plugin port overrides; camelCase keys matching `blockNode.ports` in the chart                     |
| `flavor`                 | No       | Shipped plugin profile: `minimal`, `lfh`, `rfh` or `all` (see below)                                  |
| `archive`                | No       | `backend: rustfs` plus `node:`, a key in `s3_nodes`; requires a flavor with the cloud-storage plugins |

`flavor` replaces the chart's `plugins.names` with
`charts/block-node-server/values-overrides/plugin-profile-<flavor>.yaml`. `rfh` and `minimal` run
no `stream-publisher`/`stream-subscriber`, so such a node must not be listed under any consensus
or mirror node. See `../solo-e2e-test/README.md` ("Block Node Plugin Flavors") for the full
operational caveats.

`grpc_tuning` is read from the entry of the node being connected **to**: to widen the frames a
backfill client uses, set it on the peer it pulls from, not on the client itself.

## Node References

Nodes can reference other nodes in three formats:

```yaml
# Simple reference (string)
block_nodes: [block-node-1, block-node-2]

# With priority (object)
block_nodes:
  - node: block-node-1
    priority: 1
  - node: block-node-2
    priority: 2

# Inline priority (shorthand)
block_nodes:
  - block-node-1: 1
  - block-node-2: 2
```

## Topologies

Production topologies are located in `../solo-e2e-test/topologies/`. Example topologies for trying out the tool are in `./examples/`.

|         Name          | CN | BN |             Description              |
|-----------------------|----|----|--------------------------------------|
| `single`              | 1  | 1  | Minimal setup for basic testing      |
| `paired-3`            | 3  | 3  | Each CN streams to its paired BN     |
| `fan-out-3cn-2bn`     | 3  | 2  | All CNs can stream to all BNs        |
| `minimal`             | 1  | 1  | No mirror/relay/explorer             |
| `7cn-3bn-distributed` | 7  | 3  | Grouped CNs stream to designated BNs |

## Example

```yaml
name: my-topology
description: "2 Consensus Nodes, 1 Block Node"

block_nodes:
  block-node-1:
    address: block-node-1
    port: 40840
    greedy: false  # Enable greedy backfill mode (defaults to false)
    backfill_scan_interval: 15000  # Backfill scan interval in ms (defaults to 15000)
  block-node-2:
    address: block-node-2
    port: 40840
    flavor: rfh          # cloud-only profile: no local storage, no publisher/subscriber
    peers: [block-node-1]
    greedy: true
    archive:
      backend: rustfs
      node: rustfs-1

s3_nodes:
  rustfs-1:
    buckets: [block-archive-tar, block-archive-expanded]
    port: 9000

consensus_nodes:
  node1:
    block_nodes: [block-node-1]
  node2:
    block_nodes: [block-node-1]

mirror_nodes:
  mirror-1:
    block_nodes: [block-node-1]

relay_nodes:
  relay-1:
    mirror_nodes: [mirror-1]

explorer_nodes:
  explorer-1:
    mirror_nodes: [mirror-1]
```

## Validation

```bash
# Install check-jsonschema (pip install check-jsonschema)
check-jsonschema --schemafile network-topology.schema.yaml examples/single.yaml
```

## Helm Overlay Generator

Generate Helm values overlays from topology files for Solo CLI deployments.

### Usage

```bash
./generate-chart-values-config-overlays.sh <topology-file> [options]
```

### Arguments

|     Argument      |              Description              |
|-------------------|---------------------------------------|
| `<topology-file>` | Path to topology YAML file (required) |

### Options

|     Option     |         Default         |            Description             |
|----------------|-------------------------|------------------------------------|
| `--namespace`  | `solo-network`          | Kubernetes namespace for DNS names |
| `--output-dir` | `./out/<topology-name>` | Directory for output files         |

### Output Files

|        File Pattern        |                      Description                       |
|----------------------------|--------------------------------------------------------|
| `bn-<node-id>-values.yaml` | Block Node overlay (only generated for BNs with peers) |
| `mn-<node-id>-values.yaml` | Mirror Node overlay                                    |

### Block Node Overlay

Generated for block nodes that have `peers` configured. The `greedy` value comes from the topology (defaults to false):

```yaml
blockNode:
  config:
    BACKFILL_BLOCK_NODE_SOURCES_PATH: "/opt/hiero/block-node/backfill/block-node-sources.json"
    BACKFILL_GREEDY: "false"  # Value from topology's greedy field
    BACKFILL_SCAN_INTERVAL: "15000"  # Value from topology's backfill_scan_interval field
  backfill:
    path: "/opt/hiero/block-node/backfill"
    filename: "block-node-sources.json"
    sources:
      - address: "block-node-2.solo-network.svc.cluster.local"
        port: 40840
        priority: 1
```

### Mirror Node Overlay

Generated for all Mirror Nodes with their configured Block Node connections:

```yaml
config:
  hiero:
    mirror:
      importer:
        block:
          enabled: true
          nodes:
            - host: block-node-1.solo-network.svc.cluster.local
              port: 40840
          sourceType: BLOCK_NODE
```

### Examples

```bash
# Try the tool with included examples
./generate-chart-values-config-overlays.sh examples/single.yaml
./generate-chart-values-config-overlays.sh examples/fan-out-3cn-2bn.yaml

# Specify namespace and custom output directory
./generate-chart-values-config-overlays.sh examples/fan-out-3cn-2bn.yaml --namespace my-ns --output-dir ./overlays

# Results for fan-out topology (default output):
# ./out/fan-out-3cn-2bn/bn-block-node-1-values.yaml  (backfill from block-node-2)
# ./out/fan-out-3cn-2bn/bn-block-node-2-values.yaml  (backfill from block-node-1)
# ./out/fan-out-3cn-2bn/mn-mirror-1-values.yaml      (connects to both BNs)
```

### Integration with Solo CLI

```bash
# Generate overlays, then deploy
./generate-chart-values-config-overlays.sh examples/fan-out-3cn-2bn.yaml
solo block node add -d my-deployment -f ./out/fan-out-3cn-2bn/bn-block-node-1-values.yaml
solo mirror node add -d my-deployment -f ./out/fan-out-3cn-2bn/mn-mirror-1-values.yaml
```

The `solo-e2e-test/scripts/solo-deploy-network.sh` script automatically uses this generator when deploying networks.

## Usage with Solo E2E Test

The `solo-e2e-test` directory contains the topologies and scripts:

```bash
cd ../solo-e2e-test
task up                     # Uses single topology
task up TOPOLOGY=paired-3   # Uses paired-3 topology
```

See `../solo-e2e-test/README.md` for more details.
