# Network Topology Tool

Define and validate Hiero network topologies for testing and deployment.

## Schema

Topologies are defined in YAML following `network-topology.schema.yaml`. The schema supports:

|      Section      | Required |                      Description                      |
|-------------------|----------|-------------------------------------------------------|
| `name`            | Yes      | Topology identifier                                   |
| `description`     | No       | Human-readable description                            |
| `block_nodes`     | Yes      | Block nodes that receive streams from consensus nodes |
| `consensus_nodes` | Yes      | Consensus nodes that produce block streams            |
| `mirror_nodes`    | No       | Mirror nodes that subscribe to block nodes            |
| `relay_nodes`     | No       | JSON-RPC relay nodes that connect to mirror nodes     |
| `explorer_nodes`  | No       | Explorer UI nodes that display data from mirror nodes |

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

|       Name        | CN | BN |           Description            |
|-------------------|----|----|----------------------------------|
| `single`          | 1  | 1  | Minimal setup for basic testing  |
| `paired-3`        | 3  | 3  | Each CN streams to its paired BN |
| `fan-out-3cn-2bn` | 3  | 2  | All CNs can stream to all BNs    |

## Example

```yaml
name: my-topology
description: "2 Consensus Nodes, 1 Block Node"

block_nodes:
  block-node-1:
    address: block-node-1
    port: 40840

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
check-jsonschema --schemafile network-topology.schema.yaml topologies/single.yaml
```

## Usage with Solo E2E Test

The `solo-e2e-test` scripts use these topologies for network deployment:

```bash
cd ../solo-e2e-test
task up TOPOLOGY=single
task up TOPOLOGY=paired-3
```
