## Network Topology Generator

This tool defines a shared network-topology format and utilities for validating
and materialising test configurations. It currently includes:
- `network-topology.schema.yaml` &mdash; the canonical schema shared across teams.
- `examples/` &mdash; sample topologies that cover simple and larger network shapes.
- `scripts/generate-configs.sh` &mdash; helper that renders node-specific configs from a topology file.

This schema and tooling objective is to:
- Provide a common format for defining network topologies across teams.
- Enable easy validation of topologies against a shared schema.
- Simplify generation of per-node configuration files for test deployments.
- Facilitate sharing and reuse of network topologies in tests.

### Prerequisites

- `yq` (YAML processor) and `jq` (JSON processor) used by the generator script.
- A JSON/YAML Schema validator (for example `check-jsonschema`) to lint topologies against the schema.

On macOS you can install the CLI dependencies with Homebrew:

```bash
brew install jq
brew install yq

# schema validator
brew install check-jsonschema
```

### Validate a topology

Lint any topology file against the shared schema before committing it:

```bash
check-jsonschema --schemafile network-topology.schema.yaml examples/*.yaml
```

### Generate per-node configs

Use the helper script to materialise config bundles for each block node (BN) and consensus node (CN):

Simple example:

```bash
scripts/generate-configs.sh examples/simple-1-1.yaml
```

Large example:

```bash
scripts/generate-configs.sh examples/7CNs-4BNs-large.yaml
```

Larger example:

```bash
scripts/generate-configs.sh examples/7CNs-2BNs-each.yaml
```

Shared priorities example:

```bash
scripts/generate-configs.sh examples/4CNs-4BNs-shared-priority.yaml
```

YAML supports connection-local priorities in two syntaxes. Both of the following
produce peers with priority `1` when generating the BN configs:

```yaml
peers:
  - bn2: 1           # shorthand single-key map (preferred for quick edits)
  - node: bn3        # explicit object form for richer tooling support
    priority: 1
```

`consensus_nodes.*.block_nodes` accepts the same forms, so you can reuse priorities
when mapping consensus nodes to block nodes. See
`examples/4CNs-4BNs-shared-priority.yaml` for a complete topology showcasing
shared priorities across multiple links.

The script writes JSON configs under `out/<node>/config.json`. Example output for the `cn1`
node in the `simple-1-1` topology:

```json
{
  "nodes": [
    {
      "address": "localhost",
      "port": 40800,
      "priority": 1
    }
  ]
}
```

Priorities are resolved in the following order when rendering configs:
1. Connection-level override (from the peer/block list entry)
2. The referenced block node's `priority` property
3. Default incremental order within the list
