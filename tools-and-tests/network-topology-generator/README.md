## Network Topology Generator

This tool defines a shared network-topology format and utilities for validating
and materialising test configurations. It currently ships with:
- `network-topology.schema.yaml` &mdash; the canonical schema shared across teams.
- `examples/` &mdash; sample topologies that cover simple and larger network shapes.
- `scripts/generate-configs.sh` &mdash; helper that renders node-specific configs from a topology file.

### Prerequisites

- `yq` (YAML processor) and `jq` (JSON processor) used by the generator script.
- A JSON/YAML Schema validator (`yajsv` or `jsonschema`) to lint topologies against the schema.

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
