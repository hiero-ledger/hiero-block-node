# Choosing Block Node Plugins

A Block Node is built from plugins. Each plugin adds one capability, and the set you load decides what the node can do. This page lists the plugins a working Tier 1 Block Node needs, what each optional plugin adds, which plugin profile fits your deployment, and how to select and check your plugins.

## Required plugins

A [Tier 1](../glossary.md#tier-1-block-node) Block Node receives blocks from Consensus Nodes, verifies them, stores them, and reports its status. It needs these seven plugins:

|        Plugin        |                                                             What it does                                                             |                                                    Without it                                                     |
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `facility-messaging` | Passes blocks and notifications between plugins                                                                                      | The Block Node does not start: `No BlockMessagingFacility provided`                                               |
| `facility-app-state` | Holds and persists the node's state, such as [TSS](../glossary.md#tss-hints) data, address book history, and the stored block ranges | The Block Node does not start: `No ApplicationStateFacility provided`. Required from Block Node 0.45.0            |
| `stream-publisher`   | Receives the block stream from Consensus Nodes                                                                                       | The node cannot receive blocks from Consensus Nodes                                                               |
| `block-verification` | Verifies every block, whatever its source                                                                                            | Blocks arrive but are never acknowledged or stored, so they pile up in the Consensus Nodes' buffers               |
| `blocks-file-recent` | Stores recent blocks on local disk                                                                                                   | Blocks arrive but are never acknowledged or made available                                                        |
| `server-status`      | Answers `serverStatus`, which Consensus Nodes use to choose a Block Node                                                             | Consensus Nodes do not stream blocks to the node                                                                  |
| `health`             | Serves the `/healthz/livez` and `/healthz/readyz` endpoints that the Kubernetes probes call                                          | On Kubernetes, the pod never becomes ready, and the failing liveness probe makes Kubernetes restart it repeatedly |

> **Note:** A [Tier 2](../glossary.md#tier-2-block-node) Block Node does not receive blocks from Consensus Nodes. Remove `stream-publisher` from its plugin list and add `backfill`, which fetches blocks from upstream Block Nodes.

## Optional plugins

Add these plugins for the features your node provides:

|          Plugin          |                                                                        What it adds                                                                        |                                                                                          Add it when                                                                                          |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stream-subscriber`      | Streams blocks to Mirror Nodes and downstream Block Nodes, and serves peer Block Nodes that backfill from this node                                        | Anything subscribes to or backfills from this node, for example a Mirror Node                                                                                                                 |
| `block-access-service`   | Returns a single block on request (`getBlock`)                                                                                                             | Clients fetch individual blocks                                                                                                                                                               |
| `blocks-file-historic`   | Keeps long-term block history on local disk in compressed form                                                                                             | The node keeps full history locally                                                                                                                                                           |
| `backfill`               | Detects missing blocks and fetches them from peer Block Nodes                                                                                              | The node can fall behind, starts without full history, or is a Tier 2 node                                                                                                                    |
| `roster-bootstrap-rsa`   | Loads the RSA address book history from a peer Block Node or a Mirror Node, for verifying [Wrapped Record Blocks](../glossary.md#wrb-wrapped-record-block) | The node verifies Wrapped Record Blocks and you do not supply the RSA address book file yourself (see [Application State Configuration](../configuration.md#application-state-configuration)) |
| `roster-bootstrap-tss`   | Fetches TSS data from peer Block Nodes                                                                                                                     | The node does not process block 0 itself, which is where the network publishes its [ledger ID](../glossary.md#ledger-id)                                                                      |
| `cloud-storage-archive`  | Uploads verified blocks to cloud storage in `tar` archives                                                                                                 | You archive block history in cloud storage                                                                                                                                                    |
| `cloud-storage-expanded` | Uploads each verified block as its own compressed object to S3-compatible storage                                                                          | You want each block available as an individual object. See [Cloud Storage Expanded Plugin](./cloud-storage-expanded-plugin.md)                                                                |

The chart default plugin list includes both roster plugins. Keep them unless you know your node does not need them.

`state-management-hashgraph` keeps a live copy of the network state inside the Block Node. It is in beta and is not part of any chart profile yet.

## Plugin profiles

The Helm chart ships four plugin profiles in [`charts/block-node-server/values-overrides/`](https://github.com/hiero-ledger/hiero-block-node/tree/main/charts/block-node-server/values-overrides):

|         Profile          |                                                                                             Plugins                                                                                             |                                                                                                                                                Use it for                                                                                                                                                |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `plugin-profile-lfh`     | The seven required plugins, plus `stream-subscriber`, `block-access-service`, `blocks-file-historic`, `backfill`, `roster-bootstrap-rsa`, and `roster-bootstrap-tss`. This is the chart default | A Tier 1 node that keeps its full block history on local disk ([Local Full History](../glossary.md#local-full-history-lfh))                                                                                                                                                                              |
| `plugin-profile-all`     | `plugin-profile-lfh` plus `cloud-storage-archive` and `cloud-storage-expanded`                                                                                                                  | A Tier 1 node that keeps full local history and also archives every block to cloud storage                                                                                                                                                                                                               |
| `plugin-profile-rfh`     | `backfill`, `block-verification`, `cloud-storage-archive`, `cloud-storage-expanded`, the two roster plugins, `facility-app-state`, `facility-messaging`, `health`, and `server-status`          | A node whose only job is archiving to cloud storage ([Remote Full History](./block-node-hardware-specifications.md#remote-full-history-rfh)). It receives blocks only through backfill from other Block Nodes, keeps no local block storage, takes no stream from Consensus Nodes, and serves no clients |
| `plugin-profile-minimal` | `facility-app-state`, `facility-messaging`, `block-access-service`, `health`, and `server-status`                                                                                               | Checking that the core of the Block Node starts. It cannot receive, store, or serve blocks, so it is not a working Block Node                                                                                                                                                                            |

There is no dedicated Tier 2 profile. For a Tier 2 node that keeps history on local disk, start from `plugin-profile-lfh` and remove `stream-publisher`.

Each profile needs different hardware. See [Block Node Hardware Specifications](./block-node-hardware-specifications.md).

## Selecting plugins

How you select plugins depends on how you deploy.

**Solo Provisioner:** `block node install` asks for a plugin preset. To set it on the command line, add `--plugin-preset`:

- `tier1-lfh` - the `plugin-profile-lfh` plugins
- `tier1-rfh` - the `plugin-profile-rfh` plugins
- `custom` - pick individual plugins at the prompt. On the command line, `--plugin-preset custom` requires `--plugins`
- `none` - no override, so the plugin list comes from your `--values` file or the chart default

To pass your own list, use `--plugins` with a comma-separated list. It overrides `--plugin-preset`. See [Virtual Machine Single Node Kubernetes Deployment](./solo-weaver-single-node-k8s-deployment.md).

**Helm:** apply a profile file with `-f`, for example:

```bash
helm install block-node oci://ghcr.io/hiero-ledger/hiero-block-node/block-node-server \
  --version <version> -n block-node --create-namespace -f plugin-profile-lfh.yaml
```

Download the profile file you need from [`charts/block-node-server/values-overrides/`](https://github.com/hiero-ledger/hiero-block-node/tree/main/charts/block-node-server/values-overrides) first. To use your own list, set `plugins.names` in your values file. See [Default Hiero plugins](../configuration.md#default-hiero-plugins).

**Full image:** if you run the pre-built `hiero-block-node-full` image, select plugins with `BN_PLUGINS`. See [Plugin Catalog (Full Image)](../architecture/plugins.md#plugin-catalog-full-image).

## Building your own plugin list

1. Start from the profile closest to your deployment.
2. Keep the seven required plugins if the node receives blocks from Consensus Nodes. For a Tier 2 node, replace `stream-publisher` with `backfill`.
3. Add the optional plugins for the features you need.
4. Select the list as described in [Selecting plugins](#selecting-plugins).

> **Note:** When you upgrade to Block Node 0.45.0 or later with your own plugin list, add `facility-app-state` first. Without it, the node does not start.

## Checking that your plugin set works

1. Call `serverStatus` on the Status API port: `40840` with the base chart, or `40982` when per-service ports are set (for example by the chart's `lfh-values.yaml` overrides; the plugin profiles do not change ports). The Block Node has no gRPC server reflection, so `grpcurl` needs the Block Node protos (see [Download the proto bundle](../api-quickstart.md#download-the-proto-bundle)):

   On Kubernetes, the chart's Service is internal to the cluster by default. If no load balancer exposes the port, forward it first and leave this running in a separate terminal:

   ```bash
   kubectl port-forward -n <namespace> svc/<release>-block-node-server <status-api-port>:<status-api-port>
   ```

   Then call `serverStatus`, using `localhost` as `<host>` if you forwarded the port:

   ```bash
   grpcurl -plaintext -import-path ~/bn-proto -proto block-node/api/node_service.proto \
     -d '{}' <host>:<status-api-port> org.hiero.block.api.BlockNodeService/serverStatus
   ```

   While blocks arrive, `lastAvailableBlock` advances.

2. Check the verification metrics on port `16007`. On Kubernetes, the metrics port is not exposed outside the cluster, so forward it first and leave this running in a separate terminal:

   ```bash
   kubectl port-forward -n <namespace> svc/<release>-block-node-server 16007:16007
   ```

   Then query the metrics:

   ```bash
   curl -s http://localhost:16007/metrics | grep verification_proof_total
   ```

   The `result="success"` counter for your network's proof type (for example `proof_type="tss"`) increases, and every `result="failure"` counter stays at `0`.

3. To test a node before any Consensus Node streams to it, publish test blocks with the simulator. See [Testing a Deployed Block Node Using the Simulator](./testing-a-deployed-block-node-using-the-simulator.md).

## Writing your own plugin

To build a plugin of your own, see [Adding a New Plugin](../architecture/plugins.md#adding-a-new-plugin). To load a plugin that is published to a Maven repository, see [Third-party plugins](../configuration.md#third-party-plugins-maven-resolvable).

## Related documentation

- [Block Node Overview](../block-node-overview.md): choosing a tier and a deployment profile
- [Configuration Reference](../configuration.md#default-hiero-plugins): setting the plugin list and plugin sources
- [Plugin System and Extensibility](../architecture/plugins.md): how plugins are built and loaded
- [Block Node Hardware Specifications](./block-node-hardware-specifications.md)
- [Cloud Storage Expanded Plugin](./cloud-storage-expanded-plugin.md)
- [Network Ports and Protocols](./network-ports-and-protocols.md)
