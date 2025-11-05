# BlockStream Simulator Helm Chart

Installs the BlockStream Simulator on a Kubernetes cluster.

## Prerequisites

- Helm 3+
- Kubernetes 1.29+

Set Release name and version to install.

```bash
export RELEASE="simulator-release"
export VERSION="0.22.0"
```

## Template

To generate the K8 manifest files without installing the chart, you need to clone this repo and navigate to `/charts` folder.

```bash
helm template --name-template simulator-release blockstream-simulator/ --dry-run --output-dir out
```

## Install using a published chart

To pull the packaged chart from the public repo:

```bash
helm pull oci://ghcr.io/hiero-ledger/hiero-block-node/charts/blockstream-simulator-chart --version "${VERSION}"
```

## Install using a local chart cloned from the repo

```bash
git clone git@github.com:hiero-ledger/hiero-block-node.git
cd hiero-block-node
helm install "${RELEASE}" charts/blockstream-simulator -f <path-to-custom-values-file>
```

## Configure

The chart comes with a set of default `Values.yaml` file that sets it in ProducerMode and looks for GRPC BN Server with
the following service name `my-bn-block-node-helm-chart` within the same cluster, ready to start streaming blocks.
However, is also possible to use the simulator in ConsumerMode, to do so, you need to set the following values in the
`values.yaml` file:

```yaml
simulator:
  config:
    BLOCK_STREAM_SIMULATOR_MODE: "CONSUMER"
    # Further custom configuration...
    <ENV_VARIABLE>: "<Value>"
```

and if your BlockNode RELEASE name is different from the assumed: `blkNod`, you need to set the following value in the
`values.yaml` file:

```yaml
simulator:
  config:
    GRPC_SERVER_ADDRESS: "<BlockNode_HostName(K8 ServiceName)>"
```

Refer to the Simulator configuration for further configuration options, any configuration that can be changed via ENV
variables should be set in the `values.yaml` file under the `config' section in a directory for config and a separate
one for secrets (if needed).
