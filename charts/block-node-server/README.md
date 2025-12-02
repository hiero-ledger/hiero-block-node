# Helm Chart

Installs the Hiero Block Node on a Kubernetes cluster.

## Prerequisites

* [Helm 3+](https://helm.sh/docs/intro/install/)
* [Kubernetes 1.29+](https://kubernetes.io/releases/)

## Development and Test Environment Setup

For development and test environments, it is recommended to use [minikube](https://minikube.sigs.k8s.io/docs/start/).

### Basic minikube setup

```bash
minikube delete && minikube start --kubernetes-version=v1.29.0
```

### minikube setup with monitoring support

If you want to deploy the `kube-prometheus-stack` for metrics visualization:

```bash
minikube delete && minikube start \
  --kubernetes-version=v1.29.0 \
  --memory=8g \
  --bootstrapper=kubeadm \
  --extra-config=kubelet.authentication-token-webhook=true \
  --extra-config=kubelet.authorization-mode=Webhook \
  --extra-config=scheduler.bind-address=0.0.0.0 \
  --extra-config=controller-manager.bind-address=0.0.0.0
```

## Configuration

Set environment variables that will be used throughout this guide. Replace the values with appropriate values for your environment:

```bash
export RELEASE="bn-release"  # bn-release is short for block-node-release
export VERSION="0.25.0-SNAPSHOT"
```

## Installation Options

### Option 1: Template (without installing)

To generate Kubernetes manifest files without installing the chart:

1. Clone this repository
2. Navigate to the `/charts` folder
3. Run the following command:

```bash
helm template --name-template bn-release block-node-server/ --dry-run --output-dir out
```

### Option 2: Install using a published chart

#### Pull the packaged chart

```bash
helm pull oci://ghcr.io/hiero-ledger/hiero-block-node/block-node-server --version "${VERSION}"
```

#### Install with default values

```bash
helm install "${RELEASE}" block-node-server-$VERSION.tgz
```

#### Install with custom values

```bash
helm install "${RELEASE}" block-node-server-$VERSION.tgz -f <path-to-custom-values-file>
```

### Option 3: Install directly from cloned repository

*Note:* There is no need to add the repo, If using the chart directly after cloning the GitHub repository. Assuming you are at the root folder of the repository:

1. Build dependencies:

```bash
helm dependency build charts/block-node-server
```

2. Install the chart:

```bash
helm install "${RELEASE}" charts/block-node-server -f <path-to-custom-values-file>
```

## Custom Configuration

There are several ways to configure the Hiero Block Node. The following is the most common and recommended way to configure the Hiero Block Node.

1. Create an override values file, `values.yaml`:
2. Add the necessary environment configuration variables to the following section:

```yaml
blockNode:
  config:
    # Add any additional env configuration here
    # key: value
    BLOCKNODE_STORAGE_ROOT_PATH: "/opt/hiero/block-node/storage"

```

3. Secrets should be set at the following structure:

   ```yaml
   blockNode:
     secret:
       PRIVATE_KEY: "<Secret>"
   ```

or passed at the command line:

```bash
helm install "${RELEASE}" hiero-block-node/block-node-server --set blockNode.secret.PRIVATE_KEY="<Secret>"
```

### Enable Prometheus + Grafana Stack

By default the stack includes a chart dependency that includes a prometheus + grafana + node-exporter stack, also adds 3 provisioned dashboards
- **Hiero Block Node Dashboard:** to monitor the Hiero Block Node metrics, this are the server application specific metrics.
- **Node Exporter Full:** to monitor the node-exporter metrics, system metrics at the K8 cluster/node level.
- **Kubernetes View Pods:** to monitor the kubernetes pods metrics, system metrics at the container level.

If you prefer to use your own prometheus+grafana stack, you can disable the stack by setting the following values:

```yaml
kubepromstack:
  enabled: false
```

### Enable Loki + Promtail

By default the stack includes chart dependencies for a loki + promtail stack, to collect logs from the Hiero Block Node and the K8 cluster.
If you prefer to use your own loki+promtail stack, you can disable the stack by setting the following values:

```yaml
loki:
  enabled: false

promtail:
  enabled: false
```

## Post-Installation

Follow the `NOTES` instructions after installing the chart to perform `port-forward` to the Hiero Block Node and be able to use it.

## Upgrade

### Upgrade using a published chart (OCI registry)

To upgrade the chart to a new version from the OCI registry:

1. Set the new version:

```bash
export VERSION="0.25.0-SNAPSHOT"
```

2. Save your current configuration:

```bash
helm get values "${RELEASE}" > user-values.yaml
```

3. Upgrade the release directly from OCI registry:

```bash
helm upgrade "${RELEASE}" oci://ghcr.io/hiero-ledger/hiero-block-node/block-node-server --version "${VERSION}" -f user-values.yaml
```

### Upgrade using a local chart

To upgrade the chart from a local `.tgz` file or cloned repository:

1. If using a downloaded `.tgz` file, ensure you have the new version locally. If using a cloned repository, pull the latest changes:

```bash
git pull origin main
helm dependency build charts/block-node-server
```

2. Save your current configuration:

```bash
helm get values "${RELEASE}" > user-values.yaml
```

3. Upgrade the release from local chart:

```bash
# If using a .tgz file:
helm upgrade "${RELEASE}" block-node-server-$VERSION.tgz -f user-values.yaml
# If using cloned repository:
helm upgrade "${RELEASE}" charts/block-node-server -f user-values.yaml
```

## Uninstall

To uninstall the chart:

```bash
helm uninstall "${RELEASE}"
```

This will remove all Kubernetes resources associated with the release.

## Troubleshooting

This section covers common issues you may encounter when working with the Hiero Block Node Helm chart.

### Chart Dependencies Missing

**When running:**

```bash
helm template --name-template bn-release block-node-server/ --dry-run --output-dir out
```

**Error:**

```
Error: An error occurred while checking for chart dependencies. You may need to run `helm dependency build` to fetch missing dependencies: found in Chart.yaml, but missing in charts/ directory: kube-prometheus-stack, loki, promtail
Error: Chart.yaml file is missing
```

**Solution:**

Before running the template command, build the chart dependencies:

```bash
helm dependency build charts/block-node-server
```

This will download all required chart dependencies to the `charts/` directory.

---

### Chart Version Not Found in Registry

**When running:**

```bash
helm pull oci://ghcr.io/hiero-ledger/hiero-block-node/block-node-helm-chart --version "${VERSION}"
```

**Error:**

```
Error: failed to perform "fetchReference" on source: ghcr.io/hiero-ledger/hiero-block-node/block-node-helm-chart:0.22.8-SNAPSHOT: not found
```

or

```
Error: failed to perform "fetchReference" on source: ghcr.io/hiero-ledger/hiero-block-node/block-node-helm-chart:0.21.2: not found
```

**Cause:**

The specified chart version or name does not exist in the registry, or the OCI registry path has changed.

**Solution:**

1. Use the correct OCI registry path:

```bash
helm pull oci://ghcr.io/hiero-ledger/hiero-block-node/block-node-server --version "${VERSION}"
```

2. Verify the chart version exists by checking the [releases page](https://github.com/hiero-ledger/hiero-block-node/releases)

3. Confirm your `VERSION` environment variable is set to a valid release:

```bash
echo $VERSION
```
