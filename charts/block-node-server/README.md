# Helm Chart

Installs the Hiero Block Node on a Kubernetes cluster.

## Prerequisites

- Helm 3+
- Kubernetes 1.29+

For Development and Test environment is recommended to use minikube with the following command (if want to deploy the kube-prometheus-stack for metrics visualization):

```bash
minikube delete && minikube start --kubernetes-version=v1.23.0 --memory=8g --bootstrapper=kubeadm --extra-config=kubelet.authentication-token-webhook=true --extra-config=kubelet.authorization-mode=Webhook --extra-config=scheduler.bind-address=0.0.0.0 --extra-config=controller-manager.bind-address=0.0.0.0
```

Set environment variables that will be used for the remainder of the document:
Replacing the values with the appropriate values for your environment: bn-release is short for block-node-release, but you can name you release as you wish. And use the version that you want to install.

```bash
export RELEASE="bn-release"
export VERSION="0.23.0-SNAPSHOT"
```

## Template

To generate the K8 manifest files without installing the chart, you need to clone this repo and navigate to `/charts` folder.

```bash
helm template --name-template bn-release block-node-server/ --dry-run --output-dir out
```

## Install using a published chart

To pull the packaged chart from the public repo:

```bash
helm pull oci://ghcr.io/hiero-ledger/hiero-block-node/block-node-helm-chart --version "${VERSION}"
```

To install the chart with default values:

```bash
helm install "${RELEASE}" block-node-server/charts/block-node-server-$VERSION.tgz
```

To install the chart with custom values:

```bash
helm install "${RELEASE}" block-node-server/charts/block-node-server-$VERSION.tgz -f <path-to-custom-values-file>
```

*Note:* If using the chart directly after cloning the github repo, there is no need to add the repo. and install can be directly.
Assuming you are at the root folder of the repo.

```bash
helm dependency build charts/block-node-server

helm install "${RELEASE}" charts/block-node-server -f <path-to-custom-values-file>
```

## Configure

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

## Using

Follow the `NOTES` instructions after installing the chart to perform `port-forward` to the Hiero Block Node and be able to use it.

## Uninstall

To uninstall the chart:

```bash
helm uninstall "${RELEASE}"
```

## Upgrade

To upgrade the chart:

```bash
# update the helm repo
helm repo update
# save current user supplied values
helm get values "${RELEASE}" > user-values.yaml
# upgrade the chart
helm upgrade "${RELEASE}" hiero-block-node/block-node-server -f user-values.yaml
```
