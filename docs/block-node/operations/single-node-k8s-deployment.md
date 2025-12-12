# Single Node Kubernetes Deployment

This document provides instructions for deploying the Block Node Server Helm chart in a Single-Node Kubernetes environment.
This setup is ideal for production environments on bare metal or cloud VMs.

## Prerequisites

The single requirement is a server with a supported operating system and sufficient resources to run Kubernetes and the
Block Node Server.

Suggested minimum specifications for mainnet deployments:

1. **Local Full History (LFH)**: All block history is stored locally on the server.
   - CPU: 24 cores, 48 threads (2024 or newer CPU) (PCIe 4+)
   - RAM: 256 GB
   - Disk:
     - 8 TB NVMe SSD
     - 300 TB
   - 2 x 10 Gbps Network Interface Cards (NICs)
2. **Remote Full History (RFH)**: Block history is stored remotely.
   - CPU: 24 cores, 48 threads (2024 or newer CPU) (PCIe 4+)
   - RAM: 256 GB
   - Disk: 8 TB NVMe SSD
   - 2 x 10 Gbps Network Interface Cards (NICs)

Recommendations:
- In both configurations a Linux-based operating system is recommended, such as Ubuntu 22.04 LTS or Debian 11 LTS.
- Whiles 10 Gbps NICs are suggested, we recommend higher bandwidth NICs (20 Gbps or more) for better performance and future-proofing.
- Whiles 300 TB disk space is suggested for LFH, we recommend higher storage space (500 TB) for local full history that
will maintain block history and state on disk.

Note: Servers may be acquired by bare metal providers or cloud service providers that offer dedicated instances.
LFH servers require significant storage capacity, as such these are expected to be sourced from bare metal providers.

## Server Provisioning

Once a server has been acquired, it needs to be provisioned with the necessary software components to run Kubernetes
and the Block Node Server.

Assuming a Linux based environment, a node operator has two options at this time
1. [Solo Weaver](https://github.com/hashgraph/solo-weaver) installation (recommended)
2. Custom provisioner script

### Solo Weaver

Solo weaver is a go-based tool to simplify the provisioning of Hiero network components (like the block node) in a
streamlined and automated fashion.

To utilize the Solo Weaver experience

1. Install Weaver on the server
```bash
curl -sSL https://raw.githubusercontent.com/hashgraph/solo-weaver/main/install.sh | bash
weaver --help
```

2. Run the provisioning and install flow
Follow the [Setup Block Node](https://github.com/hashgraph/solo-weaver/blob/main/docs/quickstart.md#setup-block-node) steps.

### Custom Provisioning Script

A node operator with sufficient knowledge and expertize may desire to create a script that factors in their cloud
provider and business needs when provisioning the machine.

In this case the following recipe steps are suggested as a guide when designing your script
1. Disable Linux Swap
2. Configure [Sysctl for Kubernetes](https://kubernetes.io/docs/tasks/administer-cluster/sysctl-cluster/)
3. Setup Bind Mounts
4. Setup [Kubelet](https://kubernetes.io/docs/reference/command-line-tools-reference/kubelet/) and its Systemd Service
5. Setup [Kubectl](https://kubernetes.io/docs/reference/kubectl/kubectl/)
6. Setup [Helm](https://helm.sh/)
7. Setup [K9s](https://k9scli.io/)
8. Setup [CRI-O](https://cri-o.io/) and its Systemd Service
9. Setup [Kubeadm](https://kubernetes.io/docs/reference/setup-tools/kubeadm/)
10. Initialize Cluster
11. Setup and start [Cilium](https://cilium.io/)
12. Setup [MetalLB](https://metallb.io/)
13. Check ClusterHealth

Note: The script recipe is provided as-is (with no maintenance) and may require further edits by operators depending on
their OS and version. The recipe focuses on setting up k8s, supporting helm based installation and kubectl modification
in addition to metalLB load balancing.

## Installation Steps

With the server provisioned, follow these steps to deploy the Block Node Server on a single-node Kubernetes cluster:

Note 1: Skip steps 1 through 7 if you utilize Solo Weaver as will install teh block node for you.

Note 2: Some steps are marked as intermediate, indicating they will change due to improvements.

1. **ENV variables setup**: Set some helpful environment variables for your deployment in a `.env file:

   ```bash
   NAMESPACE=<insert namepace>
   RELEASE=<insert release name>
   VERSION=<insert latest stable block node GA version>
   POD=${RELEASE}-block-node-server-0
   ```

   A sample `.env` file is provided at [.env.sample](./../../../tools-and-tests/scripts/node-operations/sample.env).

2. (Intermediate) **Automate with Task**: Use the provided [Taskfile.yml](../../../tools-and-tests/scripts/node-operations/Taskfile.yml) to
   streamline the deployment process. The Taskfile includes tasks for installing Helm charts, configuring the Block Node
   Server, and managing the Kubernetes cluster.

3. (Intermediate) **Setup `kubectl` and `helm` environments**:

   ```bash
   task load-kubectl-helm
   ```
4. **Configure Persistent Volume Creation Script**: Create Persistent Volume (PV)s and Persistent Volume Claim (PVC)s
   for Block Node Server data storage.

   Update [./values-overrides/host-paths.yaml](../../../charts/block-node-server/values-overrides/host-paths.yaml) with the appropriate namespace.

   ```bash
   kubectl apply -f ./k8s/single-node/pv-pvc.yaml -n ${NAMESPACE}
   ```
5. **Configure Helm Chart Values**: Customize the Helm chart values for your deployment.

   Update [./lfh-values.yaml](../../../charts/block-node-server/values-overrides/lfh-values.yaml) or [./values-overrides/rfh-values.yaml](../../../charts/block-node-server/values-overrides/rfh-values.yaml) with your specific configuration settings.

6. **Install Block Node Server Helm Chart**: Deploy the Block Node Server using Helm.

   ```bash
   task helm-release
   ```
7. **Verify Deployment**: Check the status of the Block Node Server deployment to ensure it is running correctly.

   ```bash
   kubectl get pods -n ${NAMESPACE}
   kubectl logs ${POD} -n ${NAMESPACE}
   ```

   Expected output should indicate that the Block Node Server is operational.

   ```bash
   # [org.hiero.block.node.app.BlockNodeApp start] Started BlockNode Server : State = RUNNING, Historic blocks =
   ```
8. **Access Block Node Server**: Connect to the Block Node Server using the configured service endpoint.

   Install `grpcurl` if not already installed:

   ```bash
   task setup-grpcurl
   ```

   Install protobuf compiler if not already installed:

   ```bash
   task setup-bn-proto
   ```

   Use `grpcurl` to interact with the Block Node Server:

   ```bash
   grpcurl -plaintext -emit-defaults -import-path block-node-protobuf-<VERSION> -proto block-node/api/node_service.proto -d '{}' <host>:40840 org.hiero.block.api.BlockNodeService/serverStatus
   ```

   Expected output should show the server status:

   ```bash
   # expected response will be, 18446744073709551615 implies -1 which is expected on a new BN
   {
     "firstAvailableBlock": "18446744073709551615",
     "lastAvailableBlock": "18446744073709551615",
     "onlyLatestState": false,
     "versionInformation": null
   }
   ```
9. **Helm Chart Upgrades**: To upgrade the Block Node Server Helm chart to a newer version, update the `VERSION`
   variable in your `.env` file and run:

   ```bash
   task helm-upgrade
   ```
10. **(Caution) Reset Block Node Server Data**: To reset the Block Node Server (clear data and install version), run:

    ```bash
    task reset-upgrade
    ```
11. **Uninstall Block Node Server**: To uninstall the Block Node Server and remove all associated resources, run:

```bash
task clear-release
```
