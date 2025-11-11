# Single Node Kubernetes Deployment

This document provides instructions for deploying a single-node Kubernetes cluster using the Block Node Server Helm chart.
This setup is ideal for production environments on bare m=metal or cloud VMs.

## Prerequisites
 The single requirement is a server with a supported operating system and sufficient resources to run Kubernetes and the Block Node Server.

Suggested minimum specifications:

1. Local Full History (LFH)
    - CPU: 24 cores, 48 threads (2024 or newer CPU) (PCIe 4+)
    - RAM: 256 GB
    - Disk:
      - 8 TB NVMe SSD
      - 500 TB
    - 2 x 10 Gbps Network Interface Cards (NICs)
2. Remote Full History (RFH)
    - CPU: 24 cores, 48 threads (2024 or newer CPU) (PCIe 4+)
    - RAM: 256 GB
    - Disk: 8 TB NVMe SSD
    - 2 x 10 Gbps Network Interface Cards (NICs)

In both configurations a Linux-based operating system is recommended, such as Ubuntu 22.04 LTS.

Note: Servers may be acquired by bare metal providers or cloud service providers that offer dedicated instances.
LFH servers require significant storage capacity, as such these are expected to be sourced from bare metal providers.

## Server Provisioning

Once a server has been acquired, it needs to be provisioned with the necessary software components to run Kubernetes and the Block Node Server.

Assuming an ubuntu environment, [./scripts/ubuntu-production.sh](./scripts/ubuntu-production.sh) serves as a provisioner script to automate the installation of required dependencies.:


## Installation Steps

With the server provisioned, follow these steps to deploy the Block Node Server on a single-node Kubernetes cluster:

Note: Some steps are marked as intermediate, indicating they will change due to improvements

1. **ENV variables**: Set some helpful environment variables for your deployment in a `.env file:

   ```bash
    NAMESPACE=<insert namepace>
    RELEASE=<insert release name>
    VERSION=<insert latest stable block node GA version>
    POD=${RELEASE}-block-node-server-0
   ```

2. (Intermediate) **Automate with Task**: Use the provided [./scripts/Taskfile.yml](./scripts/Taskfile.yml) to streamline the deployment process.
  The Taskfile includes tasks for installing Helm charts, configuring the Block Node Server, and managing the Kubernetes cluster.


3. (Intermediate) **Setup `kubectl` and `helm` environments**:

    ```bash
    task load-kubectl-helm
    ```

4. **Configure Persistent Volume Creation Script**: Create Persistent Volume (PV)s and Persistent Volume Claim (PVC)s for Block Node Server data storage.

    Update [./values-overrides/host-paths.yaml](./values-overrides/host-paths.yaml) with the appropriate namespace.
   ```bash
   kubectl apply -f ./k8s/single-node/pv-pvc.yaml -n ${NAMESPACE}
   ```

5. **Configure Helm Chart Values**: Customize the Helm chart values for your deployment.

   Update [./values-overrides/lfh-values.yaml](./values-overrides/lfh.yaml) or [./values-overrides/rfh-values.yaml](./values-overrides/rfh.yaml) with your specific configuration settings.

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
   curl -L https://github.com/fullstorydev/grpcurl/releases/download/v1.8.7/grpcurl_1.8.7_linux_x86_64.tar.gz -o grpcurl.tar.gz
   sudo tar -xzf grpcurl.tar.gz -C /usr/local/bin grpcurl
   rm grpcurl.tar.gz
   ```

    Install protobuf compiler if not already installed:
    ```bash
    VERSION="latest stable version here"
    BASE_URL="https://github.com/hiero-ledger/hiero-block-node/releases/download/v${VERSION}"
    ARCHIVE="block-node-protobuf-${VERSION}.tgz"
    TGZ="block-node-protobuf-${VERSION}.tgz"
    DEST_DIR="${HOME}/block-node-protobuf-${VERSION}"

    curl -L "${BASE_URL}/${ARCHIVE}" -o "${ARCHIVE}"

    # Ensure unzip is available
    command -v unzip >/dev/null 2>&1 || sudo apt-get install -y unzip

    mkdir -p "${DEST_DIR}"
    tar -xzf "${ARCHIVE}" -C "${DEST_DIR}"
    # This is needed as tar doesnt support to overwrite the existing tar
    tar -xzf "${DEST_DIR}"/block-node-protobuf-${VERSION}.tgz -C "${DEST_DIR}"
    rm "${ARCHIVE}" "${DEST_DIR}/${TGZ}"
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

9. **Helm Chart Upgrades**: To upgrade the Block Node Server Helm chart to a newer version, update the `VERSION` variable in your `.env` file and run:
   ```bash
   task helm-upgrade
   ```
