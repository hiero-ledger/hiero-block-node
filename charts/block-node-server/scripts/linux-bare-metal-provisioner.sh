#!/usr/bin/env bash
set -ex

OS="$(uname -s)"
OS="${OS,,}"
ARCH="$(dpkg --print-architecture)"
readonly OS ARCH

USER="$(id -un)"
GROUP="$(id -gn)"
readonly USER GROUP

readonly PROVISIONER_HOME="/opt/solo/provisioner"
readonly SANDBOX_DIR="${PROVISIONER_HOME}/sandbox"
readonly SANDBOX_BIN="${SANDBOX_DIR}/bin"
readonly SANDBOX_LOCAL_BIN="${SANDBOX_DIR}/usr/local/bin"

readonly CRIO_VERSION="1.33.4"
readonly KUBERNETES_VERSION="1.33.4"
readonly KREL_VERSION="v0.18.0"
readonly K9S_VERSION="0.50.9"
readonly HELM_VERSION="3.18.6"
readonly CILIUM_CLI_VERSION="0.18.7"
readonly CILIUM_VERSION="1.18.1"
readonly METALLB_VERSION="0.15.2"
readonly DASEL_VERSION="2.8.1"

# Update System Packages
sudo apt update && sudo apt upgrade -y

# Disable Swap (necessary for kubeadm)
# WARNING: This failed in docker container
sudo sed -i.bak 's/^\(.*\sswap\s.*\)$/#\1\n/' /etc/fstab
sudo swapoff -a

# Install iptables
sudo apt install -y iptables

# Install gpg package (if required)
sudo apt install -y gnupg2

# Install Conntrack, EBTables, SoCat, and NFTables
sudo apt install -y conntrack socat ebtables nftables
sudo apt autoremove -y

# Enable nftables service
sudo systemctl enable nftables
sudo systemctl start nftables

# Install Kernel Modules
# WARNING: This failed in docker container
sudo modprobe overlay
sudo modprobe br_netfilter
echo "overlay" | sudo tee /etc/modules-load.d/overlay.conf
echo "br_netfilter" | sudo tee /etc/modules-load.d/br_netfilter.conf

sudo rm -f /etc/sysctl.d/15-network-performance.conf || true
sudo rm -f /etc/sysctl.d/15-k8s-networking.conf || true
sudo rm -f /etc/sysctl.d/15-inotify.conf || true

# Configure System Control Settings
cat <<EOF | sudo tee /etc/sysctl.d/75-k8s-networking.conf
net.bridge.bridge-nf-call-iptables = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward = 1
net.ipv6.conf.all.forwarding = 1
EOF

cat <<EOF | sudo tee /etc/sysctl.d/75-network-performance.conf
net.core.rmem_default = 31457280
net.core.wmem_default = 31457280
net.core.rmem_max = 33554432
net.core.wmem_max = 33554432
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65535
net.core.optmem_max = 25165824
net.ipv4.tcp_synack_retries = 2
net.ipv4.tcp_rmem = 8192 65536 33554432
net.ipv4.tcp_mem = 786432 1048576 26777216
net.ipv4.udp_mem = 65536 131072 262144
net.ipv4.udp_rmem_min = 16384
net.ipv4.tcp_wmem = 8192 65536 33554432
net.ipv4.udp_wmem_min = 16384
net.ipv4.tcp_max_tw_buckets = 1440000
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_rfc1337 = 1
net.ipv4.tcp_syncookies = 1
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_keepalive_time = 300
net.ipv4.tcp_keepalive_intvl = 15
fs.file-max = 2097152
vm.swappiness = 10
vm.dirty_ratio = 60
vm.dirty_background_ratio = 2
EOF

cat <<EOF | sudo tee /etc/sysctl.d/75-inotify.conf
fs.inotify.max_user_watches = 524288
fs.inotify.max_user_instances = 512
EOF

sudo sysctl --system >/dev/null

# Configure cgroupv2 of focal
#sudo sed -i 's/^GRUB_CMDLINE_LINUX="\(.*\)"$/GRUB_CMDLINE_LINUX="systemd.unified_cgroup_hierarchy=1"/' /etc/default/grub
#sudo sed -i 's/^GRUB_CMDLINE_LINUX="\(.*\)"$/GRUB_CMDLINE_LINUX="systemd.unified_cgroup_hierarchy=1"/' /etc/default/grub
#sudo update-grub

# Setup working directories
mkdir -p /tmp/provisioner/utils
mkdir -p /tmp/provisioner/cri-o/unpack
mkdir -p /tmp/provisioner/kubernetes
mkdir -p /tmp/provisioner/cilium

# Download Components
pushd "/tmp/provisioner/utils" >/dev/null 2>&1 || true
curl -sSLo "dasel_${OS}_${ARCH}" "https://github.com/TomWright/dasel/releases/download/v${DASEL_VERSION}/dasel_${OS}_${ARCH}"
popd >/dev/null 2>&1 || true

pushd "/tmp/provisioner/cri-o" >/dev/null 2>&1 || true
curl -sSLo "cri-o.${ARCH}.v${CRIO_VERSION}.tar.gz" "https://storage.googleapis.com/cri-o/artifacts/cri-o.${ARCH}.v${CRIO_VERSION}.tar.gz"
popd >/dev/null 2>&1 || true

pushd "/tmp/provisioner/kubernetes" >/dev/null 2>&1 || true
curl -sSLo kubeadm "https://dl.k8s.io/release/v${KUBERNETES_VERSION}/bin/${OS}/${ARCH}/kubeadm"
curl -sSLo kubelet "https://dl.k8s.io/release/v${KUBERNETES_VERSION}/bin/${OS}/${ARCH}/kubelet"
curl -sSLo kubectl "https://dl.k8s.io/release/v${KUBERNETES_VERSION}/bin/${OS}/${ARCH}/kubectl"
sudo chmod +x kubeadm kubelet kubectl
curl -sSLo kubelet.service "https://raw.githubusercontent.com/kubernetes/release/${KREL_VERSION}/cmd/krel/templates/latest/kubelet/kubelet.service"
curl -sSLo 10-kubeadm.conf "https://raw.githubusercontent.com/kubernetes/release/${KREL_VERSION}/cmd/krel/templates/latest/kubeadm/10-kubeadm.conf"
curl -sSLo "k9s_${OS^}_${ARCH}.tar.gz" "https://github.com/derailed/k9s/releases/download/v${K9S_VERSION}/k9s_${OS^}_${ARCH}.tar.gz"
curl -sSLo "helm-v${HELM_VERSION}-${OS}-${ARCH}.tar.gz" "https://get.helm.sh/helm-v${HELM_VERSION}-${OS}-${ARCH}.tar.gz"
popd >/dev/null 2>&1 || true

pushd "/tmp/provisioner/cilium" >/dev/null 2>&1 || true
curl -sSLo "cilium-${OS}-${ARCH}.tar.gz" "https://github.com/cilium/cilium-cli/releases/download/v${CILIUM_CLI_VERSION}/cilium-${OS}-${ARCH}.tar.gz"
curl -sSLo "cilium-${OS}-${ARCH}.tar.gz.sha256sum" "https://github.com/cilium/cilium-cli/releases/download/v${CILIUM_CLI_VERSION}/cilium-${OS}-${ARCH}.tar.gz.sha256sum"
sha256sum -c "cilium-${OS}-${ARCH}.tar.gz.sha256sum"
popd >/dev/null 2>&1 || true

# Ensure Cilium directory exists
sudo mkdir -p /var/run/cilium

# Setup Production Provisioner Folders
sudo mkdir -p ${PROVISIONER_HOME}
sudo mkdir -p ${PROVISIONER_HOME}/bin
sudo mkdir -p ${PROVISIONER_HOME}/logs
sudo mkdir -p ${PROVISIONER_HOME}/config

# Setup Provisioner Sandbox
sudo mkdir -p ${SANDBOX_DIR}
sudo mkdir -p ${SANDBOX_DIR}/bin
sudo mkdir -p ${SANDBOX_DIR}/etc/crio/keys
sudo mkdir -p ${SANDBOX_DIR}/etc/default
sudo mkdir -p ${SANDBOX_DIR}/etc/sysconfig
sudo mkdir -p ${SANDBOX_DIR}/etc/provisioner
sudo mkdir -p ${SANDBOX_DIR}/etc/containers/registries.conf.d
sudo mkdir -p ${SANDBOX_DIR}/etc/cni/net.d
sudo mkdir -p ${SANDBOX_DIR}/etc/nri/conf.d
sudo mkdir -p ${SANDBOX_DIR}/etc/kubernetes/pki
sudo mkdir -p ${SANDBOX_DIR}/var/lib/etcd
sudo mkdir -p ${SANDBOX_DIR}/var/lib/containers/storage
sudo mkdir -p ${SANDBOX_DIR}/var/lib/kubelet
sudo mkdir -p ${SANDBOX_DIR}/var/lib/crio
sudo mkdir -p ${SANDBOX_DIR}/var/run/cilium
sudo mkdir -p ${SANDBOX_DIR}/var/run/nri
sudo mkdir -p ${SANDBOX_DIR}/var/run/containers/storage
sudo mkdir -p ${SANDBOX_DIR}/var/run/crio/exits
sudo mkdir -p ${SANDBOX_DIR}/var/logs/crio/pods
sudo mkdir -p ${SANDBOX_DIR}/run/runc
sudo mkdir -p ${SANDBOX_DIR}/usr/libexec/crio
sudo mkdir -p ${SANDBOX_DIR}/usr/lib/systemd/system/kubelet.service.d
sudo mkdir -p ${SANDBOX_DIR}/usr/local/bin
sudo mkdir -p ${SANDBOX_DIR}/usr/local/share/man
sudo mkdir -p ${SANDBOX_DIR}/usr/local/share/oci-umount/oci-umount.d
sudo mkdir -p ${SANDBOX_DIR}/usr/local/share/bash-completion/completions
sudo mkdir -p ${SANDBOX_DIR}/usr/local/share/fish/completions
sudo mkdir -p ${SANDBOX_DIR}/usr/local/share/zsh/site-functions
sudo mkdir -p ${SANDBOX_DIR}/opt/cni/bin
sudo mkdir -p ${SANDBOX_DIR}/opt/nri/plugins

# Setup Ownership and Permissions
sudo chown -R "${USER}:${GROUP}" "${PROVISIONER_HOME}"
sudo chown -R "root:root" "${SANDBOX_DIR}"

# Setup Bind Mounts
sudo mkdir -p /etc/kubernetes /var/lib/kubelet

if ! grep -q "/etc/kubernetes" /etc/fstab; then
  echo "${SANDBOX_DIR}/etc/kubernetes /etc/kubernetes none bind,nofail 0 0" | sudo tee -a /etc/fstab >/dev/null
fi

if ! grep -q "/var/lib/kubelet" /etc/fstab; then
  echo "${SANDBOX_DIR}/var/lib/kubelet /var/lib/kubelet none bind,nofail 0 0" | sudo tee -a /etc/fstab >/dev/null
fi

if ! grep -q "/var/run/cilium" /etc/fstab; then
  echo "${SANDBOX_DIR}/var/run/cilium /var/run/cilium none bind,nofail 0 0" | sudo tee -a /etc/fstab >/dev/null
fi

sudo systemctl daemon-reload
sudo mount /etc/kubernetes
sudo mount /var/lib/kubelet
sudo mount /var/run/cilium

# Install dasel
pushd "/tmp/provisioner/utils" >/dev/null 2>&1 || true
sudo install -m 755 "dasel_${OS}_${ARCH}" "${SANDBOX_BIN}/dasel"
popd >/dev/null 2>&1 || true

# Install CRI-O
sudo tar -C "/tmp/provisioner/cri-o/unpack" -zxvf "/tmp/provisioner/cri-o/cri-o.${ARCH}.v${CRIO_VERSION}.tar.gz"
pushd "/tmp/provisioner/cri-o/unpack/cri-o" >/dev/null 2>&1 || true
DESTDIR="${SANDBOX_DIR}" SYSTEMDDIR="/usr/lib/systemd/system" sudo -E "$(command -v bash)" ./install
popd >/dev/null 2>&1 || true

# Symlink CRI-O /etc/containers/registries.conf.d
sudo ln -sf "${SANDBOX_DIR}/etc/containers" /etc/containers

# Install Kubernetes
sudo install -m 755 "/tmp/provisioner/kubernetes/kubeadm" "${SANDBOX_BIN}/kubeadm"
sudo install -m 755 "/tmp/provisioner/kubernetes/kubelet" "${SANDBOX_BIN}/kubelet"
sudo install -m 755 "/tmp/provisioner/kubernetes/kubectl" "${SANDBOX_BIN}/kubectl"

sudo ln -sf "${SANDBOX_BIN}/kubeadm" /usr/local/bin/kubeadm
sudo ln -sf "${SANDBOX_BIN}/kubelet" /usr/local/bin/kubelet
sudo ln -sf "${SANDBOX_BIN}/kubectl" /usr/local/bin/kubectl
sudo ln -sf "${SANDBOX_BIN}/k9s" /usr/local/bin/k9s
sudo ln -sf "${SANDBOX_BIN}/helm" /usr/local/bin/helm
sudo ln -sf "${SANDBOX_BIN}/cilium" /usr/local/bin/cilium

sudo mkdir -p ${SANDBOX_DIR}/usr/lib/systemd/system/kubelet.service.d
sudo cp "/tmp/provisioner/kubernetes/kubelet.service" "${SANDBOX_DIR}/usr/lib/systemd/system/kubelet.service"
sudo cp "/tmp/provisioner/kubernetes/10-kubeadm.conf" "${SANDBOX_DIR}/usr/lib/systemd/system/kubelet.service.d/10-kubeadm.conf"

# Change kubelet service file to use the sandbox bin directory
sudo sed -i "s|/usr/bin/kubelet|${SANDBOX_BIN}/kubelet|" "${SANDBOX_DIR}/usr/lib/systemd/system/kubelet.service"
sudo sed -i "s|/usr/bin/kubelet|${SANDBOX_BIN}/kubelet|" "${SANDBOX_DIR}/usr/lib/systemd/system/kubelet.service.d/10-kubeadm.conf"

# Change CRI-O service file to use the sandbox bin directory
sudo sed -i "s|/usr/local/bin/crio|${SANDBOX_LOCAL_BIN}/crio|" "${SANDBOX_DIR}/usr/lib/systemd/system/crio.service"
sudo sed -i "s|/etc/sysconfig/crio|${SANDBOX_DIR}/etc/default/crio|" "${SANDBOX_DIR}/usr/lib/systemd/system/crio.service"

cat <<EOF | sudo tee "${SANDBOX_DIR}/etc/default/crio" >/dev/null
# /etc/default/crio

# use "--enable-metrics" and "--metrics-port value"
#CRIO_METRICS_OPTIONS="--enable-metrics"

#CRIO_NETWORK_OPTIONS=
#CRIO_STORAGE_OPTIONS=

# CRI-O configuration directory
CRIO_CONFIG_OPTIONS="--config-dir=${SANDBOX_DIR}/etc/crio/crio.conf.d"
EOF

# Update CRI-O configuration
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v runc '.crio.runtime.default_runtime'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/etc/crio/keys" '.crio.runtime.decryption_keys_path'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/var/run/crio/exits" '.crio.runtime.container_exits_dir'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/var/run/crio" '.crio.runtime.container_attach_socket_dir'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/var/run" '.crio.runtime.namespaces_dir'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_LOCAL_BIN}/pinns" '.crio.runtime.pinns_path'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/run/runc" '.crio.runtime.runtimes.runc.runtime_root'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/var/run/crio/crio.sock" '.crio.api.listen'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/var/run/crio/crio.sock" '.crio.api.listen'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/var/lib/containers/storage" '.crio.root'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/var/run/containers/storage" '.crio.runroot'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/var/run/crio/version" '.crio.version_file'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/var/logs/crio/pods" '.crio.log_dir'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/var/lib/crio/clean.shutdown" '.crio.clean_shutdown_file'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/etc/cni/net.d/" '.crio.network.network_dir'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/opt/cni/bin" -s 'crio.network.plugin_dirs.[]'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/opt/nri/plugins" '.crio.nri.nri_plugin_dir'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/etc/nri/conf.d" '.crio.nri.nri_plugin_config_dir'
sudo ${SANDBOX_BIN}/dasel put -w toml -r toml -f "${SANDBOX_DIR}/etc/crio/crio.conf.d/10-crio.conf" -v "${SANDBOX_DIR}/var/run/nri/nri.sock" '.crio.nri.nri_listen'

# Install K9s
sudo tar -C ${SANDBOX_BIN} -zxvf "/tmp/provisioner/kubernetes/k9s_${OS^}_${ARCH}.tar.gz" k9s

# Install Helm
sudo tar -C ${SANDBOX_BIN} --strip-components=1 -zxvf "/tmp/provisioner/kubernetes/helm-v${HELM_VERSION}-${OS}-${ARCH}.tar.gz" "${OS}-${ARCH}/helm"

# Install Cilium
sudo tar -C ${SANDBOX_BIN} -zxvf "/tmp/provisioner/cilium/cilium-${OS}-${ARCH}.tar.gz"

# Setup Systemd Service SymLinks
sudo ln -sf ${SANDBOX_DIR}/usr/lib/systemd/system/kubelet.service /usr/lib/systemd/system/kubelet.service
sudo ln -sf ${SANDBOX_DIR}/usr/lib/systemd/system/kubelet.service.d /usr/lib/systemd/system/kubelet.service.d

# Setup CRI-O Service SymLinks
sudo ln -sf ${SANDBOX_DIR}/usr/lib/systemd/system/crio.service /usr/lib/systemd/system/crio.service

# Enable and Start Services
sudo systemctl daemon-reload
sudo systemctl enable crio kubelet
sudo systemctl start crio kubelet

# Torch prior KubeADM Configuration
sudo ${SANDBOX_BIN}/kubeadm reset --force || true
sudo rm -rf ${SANDBOX_DIR}/etc/kubernetes/* ${SANDBOX_DIR}/etc/cni/net.d/* ${SANDBOX_DIR}/var/lib/etcd/* || true

# Setup KubeADM Configuration
kube_bootstrap_token="$(${SANDBOX_BIN}/kubeadm token generate)"

# WARNING: ip not found in docker container
machine_ip="$(ip route get 1 | head -1 | sed 's/^.*src \(.*\)$/\1/' | awk '{print $1}')"

cat <<EOF | sudo tee ${SANDBOX_DIR}/etc/provisioner/kubeadm-init.yaml >/dev/null
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
bootstrapTokens:
  - groups:
    - system:bootstrappers:kubeadm:default-node-token
    token: ${kube_bootstrap_token}
    ttl: 720h0m0s
    usages:
      - signing
      - authentication
localAPIEndpoint:
  advertiseAddress: ${machine_ip}
  bindPort: 6443
nodeRegistration:
  criSocket: unix://${SANDBOX_DIR}/var/run/crio/crio.sock
  imagePullPolicy: IfNotPresent
  imagePullSerial: true
  name: $(hostname)
  taints:
    - key: "node.cilium.io/agent-not-ready"
      value: "true"
      effect: "NoExecute"
  kubeletExtraArgs:
    - name: node-ip
      value: ${machine_ip}
skipPhases:
  - addon/kube-proxy
timeouts:
  controlPlaneComponentHealthCheck: 4m0s
  discovery: 5m0s
  etcdAPICall: 2m0s
  kubeletHealthCheck: 4m0s
  kubernetesAPICall: 1m0s
  tlsBootstrap: 5m0s
  upgradeManifests: 5m0s
---
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
controlPlaneEndpoint: "${machine_ip}:6443"
certificatesDir: ${SANDBOX_DIR}/etc/kubernetes/pki
caCertificateValidityPeriod: 87600h0m0s
certificateValidityPeriod: 8760h0m0s
encryptionAlgorithm: RSA-2048
clusterName: k8s.main.gcp
etcd:
  local:
    dataDir: ${SANDBOX_DIR}/var/lib/etcd
imageRepository: registry.k8s.io
kubernetesVersion: ${KUBERNETES_VERSION}
networking:
  dnsDomain: cluster.local
  serviceSubnet: 10.0.0.0/14
  podSubnet: 10.4.0.0/14
controllerManager:
  extraArgs:
    - name: node-cidr-mask-size-ipv4
      value: "24"
EOF

# Stop on failure past this point
set -eo pipefail

# Initialize Kubernetes Cluster
# WARNING: This failed in docker container, probably because we couldn't disable swap earlier
sudo ${SANDBOX_BIN}/kubeadm init --upload-certs --config ${SANDBOX_DIR}/etc/provisioner/kubeadm-init.yaml
mkdir -p "${HOME}/.kube"
sudo cp -f /etc/kubernetes/admin.conf "${HOME}/.kube/config"
sudo chown "${USER}:${GROUP}" "${HOME}/.kube/config"

# Configure Cilium
cat <<EOF | sudo tee ${SANDBOX_DIR}/etc/provisioner/cilium-config.yaml >/dev/null
# StepSecurity Required Features
extraArgs:
  - --tofqdns-dns-reject-response-code=nameError

# Hubble Support
hubble:
  relay:
    enabled: true
  ui:
    enabled: false

# KubeProxy Replacement Config
kubeProxyReplacement: true
k8sServiceHost: ${machine_ip}
k8sServicePort: 6443

# IP Version Support
ipam:
  mode: "kubernetes"
k8s:
  requireIPv4PodCIDR: true
  requireIPv6PodCIDR: false
ipv4:
  enabled: true
ipv6:
  enabled: false

# Routing Configuration
routingMode: native
autoDirectNodeRoutes: true
#ipv4NativeRoutingCIDR: 10.128.0.0/20

# Load Balancer Configuration
loadBalancer:
  mode: dsr
  dsrDispatch: opt
  algorithm: maglev
  acceleration: "best-effort"
  l7:
    backend: disabled

nodePort:
  enabled: true

hostPort:
  enabled: true

# BPF & IP Masquerading Support
ipMasqAgent:
  enabled: true
  config:
    nonMasqueradeCIDRs: []
bpf:
  masquerade: true
  hostLegacyRouting: false
  lbExternalClusterIP: true
  preallocateMaps: true

# Envoy DaemonSet Support
envoy:
  enabled: false

# BGP Control Plane
bgpControlPlane:
  enabled: false

# L2 Announcements
l2announcements:
  enabled: false
k8sClientRateLimit:
  qps: 100
  burst: 150

# CNI Configuration
cni:
  binPath: ${SANDBOX_DIR}/opt/cni/bin
  confPath: ${SANDBOX_DIR}/etc/cni/net.d

# DaemonSet Configuration
daemon:
  runPath: ${SANDBOX_DIR}/var/run/cilium

EOF

# Install Cilium CNI
${SANDBOX_BIN}/cilium install --version "${CILIUM_VERSION}" --values ${SANDBOX_DIR}/etc/provisioner/cilium-config.yaml

# Restart Container and Kubelet (fix for cilium CNI not initializing - CNI not ready error)
sudo sysctl --system >/dev/null
sudo systemctl restart kubelet crio

${SANDBOX_BIN}/cilium status --wait

# Install MetalLB
${SANDBOX_BIN}/helm repo add metallb https://metallb.github.io/metallb
${SANDBOX_BIN}/helm install metallb metallb/metallb --version ${METALLB_VERSION} \
  --set speaker.frr.enabled=false \
  --namespace metallb-system --create-namespace --atomic --wait

# Wait for MetalLB Pods to be Running
sleep 60

# Deploy MetalLB Configuration
cat <<EOF | ${SANDBOX_BIN}/kubectl apply -f -
---
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: private-address-pool
  namespace: metallb-system
spec:
  addresses:
    - 192.168.99.0/24
---
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: public-address-pool
  namespace: metallb-system
spec:
  addresses:
    - ${machine_ip}/32
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: primary-l2-advertisement
  namespace: metallb-system
spec:
  ipAddressPools:
  - private-address-pool
  - public-address-pool
EOF


