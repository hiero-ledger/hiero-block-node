# Preparing Your Block Node for WRB Cutover

This guide walks a Tier 1 Block Node operator through the steps required to prepare a deployed Block Node for the WRB (Wrapped Record Block) streaming cutover, including pre-upgrade checks that apply to every upgrade and the additional steps specific to the WRB cutover release.

It assumes the Block Node is installed using the Solo Provisioner, either on a bare-metal server (see [Bare Metal Single Node Kubernetes Deployment](./single-node-k8s-deployment.md)) or a GCP VM (see [Virtual Machine Single Node Kubernetes Deployment](./solo-weaver-single-node-k8s-deployment.md)).

The steps are organized in two sections. The **common pre-upgrade checks** apply before every upgrade, regardless of release. The **release-specific checks** contain the additional steps the WRB cutover requires; future network releases will appear as additional subsections without changing the common section.

---

## Common pre-upgrade checks

Complete all steps in this section before every upgrade, regardless of release.

### Confirm Block Node health

Before upgrading, confirm the Block Node is healthy. Do not upgrade an unhealthy node — an in-progress failure becomes a stuck rollout.

1. List all pods and confirm the Block Node pod is `Running` with all containers ready:

   ```bash
   kubectl get pods -A
   kubectl -n block-node get pods,sts,svc
   ```

   - **Expected:** the `block-node-block-node-server-0` pod shows `1/1` ready and `Running`.
   - **If the pod shows `CrashLoopBackOff`, `ImagePullBackOff`, or `0/1` ready:** investigate with `kubectl -n block-node logs <BN_POD>` and `kubectl -n block-node describe pod <BN_POD>` before proceeding.
2. Record the current block range so you can confirm it does not regress after the upgrade. Retrieve the pod name first:

   ```bash
   BN_POD=$(kubectl -n block-node get pod \
     -l app.kubernetes.io/name=block-node \
     -o jsonpath='{.items[0].metadata.name}')
   ```

   Then call `serverStatus`:

   ```bash
   grpcurl -plaintext -emit-defaults \
     -import-path ~/bn-proto \
     -proto block-node/api/node_service.proto \
     -d '{}' \
     "$BLOCK_NODE_HOST:40840" \
     org.hiero.block.api.BlockNodeService/serverStatus
   ```

   <!-- src: hiero-block-node@fa8afa83:block-node/app-config/src/main/java/org/hiero/block/node/app/config/ServerConfig.java#L38 (SERVER_PORT default 40840) -->

   Record `firstAvailableBlock` and `lastAvailableBlock`. Both should be sensible block numbers — not `18446744073709551615` (the sentinel for "no blocks yet") — if the Block Node has been ingesting. For instructions on downloading the protobuf bundle into `~/bn-proto`, see [Connecting a Mirror Node to a Block Node](./connecting-a-mirror-node-to-a-block-node.md).

3. Confirm Alloy telemetry is shipping (if configured):

   ```bash
   kubectl -n grafana-alloy get pods
   ```

   All Alloy pods should be `Running`. A non-running Alloy pod is not a blocker for the upgrade but should be investigated afterwards.

### Validate hardware readiness

Run the Solo Provisioner hardware preflight check before the upgrade. This validates CPU core count, RAM, disk, OS version, and network-connectivity requirements for the target profile.

<!-- src: solo-weaver@222c3f47:internal/workflows/preflight.go (CheckHostProfileStep, CheckCPUStep, CheckMemoryStep, CheckStorageStep, CheckOSStep) -->

```bash
sudo solo-provisioner block node check --profile=mainnet
```

- **Expected:** each step shows a green checkmark and "success".
- **If a step fails:** it reports the specific requirement not met. For hardware minimums, see [Block Node Hardware Specifications](./block-node-hardware-specifications.md). The preflight counts physical CPU cores, not vCPUs.

<!-- src: solo-weaver@222c3f47:pkg/hardware/requirements.go (MinCpuCores per profile) -->
<!-- src: solo-weaver@222c3f47:pkg/hardware/host_profile.go (GetCPUCores returns ghw.CPU().TotalCores) -->

> Note: The `--profile=mainnet` flag is required. Omitting it returns `profile flag is required`.

### Validate required directories and free space

The Block Node uses five persistent volumes. Confirm each is mounted and has adequate free space before upgrading.

<!-- src: hiero-block-node@fa8afa83:charts/block-node-server/values.yaml#L176-L219 (persistence block) -->

|     Volume     |                                                  Default mount path                                                  |   Minimum free space    |                Purpose                 |
|----------------|----------------------------------------------------------------------------------------------------------------------|-------------------------|----------------------------------------|
| `live`         | `blockNode.persistence.live.mountPath` (default `/opt/hiero/block-node/data/live`)                                   | 20% of provisioned size | Recent block stream and live state     |
| `archive`      | `blockNode.persistence.archive.mountPath` (default `/opt/hiero/block-node/data/historic`)                            | 20% of provisioned size | Compressed historic block archive      |
| `verification` | `blockNode.persistence.verification.mountPath` (default `/opt/hiero/block-node/verification`)                        | 500 MB                  | Block hash state and verification data |
| `logging`      | `blockNode.persistence.logging.mountPath` (default `/opt/hiero/block-node/logs`)                                     | 1 GB                    | Application logs                       |
| `plugins`      | `blockNode.persistence.plugins` (not mounted by default; present only when additional plugins are explicitly loaded) | —                       | Plugin JARs                            |

> Note: The exact mount paths depend on your Helm values. Run `helm -n block-node get values <release-name>` to inspect your installation's overrides.

Confirm all volumes are mounted and visible inside the pod:

```bash
kubectl -n block-node exec $BN_POD -c block-node-server -- df -h \
  /opt/hiero/block-node/data/live \
  /opt/hiero/block-node/data/historic \
  /opt/hiero/block-node/verification \
  /opt/hiero/block-node/logs
```

- **Expected:** each path reports a filesystem with non-zero size and at least 20% available.
- **If any path is missing:** the volume is not mounted. Run `kubectl -n block-node describe pod $BN_POD` and look for mount errors.

### Confirm provisioner version

Upgrade Solo Provisioner to the latest release before upgrading the Block Node. Your Hashgraph PoC will confirm the supported provisioner version for the cohort.

1. Check the installed version:

   ```bash
   sudo solo-provisioner -v
   ```

   - **Expected output** (version varies by release):

     ```text
     {"version":"0.19.0","commit":"<git-sha>","goversion":"go1.26.0"}
     ```
2. If the version is behind the target, upgrade:

   ```bash
   curl -sSL https://raw.githubusercontent.com/hashgraph/solo-weaver/main/install.sh | bash
   sudo solo-provisioner -v
   ```

   <!-- src: solo-weaver@222c3f47:install.sh#L76-L159 (downloads latest GA release, verifies checksum, runs install subcommand) -->

   The install script downloads the latest GA release for your architecture, verifies the SHA256 checksum, and replaces the existing binary.

### Back up operator-side artifacts

Before the upgrade, confirm you have current copies of the following files stored off the BN host. These cannot be reproduced from upstream if the host is lost.

<!-- src: hiero-block-node@fa8afa83:block-node/app-config/src/main/java/org/hiero/block/node/app/config/state/ApplicationStateConfig.java#L31 (rsaBootstrapFilePath default) -->
<!-- src: solo-weaver@222c3f47:internal/blocknode/storage.go#L388-L407 (ResetStorage clears archive, live, log, and optional paths) -->

|              Artifact              |                                                  Location on host                                                   |                                              Why it is irreplaceable                                               |
|------------------------------------|---------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `block-node-values.yaml`           | `/etc/solo-provisioner/block-node-values.yaml`                                                                      | Cohort-specific Helm overlay generated once at handoff                                                             |
| `rsa-bootstrap-roster.json`        | Default: `/opt/hiero/block-node/node/rsa-bootstrap-roster.json` (configurable via `app.state.rsaBootstrapFilePath`) | RSA public-key roster for WRB proof verification; only needed when not using Mirror Node auto-fetch for the roster |
| TLS bundle (cert, key, full chain) | Operator-managed; delivered at handoff                                                                              | Required to re-establish the CN-to-BN gRPC-over-TLS connection after reinstall                                     |
| `admin_key`                        | Operator-managed key custody                                                                                        | Required to update the on-chain BN registration; losing it makes the registration unmodifiable                     |

> **Caution:** Treat the `admin_key` as a production signing key from day one. Losing it means the on-chain BN registration cannot be updated.

---

## Release-specific checks

Complete the subsection below that matches the upgrade you are preparing for. Future releases will appear here as additional subsections. If no matching subsection exists for your target release, only the common checks above are required.

---

### WRB streaming cutover prep

**Applies to:** the Consensus Node release that activates Wrapped Record Block (WRB) streaming — currently scheduled for CN release 0.77.0. The exact release may change if release testing surfaces a blocker; your Hashgraph PoC will confirm the target release before the maintenance window.

For the full network cutover timeline — phases, CN-side WRB catch-up, TSS ceremony, and Jumpstart Data — see [Cutover Process and Timeline](../Cutover-Process.md).

**What is changing:** from the cutover release onwards, Consensus Nodes cease producing record streams and begin producing Block Streams with full TSS and WRAPS signatures. Block Nodes that have been streaming "preview" blocks (incomplete hashes, pre-cutover testing streams) must be reset before the cutover so they can receive authoritative block history from the Record Block History (RBH) Block Node via backfill.

<!-- src: hiero-block-node@fa8afa83:docs/block-node/Cutover-Process.md (Release N+1: "All 'production' Tier 1 block nodes are reset to clear preview blocks. All 'production' Tier 1 block nodes begin rapid backfill from RBH...") -->

Complete the [common pre-upgrade checks](#common-pre-upgrade-checks) first, then continue here.

#### Clear the block store (preview-blocks reset)

> **Caution:** This operation is destructive and cannot be undone. It scales the StatefulSet to 0, clears all files from the `live`, `archive`, `verification`, and `logging` storage directories, then scales back up to 1. All block data on this node is lost. Do not run this step until your Hashgraph PoC has confirmed the maintenance window is open and your off-host artifact backups are current.

<!-- src: solo-weaver@222c3f47:internal/blocknode/storage.go#L388-L407 (ResetStorage function) -->
<!-- src: solo-weaver@222c3f47:cmd/cli/commands/block/node/reset.go (Use: "reset"; 5-step Long description) -->

```bash
sudo solo-provisioner block node reset --profile=mainnet
```

- **Expected output:**

  ```text
  Ensuring weaver service account (weaver:2500)
  Preflight Checks
  Scaling down Block Node
  Clearing Block Node storage
  Scaling up Block Node
  Waiting for Block Node to be ready

  Completed successfully
  ```

Wait until the pod reaches `1/1 Running`, then confirm the store is empty:

```bash
grpcurl -plaintext -emit-defaults \
  -import-path ~/bn-proto \
  -proto block-node/api/node_service.proto \
  -d '{}' \
  "$BLOCK_NODE_HOST:40840" \
  org.hiero.block.api.BlockNodeService/serverStatus
```

- **Expected immediately after reset:**

  ```json
  {
    "firstAvailableBlock": "18446744073709551615",
    "lastAvailableBlock": "18446744073709551615",
    "onlyLatestState": false
  }
  ```

  Both values at `18446744073709551615` confirm the store is empty and the Block Node is ready to backfill.

If the reset fails, check `sudo head /opt/solo/weaver/logs/solo-provisioner.log` and contact your Hashgraph PoC if the pod remains stuck.

#### Provision the RSA bootstrap roster

The `roster-bootstrap-rsa` plugin must have the RSA public-key roster at startup to verify `SignedRecordFileProof` items in incoming WRBs.

<!-- src: hiero-block-node@fa8afa83:docs/design/wrb-streaming/bootstrap-roster-plugin.md (Purpose section) -->
<!-- src: hiero-block-node@fa8afa83:block-node/roster-bootstrap-rsa/src/main/java/org/hiero/block/node/roster/bootstrap/rsa/RsaRosterBootstrapConfig.java#L24-L27 (@ConfigData roster.bootstrap.rsa, mirrorNodeBaseUrl default "") -->
<!-- src: hiero-block-node@fa8afa83:block-node/app-config/src/main/java/org/hiero/block/node/app/config/state/ApplicationStateConfig.java#L31 (rsaBootstrapFilePath default) -->

**Mirror Node auto-fetch (mainnet cohort default)**

When `roster.bootstrap.rsa.mirrorNodeBaseUrl` is set in your `block-node-values.yaml`, the plugin fetches the roster from the Mirror Node REST API at startup and caches it locally. After `block node reset`, the cached copy is cleared along with the rest of the storage directories and is automatically re-fetched on the next pod start — no manual intervention is needed. Before the cutover window, confirm outbound connectivity to the Mirror Node is available from the BN host.

<!-- src: hiero-block-node@fa8afa83:block-node/roster-bootstrap-rsa/src/main/java/org/hiero/block/node/roster/bootstrap/rsa/RsaRosterBootstrapPlugin.java (three-source strategy; concurrent peer BN and Mirror Node queries) -->

> Note: If the Mirror Node is unreachable, the plugin polls indefinitely — every 5 seconds until the first roster is fetched, then every 60 seconds for periodic refresh. Both intervals are configurable. WRB proof verification is non-functional until the roster is available.

**Peer Block Node query (optional)**

When `roster.bootstrap.rsa.blockNodeSourcesPath` is set, the plugin also queries configured peer Block Nodes via gRPC to retrieve the address book. This runs concurrently with the Mirror Node query when no bootstrap file is present; whichever responds first provides the initial roster. The peer BN query follows the same two-phase polling schedule (5-second initial interval, 60-second subsequent interval, both configurable). Configure the path to a JSON file listing your peer Block Nodes:

```json
{
  "nodes": [
    { "address": "10.0.0.1", "port": 8080, "priority": 1, "name": "peer-bn-1" },
    { "address": "10.0.0.2", "port": 8080, "priority": 2, "name": "peer-bn-2" }
  ]
}
```

**Manual file (alternative)**

If `roster.bootstrap.rsa.mirrorNodeBaseUrl` is not configured, the plugin reads the roster from `app.state.rsaBootstrapFilePath` (default: `/opt/hiero/block-node/node/rsa-bootstrap-roster.json`). The file is delivered as part of the cohort package by your Hashgraph PoC. After `block node reset`, confirm the file is still present and non-empty:

```bash
kubectl -n block-node exec $BN_POD -c block-node-server -- \
  ls -lh /opt/hiero/block-node/node/rsa-bootstrap-roster.json
```

- **If the file is missing after reset:** re-deliver it from your off-host backup using the mechanism in your cohort's `block-node-values.yaml`.

In either case, confirm the roster loaded cleanly after the pod starts:

```bash
kubectl -n block-node logs $BN_POD -c block-node-server | grep -i "roster\|bootstrap\|rsa"
```

- **Expected:** no `WARNING` or `ERROR` lines referencing the roster.

#### Configure backfill sources (if required)

After the block store reset, the BN backfills WRB history automatically as long as other Block Nodes on the network already hold the relevant block range. **Most Tier 1 operators do not need to manually configure a backfill source** — once peer Block Nodes are advertising history on the network, the backfill plugin discovers them through the normal backfill path.

<!-- src: hiero-block-node@fa8afa83:block-node/backfill/src/main/java/org/hiero/block/node/backfill/BackfillConfiguration.java#L39 (blockNodeSourcesPath default "") -->
<!-- src: hiero-block-node@fa8afa83:charts/block-node-server/values.yaml#L171 (BACKFILL_BLOCK_NODE_SOURCES_PATH) -->

**Enable greedy backfill**

Hashgraph recommends enabling greedy backfill on all Tier 1 Block Nodes for the WRB cutover. With greedy backfill enabled, the BN proactively retrieves blocks beyond the latest acknowledged block, preventing the node from falling too far behind during the initial catch-up period.

<!-- src: hiero-block-node@fa8afa83:block-node/backfill/src/main/java/org/hiero/block/node/backfill/BackfillConfiguration.java#L43 (backfill.greedy default false) -->

In your Helm values overlay, set `BACKFILL_GREEDY` to `"true"`, apply the change, and confirm:

```bash
helm -n block-node upgrade <release-name> <chart> -f block-node-values.yaml
helm -n block-node get values <release-name> | grep BACKFILL_GREEDY
```

- **Expected:** `BACKFILL_GREEDY: "true"`

**Edge case — bootstrapping from genesis when no public BN holds the history yet:**

During the initial mainnet WRB cutover, the wrapped record block history may not yet be available from public Block Nodes. In this case, a special-purpose Block Node holding the offline-wrapped WRBs serves as a temporary backfill source. Operators who need access to this node will receive the endpoint and connection details through a separate operator communication channel — it is not published in this document.

If you are directed by your Hashgraph PoC to configure a specific backfill source:

1. Update your Helm values overlay to set `BACKFILL_BLOCK_NODE_SOURCES_PATH` to the path of a `block-node-sources.json` file, then confirm it was applied:

   ```bash
   helm -n block-node get values <release-name> | grep -i backfill
   ```
2. The `block-node-sources.json` file format is a JSON array of Block Node endpoints. Use the hostname and port provided by your Hashgraph PoC:

   ```json
   [
     {
       "address": "<host-provided-by-hashgraph-poc>",
       "port": 40840
     }
   ]
   ```

   > Note: Even when backfilling from a special-purpose BN, all blocks are cryptographically verified by the BN's verification plugin before they are stored. Block legitimacy is confirmed regardless of the backfill source.

3. Monitor backfill progress. Full backfill of current network history takes one to several weeks; the BN does not need to complete backfill before the cutover release, but it must be active and making progress:

   ```bash
   kubectl -n block-node logs $BN_POD -c block-node-server --since=5m | grep -i "backfill"
   ```

---

## Verify readiness

After completing all applicable checks, confirm the BN is ready before the maintenance window opens.

|                   Check                   |                                    Command                                    |                           Expected result                            |
|-------------------------------------------|-------------------------------------------------------------------------------|----------------------------------------------------------------------|
| Pod is running                            | `kubectl -n block-node get pods`                                              | `1/1 Running`                                                        |
| Hardware preflight passes                 | `sudo solo-provisioner block node check --profile=mainnet`                    | All steps green                                                      |
| Block store cleared (WRB only)            | `grpcurl ... BlockNodeService/serverStatus`                                   | `firstAvailableBlock = uint64_max`                                   |
| RSA roster loaded (WRB only)              | `kubectl -n block-node logs $BN_POD -c block-node-server \| grep -i roster`   | No WARNING or ERROR                                                  |
| Greedy backfill enabled (WRB only)        | `helm -n block-node get values <release-name> \| grep BACKFILL_GREEDY`        | `BACKFILL_GREEDY: "true"`                                            |
| Backfill active (WRB only, if configured) | `kubectl -n block-node logs $BN_POD -c block-node-server \| grep -i backfill` | Fetching or completed (if `BACKFILL_BLOCK_NODE_SOURCES_PATH` is set) |
| Alloy shipping                            | `kubectl -n grafana-alloy get pods`                                           | `1/1 Running`                                                        |

If any check fails, resolve the issue and confirm readiness with your Hashgraph PoC before the maintenance window opens.

---

## Troubleshooting

|                                     Symptom                                     |                             Likely cause                             |                                                                                                                   Resolution                                                                                                                    |
|---------------------------------------------------------------------------------|----------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `solo-provisioner block node check` fails with "CPU does not meet requirements" | VM has fewer physical cores than the profile minimum                 | See [Block Node Hardware Specifications](./block-node-hardware-specifications.md). The check counts physical cores, not vCPUs.                                                                                                                  |
| `solo-provisioner block node check` fails with "profile flag is required"       | `--profile` flag missing                                             | Always pass `--profile=mainnet`.                                                                                                                                                                                                                |
| `block node reset` exits with "permission denied"                               | Command run without `sudo`                                           | Prefix with `sudo`.                                                                                                                                                                                                                             |
| Pod stuck in `0/1` or init containers running after reset                       | Init containers setting up storage and resolving plugins             | Allow 2-5 minutes. Run `kubectl -n block-node describe pod $BN_POD` to see init-container status.                                                                                                                                               |
| RSA roster missing or not loaded after reset                                    | Mirror Node unreachable, or file missing (file-based delivery)       | If using Mirror Node auto-fetch, confirm `roster.bootstrap.rsa.mirrorNodeBaseUrl` is set and the Mirror Node is reachable — the roster is re-fetched automatically on pod start. If using file-based delivery, re-deliver from off-host backup. |
| Backfill not starting                                                           | `backfill.blockNodeSourcesPath` is blank or points to a missing file | Confirm `BACKFILL_BLOCK_NODE_SOURCES_PATH` is set and the referenced JSON file exists in the pod.                                                                                                                                               |
| `firstAvailableBlock` still shows old block numbers after reset                 | Reset did not complete successfully                                  | Check `sudo head /opt/solo/weaver/logs/solo-provisioner.log` for the step that failed.                                                                                                                                                          |
| `grpcurl` returns `connection refused` on port 40840                            | Block Node is not yet listening                                      | Wait for the pod to reach `1/1 Running`; confirm `server.port` in the Block Node configuration.                                                                                                                                                 |

For issues not covered here, see [Block Node Troubleshooting](../troubleshooting.md). When opening a support ticket, attach:

```bash
sudo solo-provisioner -v --output=json
helm -n block-node list
kubectl -n block-node get pods,sts,svc,events
kubectl -n block-node logs $BN_POD -c block-node-server --tail=2000
```

---
