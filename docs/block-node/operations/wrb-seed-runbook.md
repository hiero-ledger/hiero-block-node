# WRB Seeding Runbook

## Overview

This runbook covers deploying a Local-Full-History (LFH) Block Node via Solo Provisioner and seeding it with a historical [Wrapped Record Block](../glossary.md#wrb-wrapped-record-block) (WRB) archive. The procedure applies to any environment (previewnet, testnet, mainnet) that uses the single-node Solo-Provisioner deployment shape; examples below use previewnet paths and values files, but substituting the equivalent `<env>-lfh-*.yaml` files and `<env>` profile targets the same steps at testnet or mainnet.

**Audience**: Operators standing up a Tier 1 Block Node for the first time, or seeding an existing empty install with historical blocks.

**Scope**: Covers the T1 backfill portion of the WRB distribution operator runbook (part of #2961). Does not cover T2 live-push, T3 address-book conversion, or T4 roster/TSS configuration.

---

## Prerequisites

- GCP VM (or equivalent) provisioned per [Solo Weaver Single-Node K8s Deployment](./solo-weaver-single-node-k8s-deployment.md)
- `sudo solo-provisioner -v` returns a version string on the VM
- `kubectl` on PATH, current context pointed at the target cluster
- `java` on PATH (JDK 25+)
- A shaded tools jar (`tools-*-all.jar`) staged on the VM
- A local WRB archive directory to seed from
- The 4 values/config files from PR #3333 staged on the VM:
  - `previewnet-lfh-provisioner-config.yaml` (or the `-smoketest` variant)
  - `previewnet-lfh-values.yaml` (or the `-smoketest` variant)
- `previewnet-lfh-static-pvs.yaml` if the target cluster does not have a default `StorageClass` (see [Footgun 1](#footgun-1-persistencecreate-true-requires-a-default-storageclass) below)

---

## Fresh install + seed (recommended)

### Step 1. Pre-provision static PVs (single-node clusters only)

If the target cluster does not have a default `StorageClass` (Solo Provisioner's stock previewnet install does not), pre-apply the static PVs before running `solo-provisioner install`. Otherwise the chart's `volumeClaimTemplate` PVCs stay `Pending` forever and the BN pod cannot schedule.

```bash
kubectl apply -f previewnet-lfh-static-pvs.yaml
```

Verify PVs are `Available`:

```bash
kubectl get pv | grep pv-static
```

### Step 2. Install the Block Node

**Production shape** (properly-sized VM, 3 TB+ disk):

```bash
sudo solo-provisioner block node install \
  -p previewnet \
  --config previewnet-lfh-provisioner-config.yaml \
  --values previewnet-lfh-values.yaml \
  --non-interactive
```

**Smoke-test shape** (smaller-disk VM):

```bash
sudo solo-provisioner block node install \
  -p previewnet \
  --config previewnet-lfh-provisioner-config-smoketest.yaml \
  --values previewnet-lfh-values-smoketest.yaml \
  --skip-hardware-checks \
  --non-interactive
```

Verify install completed:

```bash
kubectl get pods -n block-node
kubectl get pvc -n block-node
```

Expected: pod `1/1 Running`, 5 PVCs `Bound`.

### Step 3. Seed the historic archive

Scale the BN down (the seed writes to the archive PV directly and must not race the running pod):

```bash
kubectl scale statefulset/block-node-block-node-server --replicas=0 -n block-node
```

Wait for pod termination, then run the backfill:

```bash
CLI_JAR=/path/to/tools-*-all.jar \
sudo -E ./backfill-wrb-to-bn.sh --install-and-seed \
  --profile previewnet \
  --config previewnet-lfh-provisioner-config-smoketest.yaml \
  --values previewnet-lfh-values-smoketest.yaml \
  /path/to/wrappedBlocks
```

The script runs `blocks bulk-load` to stage the archive, hard-links (or copies, if cross-filesystem) into the archive PV's hostPath, and chowns to `2000:2000` (the BN's runtime user).

### Step 4. Move seeded files into the `archive-data/` subdir

**Required manual step** because the chart's `archive-storage` volumeMount uses `subPath: archive-data`, so the pod reads from `<pv-hostpath>/archive-data/` rather than `<pv-hostpath>/` (see [Footgun 2](#footgun-2-subpath-mismatch-in-the-backfill-script)):

```bash
cd /mnt/fast-storage/block-node/archive
sudo bash -c 'for e in *; do [ "$e" != "archive-data" ] && mv "$e" archive-data/; done'
sudo chown -R 2000:2000 archive-data
```

### Step 5. Scale the BN back up

```bash
kubectl scale statefulset/block-node-block-node-server --replicas=1 -n block-node
```

Wait ~30s for the pod to become `1/1 Ready`. Check for crashes:

```bash
kubectl get pod -n block-node
```

If `RESTARTS > 0`, see [Footgun 3](#footgun-3-plugin-crash-loop-on-single-corrupt-zip).

### Step 6. Verify blocks are served

```bash
kubectl port-forward -n block-node svc/block-node-block-node-server 18082:40982 &
sleep 3
grpcurl -plaintext -emit-defaults \
  -import-path /path/to/block-node-protobuf-<version> \
  -proto block-node/api/node_service.proto \
  -d '{}' \
  localhost:18082 org.hiero.block.api.BlockNodeService/serverStatus
```

Expected shape after a successful seed:

```json
{
  "firstAvailableBlock": "0",
  "lastAvailableBlock": "<highest block number in the seeded archive>",
  "onlyLatestState": false,
  "nextExpectedBlock": "0"
}
```

The BN startup log should also show the range populated:

```
Started BlockNode Server : State=RUNNING HistoricBlockRange=0->1699999
```

If `serverStatus` returns `firstAvailableBlock: "18446744073709551615"` (UINT64_MAX sentinel), the plugin scanned an empty archive — the seed data is likely at the volume root instead of `archive-data/`. Re-run Step 4.

---

## Operational footguns

Non-obvious behaviors an operator will encounter. Working around each is documented; upstream fixes are tracked as separate follow-up issues.

### Footgun 1: `persistence.create: true` requires a default StorageClass

**Symptom**: after `solo-provisioner install`, `kubectl get pvc -n block-node` shows all 5 chart-provisioned PVCs (`*-storage-block-node-block-node-server-0`) `Pending`, and the pod is stuck at `Pending` with scheduling event `pod has unbound immediate PersistentVolumeClaims`.

**Cause**: the values file sets `persistence.*.create: true`, which asks the chart to provision PVCs via the cluster's default `StorageClass`. Solo Provisioner's stock previewnet install does not create a default `StorageClass`, so no `PersistentVolume` is ever produced to bind those PVCs to.

**Fix**: apply `previewnet-lfh-static-pvs.yaml` (Step 1 above) BEFORE `solo-provisioner install`. The 5 PVs in that file are pre-bound (`spec.claimRef`) to the exact PVC names the chart's volumeClaimTemplate generates, so they bind on creation.

**Upstream fix pending**: install `local-path-provisioner` (or equivalent) into Solo Provisioner's default cluster setup so `persistence.create: true` works out of the box.

### Footgun 2: subPath mismatch in the backfill script

**Symptom**: after running `backfill-wrb-to-bn.sh` and starting the BN, `serverStatus` returns `firstAvailableBlock: "18446744073709551615"` (UINT64_MAX sentinel) even though the seed reported success. The startup log shows `HistoricBlockRange=` (empty).

**Cause**: the chart's `archive-storage` volumeMount includes `subPath: archive-data`, so the pod's `/opt/hiero/block-node/data/historic` is backed by `<pv-hostpath>/archive-data/` (a subdirectory of the PV) rather than the PV root. The backfill script writes to the PV root, one level above where the pod actually reads.

The values file explicitly sets `subPath: ""` for the archive volume, but the effective pod spec still shows `subPath: archive-data`. Either the chart template ignores the values-file override, or applies a hardcoded default that clobbers it.

**Fix**: after the script completes and BEFORE bringing the BN back up, move the seeded files into the `archive-data/` subdir (Step 4 above).

**Upstream fix pending**: chart template should either honor the values-file `subPath: ""` override, or the backfill script should be aware of the effective `subPath` and write to the correct location directly.

### Footgun 3: plugin crash-loop on single corrupt zip

**Symptom**: after seeding, the pod initially starts fine (1/1 Ready, `HistoricBlockRange` populated in the startup log). On the NEXT restart (helm upgrade, pod delete, node reboot) it enters `CrashLoopBackOff` with this exception:

```
Exception in thread "main" java.lang.IllegalStateException:
  First zipped block number [0] cannot be greater than the latest zipped block number [-1]
    at BlockFileHistoricPlugin.init(BlockFileHistoricPlugin.java:181)
```

**Cause**: during the first successful startup, `BlockFileHistoricPlugin` scans the archive and detects any content-corrupt zip files (valid ZIP structure, but internal block data unreadable). Corrupt files are moved to a `corrupted/` quarantine subdirectory. The quarantine leaves three artifacts that trip up subsequent inits:

1. `corrupted/` subdirectory inside the historic dir (walked by the plugin as if it were part of the archive)
2. An empty leaf directory where the quarantined file used to live (e.g., `000/000/000/000/17/`)
3. A stale `historic-plugin-bulk-load-state.json` file left over from the bulk-load tool

Any of these three can make `maxStoredBlockNumber` return `-1` on subsequent init, which triggers the fatal `firstBlock (0) > latestBlock (-1)` check.

**Fix**: scale down, remove all three artifacts, scale back up:

```bash
kubectl scale statefulset/block-node-block-node-server --replicas=0 -n block-node

# Wait for pod to terminate, then clean up:
sudo rm -rf /mnt/fast-storage/block-node/archive/archive-data/corrupted
sudo rmdir /mnt/fast-storage/block-node/archive/archive-data/000/000/000/000/<empty-leaf>
sudo rm /mnt/fast-storage/block-node/archive/archive-data/historic-plugin-bulk-load-state.json

kubectl scale statefulset/block-node-block-node-server --replicas=1 -n block-node
```

After cleanup the plugin comes up cleanly and serves the (170-out-of-171) valid blocks. The single corrupt file's block range (e.g., blocks 170000–179999 for `17/00000.zip`) is missing from the archive — the BN can still fill that gap via live backfill from Tier 0 once wired up.

**Upstream fix pending**: `BlockFileHistoricPlugin.init()` should be resilient to (a) the `corrupted/` subdirectory in the walk, (b) empty leaf directories left by quarantine, and (c) stale state files from the bulk-load tool. One bad file in a 171-file archive should not permanently prevent the BN from starting.

---

## Reference

- Deployment shape and hardware guidance: [Solo Weaver Single-Node K8s Deployment](./solo-weaver-single-node-k8s-deployment.md)
- WRB CLI operations (produce/wrap/validate WRB archives): [WRB CLI Runbook](./wrb-cli-runbook.md)
- Preparing a BN for WRB cutover: [Preparing Your Block Node for WRB Cutover](./preparing-your-block-node-for-wrb-cutover.md)
- WRB streaming design: [Special-purpose WRB BN design](../../design/wrb-streaming/sp-wrb-bn-design.md)
- Backfill script source: `tools-and-tests/tools/scripts/backfill-wrb-to-bn.sh`
- Static PVs YAML: `charts/block-node-server/values-overrides/previewnet-lfh-static-pvs.yaml`
