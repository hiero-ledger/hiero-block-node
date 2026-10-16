# DR Playbook: Block Node Bootstrap from Snapshot

> **Summary:** A Block Node's on-disk data is corrupt, missing, or incompatible and cannot recover
> through normal reset. This playbook restores the Block Node from an offline backup snapshot so it
> resumes serving with minimal backfill instead of re-ingesting the full history from scratch.

|                      |                                                                                           |
|----------------------|-------------------------------------------------------------------------------------------|
| **Playbook ID**      | PB-01                                                                                     |
| **Severity**         | SEV-2 (SEV-1 if the affected BN is the only Tier-1 node serving the network)              |
| **Primary executor** | Hashgraph DevOps / Tier-1 Block Node operator                                             |
| **Escalate to**      | Hashgraph Engineering if snapshot restore fails or `serverStatus` reports unexpected data |

---

## Pre-incident preparation

> **Fill this in before an incident — not during one.**

|          Variable           | Your value |
|-----------------------------|------------|
| `$RELEASE`                  |            |
| `$NAMESPACE`                |            |
| `$BLOCK_NODE_HOST`          |            |
| `<SNAPSHOT>` path           |            |
| Snapshot cadence            |            |
| On-call Engineering contact |            |

If any of these are blank when an incident fires, locate them first — every other step in this
playbook depends on them. Quick lookups:
- `$RELEASE` and `$NAMESPACE`: `helm list -A | grep block-node`
- `$BLOCK_NODE_HOST`: `kubectl get svc -n $NAMESPACE | grep block-node` — use the EXTERNAL-IP or NodePort hostname

---

## Is this your doc?

**Use this playbook when:**

- The Block Node pod fails to start, crashes repeatedly on startup, or `serverStatus` returns
  block-range values that disagree with peer Block Nodes
- You need to restore a Block Node after data corruption without waiting weeks for full re-ingestion
- You are executing the recovery path from another playbook (PB-04 or PB-06) that references
  snapshot bootstrap

**Not your doc if:**

- The Block Node is healthy but behind — it will backfill automatically; no action needed
- You have no snapshot backup available — follow
  [Resetting and Upgrading the Block Node](../resetting-and-upgrading-the-block-node.md) and accept
  a full re-ingestion window instead
- A standby Block Node is available — use PB-02 to route traffic to the standby immediately,
  then return here to restore the broken BN without time pressure

---

## Before starting

> **Open the incident bridge if this is a production outage. Assign:**
> - **Incident commander** — owns resolution, makes go/no-go calls
> - **Communication lead** — owns public status page and stakeholder updates
>
> Confirm a snapshot backup exists and is accessible before executing any steps.
> If no snapshot is available, stop here and use the plain reset path.
>
> **Prerequisite — grpcurl and bn-proto:** Solo Provisioner does not install `grpcurl` or the Block
> Node protobuf bundle. If either is missing on the machine, set up `~/bn-proto` before starting
> Phase 1:
>
> ```bash
> BUNDLE_URL=$(curl -s https://api.github.com/repos/hiero-ledger/hiero-block-node/releases/latest \
>   | grep "browser_download_url.*block-node-protobuf.*tgz" | head -1 | cut -d '"' -f 4)
> mkdir -p ~/bn-proto && curl -sL "$BUNDLE_URL" | tar -xzC ~/bn-proto
> ```
>
> Install `grpcurl` via your system package manager if it is not already present.

---

## Phase 1 - Confirm diagnosis and locate snapshot

**Estimated time: 5-10 minutes**

**Step 1 - Check whether the Block Node can recover on its own**

```bash
kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server --tail=100 | grep -iE "ERROR|FATAL|corrupt|exception"
```

| #  |                                          Action                                           |                                                                                                                                                                        Notes                                                                                                                                                                        | Done |
|----|-------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1a | Run the log command above and identify the error type.                                    | If no output, the pod may not be starting at all — run `kubectl get pod -n $NAMESPACE` to check status. Transient errors (OOM, network timeout, eviction): attempt a restart — `kubectl rollout restart statefulset/$RELEASE-block-node-server -n $NAMESPACE`, wait 2 minutes, recheck. **If it recovers, stop here — no snapshot restore needed.** | ☐    |
| 1b | If the error is persistent (corrupt data, missing files, incompatible storage), continue. | Do not attempt further restarts — each restart risks making corruption worse. Proceed to Step 2.                                                                                                                                                                                                                                                    | ☐    |

**Step 2 - Record the current (failed) state**

> Skip this step if the pod is not running.
> If `~/bn-proto` is not set up, run the setup command from the **Before Starting** section first.

```bash
grpcurl -plaintext -emit-defaults \
  -import-path ~/bn-proto \
  -proto block-node/api/node_service.proto \
  -d '{}' $BLOCK_NODE_HOST:40982 \
  org.hiero.block.api.BlockNodeService/serverStatus
```

| # |                           Action                            |                                                                                           Notes                                                                                           | Done |
|---|-------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 2 | Run the command above and note the block range.             | `firstAvailableBlock` = _______ / `lastAvailableBlock` = _______                                                                                                                          | ☐    |
| 3 | Locate the snapshot and confirm it predates the corruption. | `<SNAPSHOT>` path (from pre-incident table): _______. Snapshot's latest block: _______. Confirm the snapshot timestamp is earlier than the first signs of corruption.                     | ☐    |
| 4 | Confirm downstream consumers.                               | `curl -s http://$BLOCK_NODE_HOST:16007/metrics \| grep blocknode_subscriber_open_connections`. Note the subscriber count — they will see a brief interruption. Notify if SLA requires it. | ☐    |

---

## Phase 2 - Stop the Block Node

**Estimated time: 2-3 minutes**

| # |                       Action                       |                                                                                                                    Notes                                                                                                                     | Done |
|---|----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1 | Scale the StatefulSet to 0 to stop the pod safely. | `kubectl scale statefulset/$RELEASE-block-node-server --replicas=0 -n $NAMESPACE`. Then wait: `kubectl wait pod -l app.kubernetes.io/instance=$RELEASE -n $NAMESPACE --for=delete --timeout=120s`. Do not proceed until the command returns. | ☐    |
| 2 | Confirm PVCs are intact — do not delete them.      | `kubectl get pvc -n $NAMESPACE`. You must see `live-storage-pvc`, `archive-storage-pvc`, and `application-state-storage-pvc`. If any are missing, stop and escalate to Engineering immediately.                                              | ☐    |

---

## Phase 3 - Restore snapshot data

**Estimated time: depends on snapshot size**

> **Note:** This procedure assumes a Solo Provisioner deployment where PVCs are backed by hostPath
> volumes on the host. For other deployment types, confirm the restore approach with Hashgraph
> Engineering before proceeding.

**Step 1 - Locate PVC host paths**

```bash
kubectl get pv -o custom-columns=NAME:.metadata.name,PATH:.spec.hostPath.path,CLAIM:.spec.claimRef.namespace \
  | grep $NAMESPACE
```

Record the PATH values for the live and archive PVs. You will need them in Steps 2 and 3.

| # |                      Action                       |                      Notes                       | Done |
|---|---------------------------------------------------|--------------------------------------------------|------|
| 1 | Run the command above and record both host paths. | `LIVE_PATH` = _______ / `ARCHIVE_PATH` = _______ | ☐    |

**Step 2 - Restore snapshot data**

Replace `<SNAPSHOT>` with the snapshot path from the pre-incident table, and `<LIVE_PATH>` /
`<ARCHIVE_PATH>` with the values from Step 1.

```bash
sudo rsync -a --delete <SNAPSHOT>/live/    <LIVE_PATH>/
sudo rsync -a --delete <SNAPSHOT>/archive/ <ARCHIVE_PATH>/
sudo chown -R 2000:2000 <LIVE_PATH> <ARCHIVE_PATH>
```

The `--delete` flag removes files not present in the snapshot, so a separate clear step is not
needed. The `chown` resets ownership to the Block Node process user (UID/GID 2000).

| # |              Action               |                                        Notes                                        | Done |
|---|-----------------------------------|-------------------------------------------------------------------------------------|------|
| 2 | Run the rsync and chown commands. | Snapshot path (from pre-incident table): _______. Snapshot's latest block: _______. | ☐    |

**Step 3 - Spot-check the restore**

```bash
ls -lh <LIVE_PATH>/ | head -20
ls -lh <ARCHIVE_PATH>/ | head -20
```

| # |                          Action                           |                                                                                                Notes                                                                                                 | Done |
|---|-----------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 3 | Run ls on both paths and confirm block files are present. | Expected: numbered block files or subdirectories — non-empty output means the restore succeeded. If either directory is empty, the restore in Step 2 did not complete - repeat it before continuing. | ☐    |

---

## Phase 4 - Restart and verify

**Estimated time: 5-10 minutes**

| # |                              Action                               |                                                                                          Notes                                                                                          | Done |
|---|-------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1 | Scale the StatefulSet back to 1 and wait for the pod to be ready. | `kubectl scale statefulset/$RELEASE-block-node-server --replicas=1 -n $NAMESPACE && kubectl rollout status statefulset/$RELEASE-block-node-server -n $NAMESPACE --timeout=300s`         | ☐    |
| 2 | Confirm the Block Node started cleanly.                           | `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server --tail=50`. No `ERROR` or `FATAL` lines should appear during init. If they do, see Troubleshooting below. | ☐    |

**Step 3 - Verify block range matches the snapshot**

```bash
grpcurl -plaintext -emit-defaults \
  -import-path ~/bn-proto \
  -proto block-node/api/node_service.proto \
  -d '{}' $BLOCK_NODE_HOST:40982 \
  org.hiero.block.api.BlockNodeService/serverStatus
```

| # |                    Action                     |                                                                                 Notes                                                                                  | Done |
|---|-----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 3 | Run serverStatus and confirm the block range. | `firstAvailableBlock` should match the oldest block in the snapshot. `lastAvailableBlock` should match the snapshot's latest block, then advance as new blocks arrive. | ☐    |

**Step 4 - Confirm the Block Node is ingesting new blocks**

```bash
watch -n 10 'grpcurl -plaintext -emit-defaults \
  -import-path ~/bn-proto \
  -proto block-node/api/node_service.proto \
  -d "{}" '$BLOCK_NODE_HOST':40982 \
  org.hiero.block.api.BlockNodeService/serverStatus'
```

| # |                                       Action                                        |                                                         Notes                                                          | Done |
|---|-------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|------|
| 4 | Run the watch command and confirm `lastAvailableBlock` increases every ~10 seconds. | If it does not advance after 2-3 minutes, see Troubleshooting below.                                                   | ☐    |
| 5 | Confirm backfill is running for the gap between snapshot and chain tip.             | `curl -s http://$BLOCK_NODE_HOST:16007/metrics \| grep backfill`. `backfill_pending_blocks` should decrease over time. | ☐    |

---

## Phase 5 - Post-incident

- [ ] File an incident report: root cause of the corruption, snapshot date used, gap blocks
  requiring backfill, and estimated backfill completion time
- [ ] Notify downstream consumers (Mirror Nodes, subscribers) that the Block Node is restored
- [ ] Review backup frequency: if the snapshot was more than 24 hours old, the backfill
  window is longer than necessary — confirm daily snapshot cadence is in place
- [ ] Investigate and fix the root cause of the original corruption before returning this
  Block Node to production load

---

## Troubleshooting

|                             Symptom                              |                         Likely cause                         |                                                                                                                                           Action                                                                                                                                           |
|------------------------------------------------------------------|--------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pod starts but immediately crashes (CrashLoopBackOff)            | Snapshot data is incompatible with the running chart version | Check the snapshot version against the current chart version. If they differ, upgrade the chart to match the snapshot version before restoring, or use a snapshot from the same version                                                                                                    |
| `firstAvailableBlock` shows `18446744073709551615` after restore | PVC data was not restored — the BN sees an empty store       | Re-run the ls spot-check from Phase 3 Step 3. If empty, the rsync in Phase 3 Step 2 did not complete successfully - repeat it.                                                                                                                                                             |
| `lastAvailableBlock` is not advancing                            | BN is not receiving blocks from CNs                          | `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \| grep -i "publisher\|stream\|connect"`. If no active publisher connection, check that CN `block-nodes.json` still points to this BN's address and that the firewall allows inbound TCP on the publish port |
| Backfill metrics show `backfill_fetch_errors` increasing         | Upstream Block Nodes are unreachable                         | Check connectivity to upstream Tier-1 BNs. Verify the BN's backfill source configuration                                                                                                                                                                                                   |
| Phase 2 Step 1 `kubectl wait` times out                          | Pod did not terminate within 120 seconds                     | `kubectl describe pod -n $NAMESPACE $RELEASE-block-node-server-0` — check for a finalizer blocking deletion. Force-delete if necessary: `kubectl delete pod -n $NAMESPACE $RELEASE-block-node-server-0 --grace-period=0 --force`                                                           |

---

## Reference

### Why this happens

Block Node data corruption can result from:

- Unexpected pod termination mid-write (power loss, OOM kill, node eviction)
- A Block Node release that introduced a storage incompatibility (forward-only migration failed)
- Filesystem or disk hardware errors on the underlying node
- Cross-network reset that left residual incompatible data

### Total estimated time

|                 Phase                  |         Estimate         |
|----------------------------------------|--------------------------|
| Phase 1 - Diagnose                     | 5-10 min                 |
| Phase 2 - Stop                         | 2-3 min                  |
| Phase 3 - Restore                      | depends on snapshot size |
| Phase 4 - Restart and verify           | 5-10 min                 |
| **Total (excluding restore transfer)** | **~15-25 min**           |

### Related playbooks

- PB-02: Partial Tier-1 BN loss *(not yet written)* - route traffic to a standby before restoring
- [PB-04: Network-wide backpressure](./04-network-wide-backpressure.md) - if corruption caused
  CN buffer saturation
- PB-06: BN data corruption or block verification errors *(not yet written)* - for verification
  failures that preceded the corruption

### Key config and metrics

|             Item             |                                      Value                                       |
|------------------------------|----------------------------------------------------------------------------------|
| StatefulSet name             | `$RELEASE-block-node-server`                                                     |
| Namespace                    | `$NAMESPACE`                                                                     |
| `serverStatus` gRPC endpoint | `$BLOCK_NODE_HOST:40982`                                                         |
| Metrics endpoint             | `http://$BLOCK_NODE_HOST:16007/metrics`                                          |
| Backfill metrics             | `backfill_pending_blocks`, `backfill_blocks_backfilled`, `backfill_fetch_errors` |
| Live block PVC               | `live-storage-pvc` — host path found via `kubectl get pv`                        |
| Archive block PVC            | `archive-storage-pvc` — host path found via `kubectl get pv`                     |
| Application state PVC        | `application-state-storage-pvc`                                                  |
| Snapshot restore command     | `rsync -a --delete` from host path — see Phase 3 Step 2                          |
| `<SNAPSHOT>` path            | **[Operator to fill in per environment]**                                        |

