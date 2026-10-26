# DR Playbook: Block Node Data Corruption or Block Verification Errors

> **Summary:** The Block Node (BN) is reporting block verification failures. Identify the failure
> category from your logs and metrics using the classification table in Phase 1 Step 4. Most
> categories resolve with a restart or upgrade (5 minutes to a few hours). Category D (data
> corruption on disk) requires a reset and backfill from a peer (2-6 hours).

|                      |                                                                                             |
|----------------------|---------------------------------------------------------------------------------------------|
| **Playbook ID**      | PB-06                                                                                       |
| **Severity**         | SEV-1 (data integrity at risk or block acknowledgments halted)                              |
| **Primary executor** | DevOps + Engineering                                                                        |
| **Escalate to**      | Hashgraph Engineering immediately if `BAD_BLOCK_PROOF` failures persist across multiple BNs |

---

## Is this your doc?

Check these without running any commands:

- You see any of these strings in BN logs at any level: `BAD_BLOCK_PROOF`,
  `MISSING_VERIFICATION_DATA`, `UNABLE_TO_PARSE`, `UNKNOWN_ERROR`,
  `MISSING_MANDATORY_ITEM`, `MISSING_MANDATORY_FIELD`, `UNSUPPORTED_ITEM_TYPE`,
  `UNSUPPORTED_STREAM_FORMAT`, `UNRECOGNIZED_PROOF_TYPE`
- You received an alert on `verification_blocks_failed` or `verification_blocks_error`
- The BN is up and responding to health checks but `verification_blocks_verified_total` has
  stopped increasing while `verification_blocks_failed_total` or
  `verification_blocks_error_total` is rising

**Not your doc if:**

- The Block Node process is down or the host is unreachable - see PB-04
- All transactions are halted due to network-wide backpressure - start with PB-04 to stabilize,
  then return here to address the underlying verification failure
- The TSS / hinTS ceremony itself is failing (not just verification of blocks it produces) - see
  PB-07
- You see only `CANCELLED` or `CANCELLED_INCOMPLETE` in logs - these are session management
  events, not corruption; see the note in Phase 1 Step 4

---

## Before starting

> **Open the incident bridge immediately. Assign:**
> - **Incident commander** - owns resolution, makes go/no-go calls on each phase
> - **Communication lead** - owns public status page and stakeholder updates (not the person
> executing steps)
>
> Every phase below is executed by the incident commander and designated operators.
> Communication lead posts updates externally while technical work proceeds in parallel.

**These commands assume a Kubernetes deployment** using the Hiero Block Node Helm chart. For
bare-metal or VM deployments, substitute `journalctl -u block-node-server` for `kubectl logs`
and use the host IP directly instead of port-forwarding.

**Prerequisite - grpcurl and bn-proto:** Steps in this playbook use `grpcurl` with the Block
Node protobuf bundle. If either is not already set up on the machine, run the setup from the
**Before Starting** section of PB-01 *(planned -
[#3783](https://github.com/hiero-ledger/hiero-block-node/issues/3783))* before Phase 1 Step 1.
Until PB-01 ships, minimum setup: install `grpcurl` (see
[grpcurl releases](https://github.com/fullstorydev/grpcurl/releases)), then download and extract
the proto bundle from the [hiero-block-node releases page](https://github.com/hiero-ledger/hiero-block-node/releases):

```bash
BUNDLE_URL=$(curl -s https://api.github.com/repos/hiero-ledger/hiero-block-node/releases/latest \
  | grep "browser_download_url.*block-node-protobuf.*tgz" \
  | head -1 | cut -d '"' -f 4)
mkdir -p ~/bn-proto && cd ~/bn-proto
curl -sL -O "$BUNDLE_URL"
tar -xzf block-node-protobuf-*.tgz
```

**Alternatively**, if you have the `hiero-block-node` source checked out at the target release
branch:

```bash
cd /path/to/hiero-block-node
./gradlew :protobuf-sources:generateBlockNodeProtoArtifact
cp -r protobuf-sources/block-node-protobuf ~/bn-proto
```

The protos are written to `protobuf-sources/block-node-protobuf/` in the repo root.

> **Do not enable bad-block dumps yet.** Enabling them requires a pod restart, which can destroy
> the failure state you need for diagnosis. The dump configuration step is Phase 3 Step 0 -
> do it immediately before your first recovery restart.

---

> **Before starting Phase 1:**
> - Do not restart the BN until you have classified the failure (Step 4) - a restart may clear
> the failure state before you can capture evidence
> - Do not reset block storage until Engineering has confirmed data corruption - resets are
> irreversible without a snapshot

## Phase 1 - Diagnose

**Get the pod name** (Kubernetes deployments):

```bash
kubectl get pods -n $NAMESPACE -l app.kubernetes.io/instance=$RELEASE
```

| # |                                                               Action                                                                |                                                                                                                                                                                                                                                                                                                                                                                                                 Notes                                                                                                                                                                                                                                                                                                                                                                                                                  | Done |
|---|-------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1 | Confirm the verification plugin started successfully.                                                                               | `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \| grep -i "tss library"`. Expected success: `TSS library <version> ready to verify block proofs`. If instead you see `"cannot verify block proofs in this process, shutting down"`, the native TSS library failed to load - **go to Category E**. The BN shuts itself down in this case; it is not serving blocks.                                                                                                                                                                                                                                                                                                                                                                                                                                      | ☐    |
| 2 | Read the failure type from metrics. Forward the metrics port if needed: `kubectl port-forward -n $NAMESPACE <pod-name> 16007:16007` | `curl -s http://$BLOCK_NODE_HOST:16007/metrics \| grep -E "verification_blocks_failed\|verification_proof_total\|rsa_roster_mismatch"`. Note the values: `blocknode_verification_blocks_failed_total` = _______ / `blocknode_verification_proof_total_total{proof_type="tss",result="failure"}` = _______ / `blocknode_verification_proof_total_total{proof_type="rsa",result="failure"}` = _______ / `blocknode_rsa_roster_mismatch_total_total` = _______ / `blocknode_verification_blocks_error_total` = _______. **Note on naming:** Prometheus appends `_total` to counter names; the BN also prepends `blocknode_`. The metric key `verification_proof_total` already contains `_total`, so its full Prometheus name is `blocknode_verification_proof_total_total`. This is correct, not a typo. See Reference for full details. | ☐    |
| 3 | Read failure type from BN logs.                                                                                                     | `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \| grep -iE "completed exceptionally\|TSS library.*rejected\|No address book era\|cannot verify block proofs"`. The failure type name appears in the exception message alongside `"completed exceptionally"`, e.g. `"Verification session for block N with source PUBLISHER failed with BAD_BLOCK_PROOF"`. Additional patterns: `"TSS library X rejected the block signature"` (WARNING on first TSS proof rejection); `"No address book era covers block N"` (WARNING when RSA address book history does not cover the block). Record the failure type: _______                                                                                                                                                                                         | ☐    |

**Step 4 - Classify the failure using this table:**

|           Failure type in logs (Step 3) or metrics (Step 2)            | Category |         Typical time to recover         |                  Escalate immediately if...                   |
|------------------------------------------------------------------------|----------|-----------------------------------------|---------------------------------------------------------------|
| `UNSUPPORTED_ITEM_TYPE`                                                | **A**    | 1-4 hours (upgrade required)            | No BN release supporting this item type                       |
| `UNSUPPORTED_STREAM_FORMAT`                                            | **A**    | 1-4 hours (upgrade or stream issue)     | Upgrade does not resolve it                                   |
| `UNKNOWN_ERROR`                                                        | **A/D**  | Varies - check dump files               | Recurs after restart or backfill                              |
| `MISSING_VERIFICATION_DATA`                                            | **B**    | 5-30 minutes (restart to reload)        | Failures persist after restart                                |
| `BAD_BLOCK_PROOF` or `UNRECOGNIZED_PROOF_TYPE`                         | **C**    | 30 min - 4 hours (depends on scope)     | Multiple BNs affected simultaneously                          |
| `UNABLE_TO_PARSE`, `MISSING_MANDATORY_ITEM`, `MISSING_MANDATORY_FIELD` | **D**    | 2-6 hours (reset + backfill)            | No peer BN has an intact copy of the blocks                   |
| TSS library startup failure (from Step 1)                              | **E**    | 30-60 minutes (environment fix)         | Library still fails after fixing temp directory permissions   |
| `CANCELLED` or `CANCELLED_INCOMPLETE`                                  | —        | Self-resolving (not a corruption event) | Sustained and `verification_blocks_verified_total` not rising |

Category: _______

> **`CANCELLED` and `CANCELLED_INCOMPLETE`:** These are session management events, not data
> corruption. `CANCELLED` means a complete block was evicted from the active sessions buffer,
> which is normal under high load. `CANCELLED_INCOMPLETE` means the publisher abandoned the block
> before it was fully received (normal after a publisher restart or reconnect). If these are
> frequent and sustained and `verification_blocks_verified_total` is flat, the issue is upstream
> - see PB-04 for backpressure investigation.

| # |                                                        Action                                                        |                                                                                                                                                                                                     Notes                                                                                                                                                                                                      | Done |
|---|----------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 5 | Check whether peer BNs are seeing the same failures. **Skip this step if you are not Category C - jump to Phase 2.** | If you operate more than one Tier-1 BN, check the same metrics and logs on each. If ALL BNs report `BAD_BLOCK_PROOF` for the same block range, the blocks themselves may be malformed (upstream CN issue) - **escalate to Hashgraph Engineering immediately**. If only this BN shows failures while peers are verifying normally, the issue is local to this BN. Record: other BNs also failing Y / N: _______ | ☐    |

---

## Phase 2 - Buy Time

| # |                                                      Action                                                       |                                                                                                                                                                                                                                                                                                   Notes                                                                                                                                                                                                                                                                                                    | Done |
|---|-------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1 | If backpressure is already active on CNs (all transactions halted), engage PB-04 Step 2 to route to a standby BN. | Proof-level verification failures (e.g. `BAD_BLOCK_PROOF`) do not by themselves cause backpressure - the BN ACKs blocks at the publisher layer before verification runs. However, if the BN has crashed, stopped accepting publisher connections, or is persistently rejecting blocks without ACKing them, CNs will stop receiving ACKs and backpressure will develop. Check: `curl -s http://<CN_HOST>:<CN_METRICS_PORT>/metrics \| grep blockStream_buffer_backPressureState`. Value 3 = active - engage PB-04 in parallel. Get the CN metrics endpoint from your CN team if you do not have it on hand. | ☐    |
| 2 | Preserve BN logs before any restart.                                                                              | `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server > /tmp/bn_verification_incident_$(date +%Y%m%d_%H%M%S).log`. Share this log with Engineering when escalating.                                                                                                                                                                                                                                                                                                                                                                                                                | ☐    |

---

## Phase 3 - Recover

Identify your failure category from Phase 1 Step 4 and jump to that section. **Complete Step 0 first regardless of category.**

### Step 0 - Enable bad-block dumps [all categories, before any restart]

The `BadBlockDumper` writes the raw bytes of each failing block to disk so Engineering can inspect
them after recovery. Enable it now so the recovery restart in your category captures any residual
failures.

> **If `VERIFICATION_DUMP_ENABLED` is already `true`** (check the pre-incident config record at
> the end of this doc), skip the Helm upgrade below - dumps are already active. Proceed directly
> to your category's steps.

```yaml
# Add to your Helm override values:
blockNode:
  config:
    VERIFICATION_DUMP_ENABLED: "true"
```

```bash
helm upgrade $RELEASE oci://ghcr.io/hiero-ledger/hiero-block-node/block-node-server \
  --version <chart-version> -n $NAMESPACE -f <path-to-your-values.yaml>
```

This configuration change triggers a pod restart. Wait for the pod to become ready before
proceeding:

```bash
kubectl rollout status statefulset/$RELEASE-block-node-server -n $NAMESPACE --timeout=300s
```

Dump files appear at `VERIFICATION_DUMP_DIRECTORY_PATH` (default:
`/opt/hiero/block-node/verification/dumps`). Two files are written per failed block:
`block-<N>-<FAILURE_TYPE>.blk` and `block-<N>-<FAILURE_TYPE>.meta.json`. Files are retained for
`VERIFICATION_DUMP_RETENTION_DAYS` days (default: 7). Copy them off the pod before they age out:

```bash
kubectl cp $NAMESPACE/$RELEASE-block-node-server-0:/opt/hiero/block-node/verification/dumps \
  /tmp/bn-dumps-$(date +%Y%m%d_%H%M%S)/
```

---

### Category A - Stream format or item type mismatch (BN needs upgrade)

The BN cannot process the block stream format or item type that the network is now producing.
This BN cannot verify new blocks until it is upgraded.

| #  |                             Action                             |                                                                                                                                                                                                                                                                                                                                              Notes                                                                                                                                                                                                                                                                                                                                               | Done |
|----|----------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| A1 | Identify the unsupported item type or stream format from logs. | Check BN logs: `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \| grep -E "UNSUPPORTED_ITEM_TYPE\|UNSUPPORTED_STREAM_FORMAT"`. The exception message names the specific item type or format that failed. If dump files were written in Phase 3 Step 0, inspect the `.meta.json` file for the affected block - it includes the failure type and relevant context. Then confirm the latest BN release addresses it: [hiero-block-node releases](https://github.com/hiero-ledger/hiero-block-node/releases). If no compatible release is available: **escalate to Hashgraph Engineering** - do not attempt to run an unsupported BN version against the live network. | ☐    |
| A2 | Upgrade this BN to a release that supports the item type.      | Follow [Resetting and Upgrading the Block Node](../resetting-and-upgrading-the-block-node.md). Do not reset block storage - only the chart version needs updating.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | ☐    |
| A3 | Confirm verification resumes after the upgrade.                | `watch -n 10 'curl -s http://$BLOCK_NODE_HOST:16007/metrics \| grep blocknode_verification_blocks_verified_total'`. The counter should increase. Go to Phase 4. If `UNSUPPORTED_STREAM_FORMAT` failures continue after upgrade, the block stream from an upstream CN may be malformed rather than a BN version gap - escalate to Hashgraph Engineering with dump files and the block range.                                                                                                                                                                                                                                                                                                      | ☐    |

---

### Category B - Missing verification data

The BN does not have the TSS data or RSA public keys it needs to verify the current proof type.
This can be transient (TSS adoption transaction has not propagated yet to this BN's application
state) or persistent (verification data stuck after a restart).

| #  |                                      Action                                       |                                                                                                                                                                                                                                                                                                                                        Notes                                                                                                                                                                                                                                                                                                                                         | Done |
|----|-----------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| B1 | Check whether the failure is occurring within the TSS warm-up window.             | Check pod age: `kubectl get pod -n $NAMESPACE $RELEASE-block-node-server-0 -o jsonpath='{.status.containerStatuses[0].state.running.startedAt}'`. If the pod started within the last 5 minutes, this may be a transient - TSS data takes a moment to propagate from application state after startup or a roster change. Confirm TSS loaded: `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \| grep -i "tss library"`. Expected: `"TSS library ... ready to verify block proofs"`. If you see `"cannot verify block proofs in this process"` instead, **go to Category E**. If the pod is young (< 5 minutes), wait 2 minutes before proceeding to B2. | ☐    |
| B2 | If failures persist beyond 5 minutes, restart the BN to reload application state. | `kubectl rollout restart statefulset/$RELEASE-block-node-server -n $NAMESPACE`. Wait for the pod to become ready: `kubectl rollout status statefulset/$RELEASE-block-node-server -n $NAMESPACE --timeout=300s`                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | ☐    |
| B3 | Confirm `MISSING_VERIFICATION_DATA` failures have stopped.                        | `curl -s http://$BLOCK_NODE_HOST:16007/metrics \| grep 'verification_proof_total.*failure'`. If failures continue after the restart: for TSS blocks, escalate to Hashgraph Engineering (TSS adoption transaction may not have settled on this BN's state). For RSA WRB blocks (pre-TSS cutover RSA Write-ahead Record Blocks), check the BN logs for `"No address book era covers block N"` - if present, the `rsa-address-book-history.json` file does not cover that block range; escalate to Engineering to update it. If failures have stopped: proceed to Phase 4.                                                                                                              | ☐    |

---

### Category C - Bad block proof

`BAD_BLOCK_PROOF` means the cryptographic proof for a block failed verification. The action depends
on how many blocks are affected and whether peer BNs agree.

> **If all BNs report `BAD_BLOCK_PROOF` for the same blocks:** the blocks may be malformed at the
> source. **Do not reset.** Escalate to Hashgraph Engineering immediately with the dump files and
> the exact block range.

| #  |                                                 Action                                                  |                                                                                                                                                                                                                                                                                                                                                                         Notes                                                                                                                                                                                                                                                                                                                                                                         | Done |
|----|---------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| C1 | Determine whether this is a single block or a persistent pattern.                                       | `watch -n 5 'curl -s http://$BLOCK_NODE_HOST:16007/metrics \| grep "verification_proof_total.*failure"'`. If the `tss` or `rsa` failure counter stops increasing after 1-2 blocks and `verification_blocks_verified_total` resumes climbing, the failure was transient (another CN provided the block correctly). Monitor for 5 minutes. No further action needed if it self-resolves. If the failure counter keeps increasing: continue to C2.                                                                                                                                                                                                                                                                                                       | ☐    |
| C2 | Check whether `rsa_roster_mismatch_total` is rising (RSA-specific).                                     | A rising `blocknode_rsa_roster_mismatch_total_total` means RSA block proofs contain signatures from node IDs not in the loaded address book era (`rsa-address-book-history.json`). **This does not by itself cause `BAD_BLOCK_PROOF`** - the block still verifies if at least one valid signature from a known node exists. Monitor `blocknode_verification_proof_total_total{proof_type="rsa",result="success"}` alongside: if RSA success is also rising, the mismatches are noise. If RSA success is flat and RSA failure is rising, the address book era file does not cover the current block range - escalate to Engineering to update `rsa-address-book-history.json`. A restart does not fix this; the address book era file must be updated. | ☐    |
| C3 | If failures persist across many blocks and only this BN is affected, reset and backfill from a peer BN. | The BN's stored proof verification keys or TSS bootstrap data may have diverged from the network. Follow [Resetting and Upgrading the Block Node](../resetting-and-upgrading-the-block-node.md) to reset block storage and restart. The BN will backfill from a peer Tier-1 BN. Note the block at which failures started: _______. After reset, confirm backfill catches up past that block without `BAD_BLOCK_PROOF` errors. **If failures resume after backfill:** the blocks themselves may be malformed - escalate to Hashgraph Engineering with the block number range and dump files from Step 0. If backfill completes without failures: proceed to Phase 4.                                                                                   | ☐    |

---

### Category D - Data corruption (parse failures)

`UNABLE_TO_PARSE`, `MISSING_MANDATORY_ITEM`, or `MISSING_MANDATORY_FIELD` on persisted blocks
indicates corrupt bytes on disk. The affected blocks cannot be served or verified. Reset and
backfill from a peer BN, or restore from a snapshot.

| #  |                                       Action                                       |                                                                                                                                                                                                                                                                                                                                Notes                                                                                                                                                                                                                                                                                                                                 | Done |
|----|------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| D1 | Determine the range of corrupt blocks.                                             | `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \| grep -E "UNABLE_TO_PARSE\|MISSING_MANDATORY" \| head -30`. First corrupt block: _______ / Last corrupt block (so far): _______                                                                                                                                                                                                                                                                                                                                                                                                                                                      | ☐    |
| D2 | Check whether peer BNs have the blocks intact.                                     | `grpcurl -plaintext -import-path ~/bn-proto -proto block-node/api/node_service.proto -d '{}' <PEER_BN_HOST>:$BN_STATUS_PORT org.hiero.block.api.BlockNodeService/serverStatus`. If the peer's `lastAvailableBlock` covers the corrupt range: proceed to Option A. If no peer has the blocks: proceed to Option B.                                                                                                                                                                                                                                                                                                                                                    | ☐    |
| D3 | **Option A** - Reset and backfill from peer (if peer BN covers the corrupt range). | Follow [Resetting and Upgrading the Block Node](../resetting-and-upgrading-the-block-node.md) to reset block storage. The BN will backfill the missing range automatically from the peer BN. Monitor: `watch -n 30 'curl -s http://$BLOCK_NODE_HOST:16007/metrics \| grep backfill_pending_blocks'`. `backfill_pending_blocks` should decrease to 0. Typical backfill rates are 500-2000 blocks per minute; a gap of 100,000 blocks takes roughly 1-3 hours. **If parse failures resume after backfill completes:** the blocks may be corrupted at the peer as well - escalate to Hashgraph Engineering. If backfill completes without failures: proceed to Phase 4. | ☐    |
| D4 | **Option B** - Restore from snapshot (if no peer covers the corrupt range).        | Engage PB-01: Bootstrap from Snapshot *(planned - [#3783](https://github.com/hiero-ledger/hiero-block-node/issues/3783))*. After the restore, confirm the BN ingests blocks past the previously corrupt range without parse errors. **If failures resume after restore:** escalate to Hashgraph Engineering with the snapshot timestamp and the block range. If the restore completes without failures: proceed to Phase 4.                                                                                                                                                                                                                                          | ☐    |

---

### Category E - TSS library startup failure

The TSS native library could not be loaded during BN startup. The BN shut itself down. Verification
cannot run without the library. This is usually an environment issue (temp directory not writable or
not executable, JVM architecture mismatch).

| #  |                                   Action                                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Done |
|----|-----------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| E1 | Confirm the exact error from BN startup logs.                               | `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \| grep -A3 "TSS library"`. **If the output is empty**, the current container restarted after the crash; add `--previous` to read the last crashed container's logs: `kubectl logs --previous -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \| grep -A3 "TSS library"`. The log includes the `java.io.tmpdir` path. Check whether that directory is writable and executable inside the container: `kubectl exec -n $NAMESPACE $RELEASE-block-node-server-0 -- sh -c 'ls -la $(dirname $(mktemp)) && touch /tmp/.bntest && rm /tmp/.bntest && echo OK'`. If the test fails: the container's temp directory is not writable - check `securityContext` and `fsGroup` in your Helm values and adjust the `JAVA_TOOL_OPTIONS` env var to set `-Djava.io.tmpdir` to a writable path. If `java.io.tmpdir` is writable but the library still fails: escalate to Engineering. | ☐    |
| E2 | After fixing the environment, restart the BN and confirm the library loads. | `kubectl rollout restart statefulset/$RELEASE-block-node-server -n $NAMESPACE`. Then check: `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \| grep "TSS library"`. Expected: `TSS library <version> ready to verify block proofs`. If it still fails: escalate. If the library loads successfully: proceed to Phase 4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | ☐    |

---

## Phase 4 - Verify and Close

> Verification is restored when the failure counters stop rising and the success counter resumes
> climbing. Confirm across all key signals before closing the incident.

| # |                                                                     Action                                                                      |                                                                                                                                                                                       Notes                                                                                                                                                                                        | Done |
|---|-------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1 | Confirm `verification_blocks_failed` has stopped increasing.                                                                                    | `watch -n 10 'curl -s http://$BLOCK_NODE_HOST:16007/metrics \| grep blocknode_verification_blocks_failed_total'`. Value should be frozen. Any continued increase: return to Phase 1 and reclassify.                                                                                                                                                                                | ☐    |
| 2 | Confirm `verification_blocks_verified` is increasing at a steady rate.                                                                          | `watch -n 10 'curl -s http://$BLOCK_NODE_HOST:16007/metrics \| grep blocknode_verification_blocks_verified_total'`. Should increment every 2 seconds (one block per consensus round).                                                                                                                                                                                              | ☐    |
| 3 | Confirm no new dump files are being written.                                                                                                    | `kubectl exec -n $NAMESPACE $RELEASE-block-node-server-0 -- ls -lt /opt/hiero/block-node/verification/dumps/ \| head -5`. No new files in the last 2 minutes = verification is healthy. If the directory does not exist (`ls: cannot access...`), dumps were not enabled in Phase 3 Step 0 - skip this step.                                                                       | ☐    |
| 4 | Confirm BN block range is advancing.                                                                                                            | If the BN is in Kubernetes, forward the gRPC port first: `kubectl port-forward -n $NAMESPACE <pod-name> $BN_STATUS_PORT:$BN_STATUS_PORT`. Then: `grpcurl -plaintext -import-path ~/bn-proto -proto block-node/api/node_service.proto -d '{}' $BLOCK_NODE_HOST:$BN_STATUS_PORT org.hiero.block.api.BlockNodeService/serverStatus`. `lastAvailableBlock` should increase every ~2 s. | ☐    |
| 5 | If you made routing changes during recovery (redirecting CNs to a standby BN or adjusting load balancer targets), restore the original routing. | Verify that CNs are publishing to this BN: `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \| grep -i "publisher\|connected"`. Update the public status page.                                                                                                                                                                                        | ☐    |

---

## Reference

### Key metrics

|                            Metric                             |                          Alert condition                          |
|---------------------------------------------------------------|-------------------------------------------------------------------|
| `verification_blocks_failed`                                  | Page if increasing and `verification_blocks_verified` is not      |
| `verification_blocks_error`                                   | Page on any non-zero value (internal BN errors, not proof issues) |
| `verification_proof_total{proof_type="tss",result="failure"}` | Alert if sustained (isolated failures may be transient)           |
| `verification_proof_total{proof_type="rsa",result="failure"}` | Alert if sustained                                                |
| `rsa_roster_mismatch_total`                                   | Alert on any increase after a roster change                       |

Prometheus appends `_total` to counter names and the BN prepends `blocknode_`. Use
`blocknode_<metric_name>_total` in PromQL. Note: `verification_proof_total` already contains
`_total` in its key name, so its full Prometheus name is `blocknode_verification_proof_total_total`.

### Failure type quick-reference

|     Failure type logged     | Category |                                           What it means                                           |
|-----------------------------|----------|---------------------------------------------------------------------------------------------------|
| `UNSUPPORTED_ITEM_TYPE`     | A        | BN cannot process a new block item type - upgrade required                                        |
| `UNSUPPORTED_STREAM_FORMAT` | A        | Block hashed with non-SHA-256 algorithm or duplicate mandatory item - upgrade or corrupted stream |
| `UNKNOWN_ERROR`             | A/D      | Unexpected internal error during verification - check dump files and logs                         |
| `MISSING_VERIFICATION_DATA` | B        | TSS data or RSA public keys not loaded - restart to reload                                        |
| `BAD_BLOCK_PROOF`           | C        | Cryptographic proof rejected - may be transient or data issue                                     |
| `UNRECOGNIZED_PROOF_TYPE`   | C        | Proof type not recognized - may indicate version mismatch                                         |
| `UNABLE_TO_PARSE`           | D        | Block bytes cannot be parsed - data corruption                                                    |
| `MISSING_MANDATORY_ITEM`    | D        | Required block item absent - data corruption                                                      |
| `MISSING_MANDATORY_FIELD`   | D        | Required protobuf field is null - data corruption                                                 |
| `CANCELLED`                 | —        | Session evicted from active buffer (normal under high load) - not corruption                      |
| `CANCELLED_INCOMPLETE`      | —        | Publisher abandoned block before it was complete - not corruption                                 |
| TSS library startup failure | E        | Native library failed to load - environment issue                                                 |

### Pre-incident configuration record

Fill this in before you need it.

- Peer Tier-1 BN host (for Category C/D): _______
- Helm chart version currently deployed: _______
- `VERIFICATION_DUMP_ENABLED` current value: _______
- `VERIFICATION_DUMP_DIRECTORY_PATH` (if non-default): _______
- On-call Hashgraph Engineering contact: _______

### Post-incident checklist

After the incident is closed: archive logs and dump files, run a post-mortem within 48 hours, and
address:

|                  Finding                   |                                  Remediation                                  |
|--------------------------------------------|-------------------------------------------------------------------------------|
| `MISSING_VERIFICATION_DATA` on restart     | Investigate TSS adoption transaction propagation timing; may need longer wait |
| BN was behind network block stream version | Shorten upgrade cycle; track hiero-block-node releases                        |
| Data corruption from disk                  | Check storage hardware health; increase snapshot frequency                    |
| `BAD_BLOCK_PROOF` from single CN           | Report to Hashgraph Engineering - that CN may be producing malformed blocks   |
| Dump directory was full or not writable    | Pre-provision dump directory with adequate space and correct permissions      |

---

[Back to DR Playbook Index](./README.md)
