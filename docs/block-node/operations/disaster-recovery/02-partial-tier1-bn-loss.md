# DR Playbook: Single or Partial Tier-1 Block Node Loss

> **Summary:** One or more Tier-1 Block Nodes are down. Consensus Nodes (CNs) auto-failover to
> the remaining BNs and the network continues processing transactions. Recover the failed BN(s)
> before CN failover capacity is exhausted and backpressure begins. The critical decision in this
> playbook is whether K8s is already recovering the pod (wait and monitor) or whether manual
> intervention is required.

|                      |                                                                                                                                                                                       |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Playbook ID**      | PB-02                                                                                                                                                                                 |
| **Severity**         | SEV-2 (degraded redundancy; network still processing transactions)                                                                                                                    |
| **Primary executor** | Operator + DevOps                                                                                                                                                                     |
| **Escalate to**      | Hashgraph Engineering if CN backpressure state reaches value 3 on any CN - immediately engage PB-04 *(draft - [#3786](https://github.com/hiero-ledger/hiero-block-node/issues/3786))* |

---

## Is this your doc?

Check these without running any commands:

- One or more BN pods are not `Running` and `Ready` (CrashLoopBackOff, Error, Pending, or Terminating)
- OR BN `/healthz/livez` is returning 503 or not responding
- OR you received an alert on `blocknode_publisher_open_connections` dropping to 0 on a BN
- The network is still processing transactions (not fully halted)

**Not your doc if:**

- All transactions are halted or multiple CNs show `blockStream_buffer_backPressureState = 3` -
  engage PB-04 *(draft - [#3786](https://github.com/hiero-ledger/hiero-block-node/issues/3786))* immediately
- BN is `Running` and `Ready` but reporting block verification errors - see PB-06 *(draft - [#3788](https://github.com/hiero-ledger/hiero-block-node/issues/3788))*
- Mirror Node is behind but all BNs are healthy - see PB-03
- One or more CNs cannot publish (CN-side issue, BNs are healthy) - see PB-05

---

## Before starting

> **Open the incident bridge.** At SEV-2 this can be lightweight initially, but assign an
> incident commander immediately. If CN backpressure begins building (Phase 2 Step 1), promote
> to SEV-1 and engage PB-04 — do not wait for full escalation before opening the bridge.
>
> Assign a **communication lead** to track status externally while technical work continues.

**These commands assume a Kubernetes deployment** using the Hiero Block Node Helm chart. For
bare-metal or VM deployments, substitute `systemctl status block-node-server` for pod status
checks and use the host IP directly instead of port-forwarding.

---

## Phase 1 - Diagnose

> **Do not skip these before acting:**
> - Do not restart a CN before checking CN backpressure state — a CN restart during active
> backpressure destroys buffered blocks if persistence is disabled
> - Do not restart all BNs simultaneously — you will remove all available BN capacity from the
> network while the pods restart
>
> **If this BN is the only Tier-1 BN in your routing config** (no other BNs in `block-nodes.json`
> or the on-chain BN registry), CNs have nowhere to failover and backpressure will begin
> immediately. In that case, skip this playbook and engage
> PB-04 *(draft - [#3786](https://github.com/hiero-ledger/hiero-block-node/issues/3786))* directly.

**Get pod names** (Kubernetes deployments):

```bash
kubectl get pods -n $NAMESPACE -l app.kubernetes.io/instance=$RELEASE -o wide
```

| # |                               Action                                |                                                                                                                                                                                                Notes                                                                                                                                                                                                | Done |
|---|---------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1 | Confirm which BN pods are down and how many restarts they have had. | `kubectl describe pod -n $NAMESPACE $RELEASE-block-node-server-0 \| grep -E "Status:\|Restart Count:\|Reason:\|Last State:"`. Record: pods affected = _______ / restart count = _______ / last termination reason = _______. A restart count of 1-3 with the pod currently `Running` may mean K8s already recovered it — check Phase 4 to verify. A restart count that keeps climbing = Category B. | ☐    |
| 2 | Check CN backpressure state. This is the most critical check.       | Ask the CN operator team (or check directly if you have access): `curl -s http://<CN_HOST>:<CN_METRICS_PORT>/metrics \| grep blockStream_buffer_backPressureState`. Value `0` = no backpressure · Value `3` = active/halted. **If any CN shows value 3: STOP. Promote this to SEV-1 and engage PB-04 immediately.** Record current state across all CNs: _______                                    | ☐    |

**Step 3 - Classify your failure.** Match the pod state you recorded in Step 1: Category: _______ ☐

|                         Pod state in Step 1                          | Category |           Typical time to recover           |                      Escalate immediately if...                      |
|----------------------------------------------------------------------|----------|---------------------------------------------|----------------------------------------------------------------------|
| Pod is `Running` and `Ready`, restartCount increased then stabilized | —        | Already recovered - go to Phase 4 to verify | BN not accepting CN connections (publisher_open_connections still 0) |
| Pod is `CrashLoopBackOff` with ≤ 3 restarts, previously stable       | **A**    | 5-15 minutes (K8s handling restart)         | Restart count exceeds 5 or pod does not reach Running                |
| Pod is `CrashLoopBackOff` with > 5 restarts or never reached Running | **B**    | 30-90 minutes (manual root-cause fix)       | CN backpressure state rises to 3 on any CN                           |
| Pod is `Pending` (not scheduling)                                    | **B**    | 30-90 minutes (node or resource issue)      | Pod remains Pending after 10 minutes                                 |
| Pod is `Running` but liveness probe failing (K8s about to kill it)   | **B**    | 30-60 minutes (deadlock or startup issue)   | Pod keeps cycling without reaching Ready                             |

---

## Phase 2 - Monitor

> A BN that is down stops acknowledging blocks from its connected CNs. Those CNs will failover to
> other BNs if available, but if failover capacity is exhausted, CN buffers fill and backpressure
> activates. Monitor CN buffer state continuously throughout Phase 3 — it is the earliest signal
> that this incident is escalating to SEV-1.

| # |                          Action                           |                                                                                                                                                                                                                                    Notes                                                                                                                                                                                                                                     | Done |
|---|-----------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1 | Watch CN backpressure and buffer saturation continuously. | `watch -n 10 'curl -s http://<CN_HOST>:<CN_METRICS_PORT>/metrics \| grep -E "blockStream_buffer_backPressureState\|blockStream_buffer_saturation"'`. macOS: `while true; do curl -s http://<CN_HOST>:<CN_METRICS_PORT>/metrics \| grep -E "backPressureState\|saturation"; sleep 10; done`. Assign one operator to watch and call out any change every 5 minutes. **If `backPressureState` reaches 3 on any CN: stop recovery, promote to SEV-1, engage PB-04 immediately.** | ☐    |
| 2 | Preserve BN logs before any manual restart.               | `kubectl logs --previous -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server > /tmp/bn_incident_$(date +%Y%m%d_%H%M%S).log`. If the pod is in its first crash (`--previous` returns no logs), also save current container logs: `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server >> /tmp/bn_incident_$(date +%Y%m%d_%H%M%S).log`. Share with Engineering when escalating.                                                           | ☐    |

---

## Phase 3 - Recover

Identify your failure category from Phase 1 Step 3 and jump to that section.

### Category A - K8s handling the restart

K8s will restart the pod automatically (StatefulSet restart policy). Your role is to monitor and
confirm recovery, not to intervene.

| #  |                                Action                                 |                                                                                                                                                                                                                                                                                          Notes                                                                                                                                                                                                                                                                                          | Done |
|----|-----------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| A1 | Watch the pod restart and wait for it to reach `Running` and `Ready`. | `kubectl get pod -n $NAMESPACE $RELEASE-block-node-server-0 -w`. K8s uses exponential backoff for restarts: 10s → 20s → 40s → 80s → 160s → 300s (cap). After a 5-restart spiral the pod may wait up to 5 minutes between attempts. If you need faster recovery and the pod is stuck in a long backoff, delete the pod to trigger an immediate restart: `kubectl delete pod -n $NAMESPACE $RELEASE-block-node-server-0` — K8s recreates it immediately (StatefulSet). **If the pod does not reach `Running` after 5 restarts or keeps failing after a manual delete:** go to Category B. | ☐    |
| A2 | Confirm the BN started successfully.                                  | `kubectl logs -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \| grep -i "started blocknode server"`. Expected: `"Started BlockNode Server : State=RUNNING HistoricBlockRange=..."`. Then proceed to Phase 4.                                                                                                                                                                                                                                                                                                                                                           | ☐    |

---

### Category B - Persistent failure requiring manual fix

The pod is not recovering on its own. Identify the root cause, then apply the matching fix.

**B1** - Describe the pod to identify the termination reason. ☐

```bash
kubectl describe pod -n $NAMESPACE $RELEASE-block-node-server-0
```

Look for `Last State: Terminated` → `Reason:`. `OOMKilled` = memory limit exceeded. `Error` = non-zero exit (check logs). Check `Events:` for scheduling failures (insufficient resources, PVC not bound, node taint).

Record reason: _______

**B2** - Read crash logs and identify the root cause. ☐

```bash
kubectl logs --previous -n $NAMESPACE $RELEASE-block-node-server-0 -c block-node-server \
  | grep -iE "error|exception|failed|cannot|shutting down|TSS library"
```

Match what you see to a root cause and apply the fix:

- **TSS library failure** - log contains `"cannot verify block proofs in this process, shutting down"`\
  Fix: Go to **PB-06 *(draft - [#3788](https://github.com/hiero-ledger/hiero-block-node/issues/3788))* Category E**

- **OOMKilled** - B1 showed `OOMKilled` in `Reason:` (a `describe` field, not a log line)\
  Fix: Increase `resources.limits.memory` and set `JAVA_OPTS: '-Xmx<value>'` in `blockNode.config`

- **Storage / PVC** - B1 `Events:` shows mount failure or PVC Pending\
  Fix: `kubectl get pvc -n $NAMESPACE` (look for `STATUS: Pending` or `Lost`), then\
  `kubectl describe pvc -n $NAMESPACE <pvc-name>` to diagnose.\
  Common causes: storage class unavailable, PV capacity exhausted, node affinity mismatch.

- **Config error** - log shows unrecognized property or missing required value\
  Fix: Correct the offending key in your Helm values

- **No match** - Escalate to Hashgraph Engineering with the log file from Phase 2 Step 2

Record root cause: _______

**B3** - After applying the fix, redeploy and wait for the pod to become ready. ☐

```bash
helm upgrade $RELEASE oci://ghcr.io/hiero-ledger/hiero-block-node/block-node-server \
  --version <chart-version> -n $NAMESPACE -f <path-to-your-values.yaml>
```

Wait: `kubectl rollout status statefulset/$RELEASE-block-node-server -n $NAMESPACE --timeout=300s`

If pod does not reach `Running` and `Ready` within 5 minutes: re-read logs (`--previous`) and re-diagnose B1/B2.

**If CN backpressure reaches 3 at any point: stop and engage PB-04.**

---

## Phase 4 - Verify and Close

> The BN is recovered when it is running, CNs have reconnected, and the block range is advancing.
> Confirm all five signals before closing the incident.

| # |                     Action                     |                                                                                                                                                                                                                                                                                                                                                                                                                                         Notes                                                                                                                                                                                                                                                                                                                                                                                                                                         | Done |
|---|------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1 | Confirm the BN pod is `Running` and `Ready`.   | `kubectl get pod -n $NAMESPACE $RELEASE-block-node-server-0`. Both `READY` column (e.g. `1/1`) and `STATUS` (`Running`) must be true. If `READY` is `0/1` and the pod is `Running`: readiness probe is still failing - check startup logs.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | ☐    |
| 2 | Confirm the BN health endpoint returns 200.    | Forward the health port if needed (run in a separate terminal or background with `&`): `kubectl port-forward -n $NAMESPACE <pod-name> 40983:40983 &`. Then: `curl -s -o /dev/null -w "%{http_code}" http://localhost:40983/healthz/livez`. Expected: `200`. `503` = BN process running but not healthy (check logs). `000` = port not reachable (port-forward failed or not running — check the port-forward output).                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | ☐    |
| 3 | Confirm CNs are reconnecting to this BN.       | `watch -n 10 'curl -s http://$BLOCK_NODE_HOST:16007/metrics \| grep blocknode_publisher_open_connections'`. macOS: `while true; do curl -s http://$BLOCK_NODE_HOST:16007/metrics \| grep blocknode_publisher_open_connections; sleep 10; done`. Should increase from 0 as CNs detect the BN is back and reconnect. CNs reconnect automatically within their configured retry interval - no manual action needed. If connections stay at 0 after 5 minutes: check BN logs for publisher connection errors and confirm the BN is reachable on port `$BN_GRPC_PORT` from the CN network.                                                                                                                                                                                                                                                                                                 | ☐    |
| 4 | Confirm the BN block range is advancing.       | Requires `grpcurl` and `~/bn-proto` (proto bundle tarball). If not set up: install `grpcurl` from [grpcurl releases](https://github.com/fullstorydev/grpcurl/releases) and extract the proto bundle from the [hiero-block-node releases page](https://github.com/hiero-ledger/hiero-block-node/releases) — see [api-quickstart](../../api-quickstart.md) for the exact commands. Forward the gRPC port if needed: `kubectl port-forward -n $NAMESPACE <pod-name> $BN_GRPC_PORT:$BN_GRPC_PORT`. Then: `grpcurl -plaintext -import-path ~/bn-proto -proto block-node/api/node_service.proto -d '{}' $BLOCK_NODE_HOST:$BN_GRPC_PORT org.hiero.block.api.BlockNodeService/serverStatus`. `lastAvailableBlock` should increase every ~2 s. If it is not advancing after 5 minutes, the BN may be backfilling; check `blocknode_backfill_pending_blocks` and wait for backfill to complete. | ☐    |
| 5 | Confirm CN backpressure state has not changed. | Re-run the CN backpressure check from Phase 1 Step 2. All CNs should still show `0`. If any CN reached a non-zero value during recovery: file a post-incident note and investigate CN buffer headroom. Update the public status page.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | ☐    |

---

## Reference

### Key metrics

|                        Metric                        |                        Alert condition                        |
|------------------------------------------------------|---------------------------------------------------------------|
| `publisher_open_connections`                         | Alert if drops to 0 (no CNs connected to this BN)             |
| `publisher_active_handlers`                          | Alert if falls while `publisher_open_connections` is non-zero |
| `health_requests{endpoint="health",result="failed"}` | Alert on any non-zero value (health probe failures)           |
| `blocknode_backfill_pending_blocks`                  | Informational: tracks backfill progress after recovery        |

Prometheus prefix: `blocknode_`. Gauges (like `publisher_open_connections`) do not get a `_total`
suffix. Counters do (e.g. `blocknode_health_requests_total`).

### PB-04 escalation boundary

|                         Condition                          |                      Action                       |
|------------------------------------------------------------|---------------------------------------------------|
| `blockStream_buffer_backPressureState = 3` on any CN       | Stop. Promote to SEV-1. Engage PB-04 immediately. |
| `blockStream_buffer_saturation` rising above 80% on any CN | Accelerate recovery. Prepare to engage PB-04.     |
| All Tier-1 BNs down simultaneously                         | Skip PB-02 entirely. Engage PB-04 immediately.    |

### Pre-incident configuration record

Fill this in before you need it.

- Peer Tier-1 BN host (for grpcurl checks): _______
- Helm chart version currently deployed: _______
- CN metrics endpoint (for backpressure checks): _______
- On-call Hashgraph Engineering contact: _______

### Post-incident checklist

After the incident is closed: archive logs, run a post-mortem within 48 hours, and address:

|                      Finding                      |                               Remediation                               |
|---------------------------------------------------|-------------------------------------------------------------------------|
| Pod OOMKilled                                     | Review heap and memory limits; add memory usage alert                   |
| TSS library startup failure                       | Investigate container temp directory permissions (see PB-06 Category E) |
| Storage mount failure (PVC Pending)               | Review storage class provisioner; add PVC status alert                  |
| CN backpressure rose above 0 during recovery      | Reduce recovery time target; consider increasing Tier-1 BN redundancy   |
| BN pod did not restart automatically within 5 min | Review K8s resource limits and node capacity for the BN StatefulSet     |

---

[Back to DR Playbook Index](./README.md)
