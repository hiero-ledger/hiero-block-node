# Block Node Troubleshooting

## Overview

This page is a troubleshooting runbook for [**Hiero Block Nodes**](https://github.com/hiero-ledger/hiero-block-node). It assumes you are an operator with SSH or kubectl access to the node and the appropriate Prometheus / Grafana UI.

**Use it when:**

- Block ingest or backfill stalls
- Subscribers / mirror nodes cannot connect
- Disk or storage is under pressure
- Metrics or dashboards look wrong

---

### 1. Observability & Diagnostics

Block nodes are generally robust, but like any distributed system component, operators occasionally encounter issues related to networking, storage, synchronization, or configuration. The reference implementation includes comprehensive logging, CLI tools, and Prometheus / Grafana metrics to identify and resolve problems quickly.

#### 1.1 Logs & diagnostics

- **Structured logs**: JSON-formatted logs are written to stdout / stderr (easily ingested by Loki, ELK, Splunk, etc.).
- **Log levels**: Controlled via [values.yaml](https://github.com/hiero-ledger/hiero-block-node/blob/main/charts/block-node-server/values.yaml#L187): `logs.level` (TRACE, DEBUG, INFO, WARN, ERROR).
- **Key log tags to grep**:
    - `BlockStreamPublishService` : incoming blocks from consensus nodes
    - `BlockVerification` : signature / proof failures


#### 1.2 Prometheus metrics & monitoring

The block node exposes a rich set of Prometheus metrics on `/metrics` (default port 16007). Key Grafana dashboards are available in the official [Hiero Block Node `dashboards/` folder.](https://github.com/hiero-ledger/hiero-block-node/tree/main/charts/block-node-server/dashboards)

Block Node metric labels are configured by default to be prepended with `hiero_block_node_`

| **Category** | **Important Metrics** | **What to Watch For** |
| --- | --- | --- |
| Block Ingestion | `block_ingest_total`, `block_ingest_errors`, `block_ingest_lag_seconds` | Sudden drop in ingest rate or rising lag → network issues or partitions |
| Verification | `block_verification_duration_seconds`, `block_verification_failures_total` | Spikes in failures → bad blocks, signature issues, or clock skew |
| Storage | `storage_bytes_total`, `storage_iops`, `storage_latency_seconds` | High I/O wait or latency → disk bottleneck |
| State Application | `state_apply_duration_seconds`, `state_apply_items_per_second` | Slow apply → CPU or disk bound |
| gRPC Services | `grpc_server_handled_total`, `grpc_server_msg_received_total` (per service) | Zero on publish / subscribe → connectivity issue |
| Stream Subscribers | `active_subscriptions`, `subscription_items_sent_total` | Dropped clients → backpressure, filtering bugs, or TLS issues |
| Snapshot | `snapshot_creation_duration_seconds`, `snapshot_last_block` | Stale snapshots → manual trigger or config review needed |
| System | `process_cpu_seconds_total`, `go_goroutines`, `fd_open` | Resource exhaustion |

---

### 2. Issue Runbooks (Observe → Diagnose → Fix → Verify)

Use the toggles below to explore runbooks during incidents. Each follows a consistent pattern:

- **Triage** – confirm what is actually broken
- **Logs / Metrics / Configuration** – narrow down root cause
- **Resolution** – apply fixes
- **Verification** – confirm recovery

### Runbooks

<details>
<summary>🚫 <strong>Block Node not receiving new blocks</strong></summary>

<aside>
💡

Use this runbook when `block_ingest_lag_seconds` is increasing and logs show little or no publish activity for new blocks.

</aside>

1. **Triage**
    - Confirm symptoms:
        - `block_ingest_lag_seconds` steadily increasing.
        - Ingest-related metrics (for example, `block_ingest_total`) flat or near-zero.
        - Logs show no recent block publish events.
    - Check node health:
        - Verify process is running and not crashlooping.
        - Confirm CPU / memory are not obviously saturated.
    - From a trusted host, test connectivity to the block node:
        - `nc \<IP_OF_BLOCK_NODE\> 50211 -vz`
            - Success: TCP reachability is OK.
            - Failure: suspect firewall, security group, or local iptables.
    - If `nc` fails:
        - Verify host firewall rules on both sides (block node and CN).
        - Check any intermediate firewalls / load balancers for drops.
        - Confirm the correct IP and port for the CN endpoint.
2. **Logs**
    - Search for:
        - Connection-related errors to consensus nodes.
        - TLS handshake failures.
        - Repeated reconnect attempts or backoff warnings.
3. **Metrics**
    - Confirm:
        - `block_ingest_lag_seconds` is rising.
        - Any connection error / retry metrics (if present) are non-zero.
    - Correlate with time of any infrastructure changes (deploys, config updates, firewall changes).
4. **Resolution**
    - Fix firewall / security group rules on gossip / ingest port (default `50211`, or configured port).
    - Correct TLS or mutual-auth configuration between CN and block node.
    - Restart the block node if needed once connectivity and TLS are corrected.
5. **Verification**
    - Confirm `block_ingest_lag_seconds` returns toward zero.
    - `block_ingest_total` increases steadily.
    - Logs show continuous block publish / ingest activity.

</details>

<details>
<summary>🔌 <strong>Subscribers/mirror nodes cannot connect</strong></summary>

1. **Triage**
    - Confirm symptoms from client side:
        - gRPC connection failures, timeouts, or TLS errors when subscribing.
        - Clients repeatedly reconnecting or backing off.
    - Confirm on the block node:
        - Service is running and listening on the expected gRPC port.
        - No obvious CPU / memory starvation.
2. **Network and endpoint checks**
    - From a trusted host (for example, mirror node):
        - `nc \<IP_OF_BLOCK_NODE\> \<GRPC_PORT\> -vz`
            - Success: TCP reachability is OK.
            - Failure: suspect firewall, security group, or local iptables.
    - Verify:
        - Correct advertised hostname / IP and port in block node config.
        - DNS or load balancer is pointing to the active node.
3. **TLS and auth**
    - Check client logs for:
        - `x509: certificate has expired or is not yet valid`.
        - Hostname mismatch between certificate and endpoint.
        - Unknown CA / trust failures.
    - On the block node, confirm:
        - TLS cert and key paths are correct and readable.
            - Certificates are not expired and chain is complete.
4. **Service configuration**
    - Ensure the publish / subscribe services are enabled in configuration (for example, `BlockStreamSubscribeService`).
    - Check any rate limits or `max-connections` settings that might be rejecting clients.
5. **Resolution**
    - Fix endpoint configuration (advertise address / port), update DNS or load balancer if needed.
    - Renew or reinstall TLS certificates and restart block node and / or clients.
    - Update firewall / security groups to allow gRPC traffic from subscribers.
6. **Verification**
    - Confirm clients successfully establish long-lived gRPC streams without continuous reconnects.
    - Metrics such as `active_subscriptions` and `subscription_items_sent_total` are stable or increasing.

</details>

<details>
<summary>💾 <strong>Disk full / out of space</strong></summary>

1. **Triage**
    - Confirm symptoms:
        - Node crashes, refuses new blocks, or logs I/O errors.
        - Alerts on storage utilization from Grafana / Prometheus.
    - Check capacity on the host:
        - `df -h` for filesystem usage.
        - `iostat`, `storage_bytes_total`, and `storage_latency_seconds` for pressure.
2. **Identify what is consuming space**
    - Determine which volume(s) hold block data, snapshots, and logs.
    - Inspect directories for unexpected growth (logs, temp, or backup folders).
    - *TODO: document the canonical data directory layout for block nodes (paths for blocks, snapshots, logs).*  ← GAP
3. **Short-term mitigation**
    - If safe, rotate / compress / prune logs.
    - If using partial-history nodes, enable or adjust pruning according to policy.
    - Temporarily add storage space to the affected volume if possible.
4. **Longer-term resolution**
    - For archival needs, migrate to a node with larger storage or externalize cold data.
    - Tune retention settings for blocks, snapshots, and logs.
    - Ensure monitoring alerts fire well before 100% usage (for example, at 75%, 85%, 95%).
5. **Verification**
    - Confirm `storage_bytes_total` and host `df -h` fall below alert thresholds.
    - Block ingest resumes normally and no further I/O errors appear in logs.

</details>

<details>
<summary>📈 <strong>Metrics endpoint not accessible</strong></summary>

1. **Triage**
    - Confirm that Grafana / Prometheus cannot scrape `/metrics` for this block node.
    - Attempt to curl from a nearby host:
        - `curl -v http://\<IP_OF_BLOCK_NODE\>:16007/metrics`
            - Success: HTTP 200 with Prometheus text output.
            - Failure: connection refused / timeout.
2. **Node-local checks**
    - Ensure metrics are enabled in the block node configuration (no `--metrics.disabled` or equivalent flag).
    - From the node itself:
        - `curl -v` [`](http://localhost:16007/metrics)http://localhost:16007/metrics`
            - If this fails, suspect local config or process issues.
    - *TODO: document exact config flags / env vars that control metrics exposure for the reference implementation.*  ← GAP
3. **Network and firewall**
    - If [localhost](http://localhost) works but remote scrape fails:
        - Check host firewall / security group rules for port `16007`.
        - Confirm any load balancers or service meshes expose the metrics port.
4. **Scraper configuration**
    - Verify Prometheus target configuration:
        - Correct job name, scheme (http / https), and port.
        - No incorrect path overrides.
5. **Resolution**
    - Enable metrics in config and restart the block node if required.
    - Open or adjust firewall / security rules for the metrics port.
    - Fix Prometheus / Grafana scrape configuration.
6. **Verification**
    - Confirm the Prometheus target is `UP` and `up\{job="block-node-metrics", instance="\<node\>"\} == 1`.
    - Grafana dashboards populate and scrape errors clear.

</details>

<details>
<summary>🕳️ <strong>Blocks are not being backfilled</strong></summary>

1. **Triage**
    - Confirm symptoms:
        - Gaps in block height or missing historical ranges expected to be present.
        - Backfill-related alerts firing.
    - Check logs for explicit backfill warnings or errors.
2. **Metrics and topology**
    - Review backfill metrics such as `hiero_block_node_backfill\*`:
        - Backfill rate, errors, and lag.
    - Confirm which upstream nodes this node is allowed to backfill from and that they are healthy.
3. **Configuration checks**
    - Verify `EARLIEST_MANAGED_BLOCK` (or equivalent) reflects the earliest block this node should own.
    - Verify `EARLIEST_BACKFILL_BLOCK` is set correctly relative to available upstream history.
    - Ensure backfill is enabled in the node configuration and not paused.
4. **Network and permissions**
    - Confirm this node can reach upstream block nodes over the required ports.
    - Check that any authentication / TLS between nodes is valid.
5. **Resolution**
    - Correct misconfigured earliest-block values (`EARLIEST_MANAGED_BLOCK`, `EARLIEST_BACKFILL_BLOCK`) and apply changes.
    - Restart the block node if configuration changes require it.
    - If upstream history is incomplete, coordinate with operators of archival nodes to provide the missing range.
6. **Verification**
    - Monitor backfill metrics until the node catches up to the desired block height.
    - Confirm local block height matches expected network height for the configured range.

</details>

---

### 3. Quick Reference Table (Summary)

The table below is a **summary-only quick reference**. Use the runbooks above for full diagnosis and remediation steps.

| **Issue** | **Symptoms** | **Diagnosis** | **Resolution** |
| --- | --- | --- | --- |
| Node not receiving new blocks | `block_ingest_lag_seconds` increasing, logs show no publish activity | Check firewall / TLS on gossip / ingest port (default `50211`), consensus node logs | Open inbound port, verify mutual TLS certs, ensure node is authorized / whitelisted by upstream CN |
| Subscribers / mirror nodes cannot connect | gRPC connection refused or TLS handshake errors | Wrong endpoint, expired cert, or service disabled in config | Verify advertise address / port, renew TLS certs, enable `BlockStreamSubscribeService` |
| Disk full / out of space | Node crashes or refuses new blocks | `df -h`, `storage_bytes_total` nearing limit | Prune old blocks (partial-history), expand volume, or migrate to archive node |
| Metrics endpoint not accessible | Grafana dashboards empty, Prometheus target `DOWN` | Port `16007` blocked or metrics disabled via config flag | Open port, enable metrics, fix Prometheus scrape job |
| Blocks not being backfilled | Log entries show backfill warnings, missing historical ranges | Check `hiero_block_node_backfill*` metrics, `EARLIEST_MANAGED_BLOCK` / `EARLIEST_BACKFILL_BLOCK` config | Fix earliest-block config, restart node if required, ensure healthy upstream archival source |

---

With proper alerting on the Prometheus metrics above (especially lag, ingest errors, and storage latency), most issues can be detected and resolved before they impact downstream mirror nodes or applications. The combination of detailed logs, built-in CLI validation tools, and comprehensive telemetry makes block nodes significantly easier to operate at scale than the previous centralized bucket model.

Contact Hashgraph for help: [`devops@hashgraph.com`](mailto:devops@hashgraph.com)
