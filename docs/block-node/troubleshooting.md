# Block Node Troubleshooting

## Overview

This page is a troubleshooting runbook for [**Hiero Block Nodes**](https://github.com/hiero-ledger/hiero-block-node). It assumes you are an operator with SSH or kubectl access to both the node and the appropriate Prometheus / Grafana UI.

**Use it when:**

- Block ingest or backfill stalls
- Subscribers / mirror nodes cannot connect
- Disk or storage is under pressure
- Metrics or dashboards look wrong

---

### 1. Observability & Diagnostics

<!-- trunk-ignore(markdownlint/MD013) -->
Block Nodes are generally robust, but like any distributed system component, operators occasionally encounter issues related to networking, storage, synchronization, or configuration. The reference implementation includes comprehensive logging, CLI tools, and Prometheus / Grafana metrics to identify and resolve problems quickly.

#### 1.1 Logs & diagnostics

- **Structured logs**: JSON-formatted logs are written to stdout / stderr (easily ingested by Loki, ELK, Splunk, etc.).
- **Log levels**: Controlled via [values.yaml](https://github.com/hiero-ledger/hiero-block-node/blob/main/charts/block-node-server/values.yaml#L187): `logs.level` (`ALL FINEST FINER FINE CONFIG INFO WARNING SEVERE OFF`).
- **Key log tags to grep**:
  - `BlockStreamPublishService` : incoming blocks from consensus nodes
  - `BlockVerification` : signature / proof failures

##### Example log lines

```text
block-node-server 2026-02-03 21:53:01.831+0000 INFO [org.hiero.block.node.backfill.BackfillFetcher getNewAvailableRange] Unable to reach node [BackfillSourceConfig[address=rfh01.previewnet.blocknode.hashgraph-devops.com, port=40840, priority=1, nodeId=0, name=, grpcWebclientTuning=null]], skipping
block-node-server 2026-02-03 21:53:01.831+0000 FINER [org.hiero.block.node.backfill.BackfillPlugin detectAndScheduleGaps] Nothing to backfill: startBound=[500] endCap=[1]
block-node-server 2026-02-03 21:53:03.748+0000 FINER [org.hiero.block.node.health.HealthServicePlugin handleLivez] Responded code 200 (OK) to liveness check
```

#### 1.2 Prometheus metrics & monitoring

The Block Node exposes a rich set of Prometheus metrics on `/metrics` (default port 16007). Key Grafana dashboards are available in the official [Hiero Block Node `dashboards/` folder.](https://github.com/hiero-ledger/hiero-block-node/tree/main/charts/block-node-server/dashboards)

All metrics are prefixed with `hiero_block_node` (for example, `hiero_block_node_publisher_block_items_received`). See the full list in the [metrics reference](https://github.com/hiero-ledger/hiero-block-node/blob/main/docs/block-node/metrics.md#metrics-by-plugin).

| **Category** | **Important Metrics** | **What to Watch For** |
| --- | --- | --- |
| Node State | `app_state_status`, `app_historical_oldest_block`, `app_historical_newest_block` | Non‑RUNNING status or stalled block height progress |
| Ingestion (Publisher) | `publisher_block_items_received`, `publisher_open_connections`, `publisher_stream_errors`, `publisher_receive_latency_ns` | Drop in receive rate, rising latency, or stream errors |
| Verification | `verification_blocks_failed`, `verification_blocks_error`, `verification_block_time`, `hashing_block_time` | Spikes in failures or verification time |
| Storage (Recent/Historic) | `files_recent_total_bytes_stored`, `files_historic_total_bytes_stored`, `files_recent_persistence_time_latency_ns` | Storage growth or persistence latency spikes |
| Messaging / Backpressure | `messaging_item_queue_percent_used`, `messaging_notification_queue_percent_used` | Queue saturation / backpressure |
| Subscribers | `subscriber_open_connections`, `subscriber_errors` | Dropped clients or streaming errors |
| Backfill | `backfill_blocks_backfilled`, `backfill_fetch_errors`, `backfill_pending_blocks`, `backfill_status` | Rising errors or stuck backfill status |
| Archive (S3) | `s3_archive_tasks_failed_total`, `s3_archive_latest_block` | Archival failures or stale archive height |

---

### 2. Issue Runbooks (Observe → Diagnose → Fix → Verify)

Use the toggles below to explore runbooks during incidents. Each follows a consistent pattern:

- **Triage** – confirm what is actually broken
- **Logs / Metrics / Configuration** – narrow down root cause
- **Resolution** – apply fixes
- **Verification** – confirm recovery

### Runbooks

<details>
<summary>🚫 **Block Node not receiving new blocks**</summary>

> **Tip:** Use this runbook when ingest appears stalled and logs show little or no publish activity for new blocks.

1. **Triage**
    - Confirm symptoms:
        - `publisher_block_items_received` flat or near-zero.
        - `publisher_open_connections` dropping toward zero.
        - Logs show no recent block publish events.
    - Check node health:
        - Verify process is running and not crashlooping.
        - Confirm CPU / memory are not obviously saturated.
    - From a trusted host, test connectivity to the Block Node:
        - `nc <IP_OF_BLOCK_NODE> 50211 -vz`
            - Success: TCP reachability is OK.
            - Failure: suspect firewall, security group, or local iptables.
    - If `nc` fails:
        - Verify host firewall rules on both sides (Block Node and CN).
        - Check any intermediate firewalls / load balancers for drops.
        - Confirm the correct IP and port for the CN endpoint.
2. **Logs**
    - Search for:
        - Connection-related errors to consensus nodes.
        - TLS handshake failures.
        - Repeated reconnect attempts or backoff warnings.
3. **Metrics**
    - Confirm:
        - `publisher_block_items_received` has stalled.
        - `publisher_stream_errors` is non-zero.
    - Correlate with time of any infrastructure changes (deploys, config updates, firewall changes).
4. **Resolution**
    - Fix firewall / security group rules on gossip / ingest port (default `50211`, or configured port).
    - Correct TLS or mutual-auth configuration between CN and Block Node.
    - Restart the Block Node if needed once connectivity and TLS are corrected.
5. **Verification**
    - Confirm `publisher_block_items_received` increases steadily.
    - `publisher_open_connections` is stable and non-zero.
    - Logs show continuous block publish / ingest activity.

</details>

<details>
<summary>🔌 **Subscribers/mirror nodes cannot connect**</summary>

1. **Triage**
    - Confirm symptoms from client side:
        - gRPC connection failures, timeouts, or TLS errors when subscribing.
        - Clients repeatedly reconnecting or backing off.
    - Confirm on the Block Node:
        - Service is running and listening on the expected gRPC port.
        - No obvious CPU / memory starvation.
2. **Network and endpoint checks**
    - From a trusted host (for example, mirror node):
        - `nc <IP_OF_BLOCK_NODE> <GRPC_PORT> -vz`
            - Success: TCP reachability is OK.
            - Failure: suspect firewall, security group, or local iptables.
    - Optional gRPC sanity checks (requires `grpcurl` and proto files):
        - `grpcurl -plaintext -proto /path/to/block_access_service.proto <IP_OF_BLOCK_NODE>:<GRPC_PORT> org.hiero.block.api.BlockAccessService/getBlock '{"retrieve_latest": true}'`
        - `grpcurl -plaintext -proto /path/to/block_stream_subscribe_service.proto <IP_OF_BLOCK_NODE>:<GRPC_PORT> org.hiero.block.api.BlockStreamSubscribeService/subscribeBlockStream '{"start_block_number": <BLOCK>, "end_block_number": <BLOCK>}'`
        - If TLS is enabled, replace `-plaintext` with `-insecure` or `-cacert <CA_FILE>`.
    - Verify:
        - Correct advertised hostname / IP and port in Block Node config.
        - DNS or load balancer is pointing to the active node.
3. **TLS and auth**
    - Check client logs for:
        - `x509: certificate has expired or is not yet valid`.
        - Hostname mismatch between certificate and endpoint.
        - Unknown CA / trust failures.
    - On the Block Node, confirm:
        - TLS cert and key paths are correct and readable.
            - Certificates are not expired and chain is complete.
4. **Service configuration**
    - Ensure the publish / subscribe services are enabled in configuration (for example, `BlockStreamSubscribeService`).
    - Check any rate limits or `max-connections` settings that might be rejecting clients.
5. **Resolution**
    - Fix endpoint configuration (advertise address / port), update DNS or load balancer if needed.
    - Renew or reinstall TLS certificates and restart Block Node and / or clients.
    - Update firewall / security groups to allow gRPC traffic from subscribers.
6. **Verification**
    - Confirm clients successfully establish long-lived gRPC streams without continuous reconnects.
    - Metrics such as `subscriber_open_connections` are stable and `subscriber_errors` do not spike.

</details>

<details>
<summary>💾 **Disk full / out of space**</summary>

1. **Triage**
    - Confirm symptoms:
        - Node crashes, refuses new blocks, or logs I/O errors.
        - Alerts on storage utilization from Grafana / Prometheus.
    - Check capacity on the host:
        - `df -h` for filesystem usage.
        - `iostat`, `files_recent_total_bytes_stored`, and `files_recent_persistence_time_latency_ns` for pressure.
2. **Identify what is consuming space**
    - Determine which volume(s) hold block data, snapshots, and logs.
    - Inspect directories for unexpected growth (logs, temp, or backup folders).
    - *TODO: document the canonical data directory layout for Block Nodes (paths for blocks, snapshots, logs).*  ← GAP
3. **Short-term mitigation**
    - If safe, rotate / compress / prune logs.
    - If using partial-history nodes, enable or adjust pruning according to policy.
    - Temporarily add storage space to the affected volume if possible.
4. **Longer-term resolution**
    - For archival needs, migrate to a node with larger storage or externalize cold data.
    - Tune retention settings for blocks, snapshots, and logs.
    - Ensure monitoring alerts fire well before 100% usage (for example, at 75%, 85%, 95%).
5. **Verification**
    - Confirm `files_recent_total_bytes_stored` (and/or `files_historic_total_bytes_stored`) and host `df -h` fall below alert thresholds.
    - Block ingest resumes normally and no further I/O errors appear in logs.

</details>

<details>
<summary>📈 **Metrics endpoint not accessible**</summary>

1. **Triage**
    - Confirm that Grafana / Prometheus cannot scrape `/metrics` for this Block Node.
    - Attempt to curl from a nearby host:
        - `curl -v http://<IP_OF_BLOCK_NODE>:16007/metrics`
            - Success: HTTP 200 with Prometheus text output.
            - Failure: connection refused / timeout.
2. **Node-local checks**
    - Ensure metrics are enabled in the Block Node configuration (`prometheus.enableEndpoint=true`).
    - From the node itself:
        - `curl -v http://localhost:16007/metrics`
            - If this fails, suspect local config or process issues.
    - Confirm `prometheus.endpointPortNumber` matches the expected port (default `16007`).
3. **Network and firewall**
    - If [localhost](http://localhost) works but remote scrape fails:
        - Check host firewall / security group rules for port `16007`.
        - Confirm any load balancers or service meshes expose the metrics port.
4. **Scraper configuration**
    - Verify Prometheus target configuration:
        - Correct job name, scheme (http / https), and port.
        - No incorrect path overrides.
5. **Resolution**
    - Enable metrics in config and restart the Block Node if required.
    - Open or adjust firewall / security rules for the metrics port.
    - Fix Prometheus / Grafana scrape configuration.
6. **Verification**
    - Confirm the Prometheus target is `UP` and `up\{job="block-node-metrics", instance="\<node\>"\} == 1`.
    - Grafana dashboards populate and scrape errors clear.

</details>

<details>
<summary>🕳️ **Blocks are not being backfilled**</summary>

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
    - Verify `BLOCK_NODE_EARLIEST_MANAGED_BLOCK` reflects the earliest block this node should own.
    - Verify `BACKFILL_START_BLOCK` / `BACKFILL_END_BLOCK` are set correctly relative to available upstream history.
    - Ensure backfill is enabled in the node configuration and not paused.
4. **Network and permissions**
    - Confirm this node can reach upstream Block Nodes over the required ports.
    - Check that any authentication / TLS between nodes is valid.
5. **Resolution**
    - Correct misconfigured earliest-block values (`BLOCK_NODE_EARLIEST_MANAGED_BLOCK`, `BACKFILL_START_BLOCK`, `BACKFILL_END_BLOCK`) and apply changes.
    - Restart the Block Node if configuration changes require it.
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
| Node not receiving new blocks | Ingest appears stalled, logs show no publish activity | Check firewall / TLS on ingest port (default `50211`), Consensus Node logs | Open inbound port, verify mutual TLS certs, ensure node is authorized / whitelisted by upstream CN |
| Subscribers / mirror nodes cannot connect | gRPC connection refused or TLS handshake errors | Wrong endpoint, expired cert, or service disabled in config | Verify advertise address / port, renew TLS certs, enable `BlockStreamSubscribeService` |
| Disk full / out of space | Node crashes or refuses new blocks | `df -h`, `files_recent_total_bytes_stored` nearing limit | Prune old blocks (partial-history), expand volume, or migrate to archive node |
| Metrics endpoint not accessible | Grafana dashboards empty, Prometheus target `DOWN` | Port `16007` blocked or metrics disabled via config | Open port, enable metrics, fix Prometheus scrape job |
| Blocks not being backfilled | Log entries show backfill warnings, missing historical ranges | Check `hiero_block_node_backfill*` metrics, `BLOCK_NODE_EARLIEST_MANAGED_BLOCK` / `BACKFILL_START_BLOCK` config | Fix earliest-block config, restart node if required, ensure healthy upstream archival source |

---

For help, open a GitHub issue in [`hiero-ledger/hiero-block-node`](https://github.com/hiero-ledger/hiero-block-node/issues).
