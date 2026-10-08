# DR Playbook: Network-Wide Backpressure Due to Block Node Streaming Failure

> **Summary:** Backpressure is active on enough Consensus Nodes to halt the network. Recovery requires restoring at least one Block Node that can accept and acknowledge blocks.

|                      |                                                                                 |
|----------------------|---------------------------------------------------------------------------------|
| **Playbook ID**      | PB-04                                                                           |
| **Severity**         | SEV-1                                                                           |
| **Primary executor** | Hashgraph DevOps                                                                |
| **Escalate to**      | Hashgraph Engineering if block proof validation fails or data loss is confirmed |

---

## Is this your doc?

Check these without running any commands:

- All transactions are **fully halted** - not just slow or throttled
- You received an alert that `blockStream_buffer_backPressureState = 3` on multiple CNs, OR you see "Block buffer is saturated; backpressure is being enabled" in CN logs
- Block Nodes are not responding to health checks, or CN logs show no active BN connections

**Not your doc if:**
- The network is still processing some transactions → fewer than 1/3 of CNs are affected, see PB-02
- BN health checks pass and blocks are being acknowledged → investigate CN-side configuration directly

---

## Before starting

> **Open the incident bridge immediately. Assign:**
> - **Incident commander** - owns resolution, makes go/no-go calls on each phase
> - **Communication lead** - owns public status page and stakeholder updates (not the person executing steps)
>
> Every phase below is executed by the incident commander and designated operators. Communication lead posts updates externally while technical work proceeds in parallel.

---

## Phase 1 - Diagnose

> **Do not skip these before acting:**
> - Do not restart any CN before checking buffer persistence (Step 1) - a restart destroys buffered blocks
> - Do not restart multiple CNs simultaneously - you will lose quorum
> - Do not assume admin transactions bypass backpressure - ALL transactions are blocked

| # |                                                                                             Action                                                                                             |                                                                                                                                                                                                                        Notes                                                                                                                                                                                                                        | Done |
|---|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1 | **Check CN buffer persistence before touching anything.**                                                                                                                                      | `grep isBufferPersistenceEnabled <CN_CONFIG_PATH>/application.properties` **If `false`: do NOT restart any CN until Phase 3 BN recovery is complete. A CN restart destroys all buffered blocks and makes data loss permanent.** Persistence enabled: Y / N                                                                                                                                                                                          | ☐    |
| 2 | Confirm backpressure state on each CN. No single metric shows network-wide backpressure - check each CN individually.                                                                          | `curl -s http://<CN_HOST>:<CN_METRICS_PORT>/metrics \| grep blockStream_buffer_backPressureState` Value 3 = active/halted. CNs at value 3: _______ / _______ total                                                                                                                                                                                                                                                                                  | ☐    |
| 3 | Identify which buffer threshold triggered.                                                                                                                                                     | CN logs: `Block buffer is saturated; backpressure is being enabled (blockCountSaturation: X%, bytesSaturation: X%, blocksInProgressSaturation: X%)`. Whichever is at 100%+ is the trigger. Triggered by: _______                                                                                                                                                                                                                                    | ☐    |
| 4 | Record last known good block numbers. Then calculate remaining recovery window: `(maxBlocks - gap) x avg_block_time`. If under 60 seconds, go to Phase 2 Step 3 immediately before continuing. | BN: `grpcurl -plaintext <BN_HOST>:<BN_GRPC_PORT> com.hedera.hapi.block.BlockNodeService/serverStatus` CN: `blockStream_buffer_latestBlockAcked` metric. Last BN block: _______ / CN current block: _______ / Gap: _______ / Remaining window: _______ s                                                                                                                                                                                             | ☐    |
| 5 | Classify the failure using these checks.                                                                                                                                                       | `systemctl status <BN_SERVICE_UNIT_NAME>` → process state. `nc -zv <BN_HOST> <BN_GRPC_PORT>` → network path. `grpc_health_probe -addr=<BN_HOST>:<BN_GRPC_PORT>` → service health. **A** - process not running or host unreachable · **B** - process running, port unreachable (nc fails) · **C** - port reachable, health probe fails or BN returning errors · **D** - health probe passes but BN reports corrupt/missing blocks. Category: _______ | ☐    |

---

## Phase 2 - Buy Time

| # |                                                                                                                                                                                               Action                                                                                                                                                                                                |                                                                                                                                                                                                                                                                                      Notes                                                                                                                                                                                                                                                                                      | Done |
|---|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1 | Monitor buffer saturation continuously. Assign one operator to watch and report every 2 minutes.                                                                                                                                                                                                                                                                                                    | `watch -n 5 'curl -s http://<CN_HOST>:<CN_METRICS_PORT>/metrics \| grep blockStream_buffer_saturation'`                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | ☐    |
| 2 | Check if a standby or Tier-2 BN is available and close to chain tip. If yes, route CNs to it and skip directly to Phase 4.                                                                                                                                                                                                                                                                          | `grpc_health_probe -addr=<STANDBY_BN_HOST>:<BN_GRPC_PORT>` then check `serverStatus`. Compare BN latest block against CN `blockStream_buffer_oldestBlock` - standby must be within that block range to be usable. **To route CNs:** update the BN endpoint in the CN config (`[FILL IN from pre-incident config record - see "BN endpoint list" and your CN endpoint update procedure]`) and apply the change. Note: if BN endpoint discovery uses on-chain registry (HIP-1137), a registration transaction is required - use Phase 2 Step 3 to open a submission window first. | ☐    |
| 3 | **Only if** you need to submit an admin transaction (e.g., register a replacement BN on-chain): note that backpressure blocks ALL transactions - including admin ones. There is no bypass. To create a temporary submission window, raise the buffer thresholds in `application.properties` on all CNs, then do a rolling CN restart (one at a time) so backpressure disengages and you can submit. | Properties to raise: `blockStream.buffer.maxBlocks`, `blockStream.buffer.maxBytes`, `blockStream.buffer.maxInProgressBlocks`. **How much to raise:** every 150 blocks added to `maxBlocks` buys ~5 minutes of window (at 2 s/block). Example: need 10 minutes → add 300 to current `maxBlocks`. Raise the other two thresholds proportionally. **Temporary workaround only - the buffer fills again until Phase 3 completes. Never restart multiple CNs at once.**                                                                                                              | ☐    |

---

## Phase 3 - Recover

Identify your failure category from Phase 1 Step 5 and jump to that section. Complete the first and last steps regardless of category.

### Step 1 - Save logs [all categories]

|                Action                |                                                            Notes                                                            | Done |
|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|------|
| Preserve BN logs before any restart. | `journalctl -u <BN_SERVICE_UNIT_NAME> --since "<INCIDENT_START>" > /tmp/bn_incident_$(hostname)_$(date +%Y%m%d_%H%M%S).log` | ☐    |

---

### Category A - BN process down

| #  |                           Action                           |                                                                                      Notes                                                                                      | Done |
|----|------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| A1 | Restart the BN service and confirm gRPC port is listening. | `systemctl restart <BN_SERVICE_UNIT_NAME>` then `grpc_health_probe -addr=localhost:<BN_GRPC_PORT>`. Still failing after 2 minutes → go to Category D.                           | ☐    |
| A2 | Confirm BN is active and catching up from a peer BN.       | `watch -n 10 'grpcurl -plaintext <BN_HOST>:<BN_GRPC_PORT> com.hedera.hapi.block.BlockNodeService/serverStatus'`. Latest block not advancing after 5 minutes → go to Category D. | ☐    |

### Category B - Network path broken

| #  |                   Action                   |                                                                        Notes                                                                        | Done |
|----|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|------|
| B1 | Restore network connectivity.              | `nc -zv <BN_HOST> <BN_GRPC_PORT>` to verify path. Steps are environment-specific (security groups, firewall, VPN). Log all changes with timestamps. | ☐    |
| B2 | Confirm BN is active and CNs can reach it. | `grpc_health_probe -addr=<BN_HOST>:<BN_GRPC_PORT>` then check `serverStatus`. Latest block not advancing → go to Category D.                        | ☐    |

### Category C - BN running but crashing

| #  |                                 Action                                 |                                                                                      Notes                                                                                      | Done |
|----|------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| C1 | Inspect BN block storage tail for incomplete writes before restarting. | `ls -lt <FILES_RECENT_LIVE_ROOT_PATH> \| head -10` (path from pre-incident config record). Zero-length files or broken sequences → go to Category D.                            | ☐    |
| C2 | Restart the BN service. Watch logs for proof verification errors.      | `systemctl restart <BN_SERVICE_UNIT_NAME>` then `journalctl -u <BN_SERVICE_UNIT_NAME> -f \| grep -iE 'verif\|proof\|error\|corrupt'`. Any proof failures → go to Category D.    | ☐    |
| C3 | Confirm BN is active and catching up from a peer BN.                   | `watch -n 10 'grpcurl -plaintext <BN_HOST>:<BN_GRPC_PORT> com.hedera.hapi.block.BlockNodeService/serverStatus'`. Latest block not advancing after 5 minutes → go to Category D. | ☐    |

### Category D - Data corruption or total loss

| #  |                  Action                   |                                                                                                                         Notes                                                                                                                         | Done |
|----|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| D1 | Retrieve the latest valid state snapshot. | Query a surviving peer BN or retrieve from cloud storage per pre-incident configuration record. Snapshot block (N): _______                                                                                                                           | ☐    |
| D2 | Restore snapshot and start the BN.        | `[FILL IN from pre-incident configuration record - see "Snapshot restore command" entry]`. Pre-incident record must include: the exact restore command, the BN config key for setting the bootstrap block number, and the target data directory path. | ☐    |
| D3 | Monitor bootstrap and catch-up.           | `journalctl -u <BN_SERVICE_UNIT_NAME> -f \| grep -iE 'bootstrap\|catchup\|block'` then `watch -n 10 'grpcurl -plaintext <BN_HOST>:<BN_GRPC_PORT> com.hedera.hapi.block.BlockNodeService/serverStatus'`                                                | ☐    |

---

### Final step - Confirm CN reconnection [all categories]

|                      Action                       |                                                                     Notes                                                                      | Done |
|---------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|------|
| Confirm CNs are reconnecting to the recovered BN. | `journalctl -u <CN_SERVICE_UNIT_NAME> -f \| grep -iE 'block.node\|reconnect\|wantedBlock'`. Look for: "Selected new block node for streaming". | ☐    |

---

## Phase 4 - Verify and Close

> The BN is back up and CNs are reconnecting. Now confirm no data was lost and backpressure has fully released.
>
> **About `TOO_FAR_BEHIND`:** CNs buffer unacknowledged blocks up to `producer.staleResendPruneBuffer` blocks behind the BN's current tip. If the gap is larger than that value, the CN emits `TOO_FAR_BEHIND` and cannot auto-replay those blocks - they are at risk of permanent loss. If you see this, stop immediately and engage PB-01.

| # |                                                                                     Action                                                                                      |                                                                                                                                                            Notes                                                                                                                                                             | Done |
|---|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 1 | Confirm the gap blocks are still in CN buffers.                                                                                                                                 | `curl -s http://<CN_HOST>:<CN_METRICS_PORT>/metrics \| grep blockStream_buffer_oldestBlock`. Gap is recoverable if `oldestBlock <= last BN block + 1`. If not: engage PB-01 (Bootstrap from Snapshot) immediately.                                                                                                           | ☐    |
| 2 | Monitor buffer drain. Watch for `TOO_FAR_BEHIND` in CN logs.                                                                                                                    | `journalctl -u <CN_SERVICE_UNIT_NAME> -f \| grep -iE 'TOO_FAR_BEHIND\|reconnect\|wantedBlock'`                                                                                                                                                                                                                               | ☐    |
| 3 | Validate block proof integrity across the outage boundary.                                                                                                                      | `<BN_TOOLS_PATH>/blocks validate --start-block <X> --end-block <Y>` where X = Last BN block and Y = CN current block recorded in Phase 1 Step 4. **Any failure: STOP. Preserve all logs. Escalate to Hashgraph Engineering immediately.**                                                                                    | ☐    |
| 4 | Confirm backpressure has released on all CNs (value 0). Backpressure releases when saturation drops to or below the recovery threshold (source default: 85% `[VERIFY]`).        | `watch -n 5 'curl -s http://<CN_HOST>:<CN_METRICS_PORT>/metrics \| grep blockStream_buffer_backPressureState'`                                                                                                                                                                                                               | ☐    |
| 5 | Confirm transaction throughput has recovered. Mirror Node reconnects automatically.                                                                                             | `curl -s "http://<MN_HOST>:5551/api/v1/transactions?limit=5&order=desc" \| jq '.transactions[0].consensus_timestamp'`. Timestamps advancing = network recovered. **User communication:** transactions submitted during the halt were not processed and must be resubmitted. Users will see no record of them on Mirror Node. | ☐    |
| 6 | Recover remaining Tier-1 BNs using Phase 3. If BN endpoint addresses changed, update CN config and restart CNs one at a time - never simultaneously. Update public status page. |                                                                                                                                                                                                                                                                                                                              | ☐    |

---

## Reference

### Buffer thresholds that trigger backpressure

|      Threshold      |             Config property              |                What it measures                 |
|---------------------|------------------------------------------|-------------------------------------------------|
| Unacked block count | `blockStream.buffer.maxBlocks`           | Blocks sent but not acknowledged by any BN      |
| Unacked block bytes | `blockStream.buffer.maxBytes`            | Total bytes of blocks sent but not acknowledged |
| In-progress blocks  | `blockStream.buffer.maxInProgressBlocks` | Blocks currently being streamed                 |

Source default: `maxBlocks` = 150, average block time = 2 s → recovery window ~5 minutes (confirmed by Tim Farber-Newman, Oct 2026)

### Key metrics

|                  Metric                  |                        Alert condition                        |
|------------------------------------------|---------------------------------------------------------------|
| `blockStream_buffer_backPressureState`   | Page immediately if = 3                                       |
| `blockStream_conn_activeConnectionCount` | Page if = 0 for > 30 s                                        |
| `blockStream_buffer_latestBlockAcked`    | Alert if frozen while `latestBlockOpened` advances            |
| `blockStream_buffer_saturation`          | Alert if sustained above your environment's observed baseline |
| `blockStream_connSend_failure`           | Alert on sustained increase                                   |

`backPressureState` values: 0 = normal · 1 = switching BN · 2 = recovering · 3 = active/halted

### Pre-incident configuration record

Fill this in before you need it. Every placeholder in this playbook maps to a value below.

- `blockStream.buffer.maxBlocks`: _______
- `blockStream.buffer.isBufferPersistenceEnabled`: _______
- `producer.staleResendPruneBuffer`: _______
- Estimated recovery window: _______ s (`maxBlocks` x avg block time in seconds)
- `BN_SERVICE_UNIT_NAME`: _______
- `CN_METRICS_PORT`: _______
- `BN_TOOLS_PATH`: _______
- `CN_CONFIG_PATH`: _______
- `FILES_RECENT_LIVE_ROOT_PATH` (BN live block storage directory): _______
- BN endpoint list: _______
- Snapshot storage location: _______
- Snapshot restore command: _______
- BN config key for bootstrap block number: _______
- BN data directory path: _______
- Standby BN host: _______

### Post-incident checklist

After the incident is closed: archive CN and BN logs, run a post-mortem within 48 hours, and address:

|               Finding                |                               Remediation                                |
|--------------------------------------|--------------------------------------------------------------------------|
| `staleResendPruneBuffer < maxBlocks` | Align: set BN value >= CN value                                          |
| Buffer persistence was off           | Enable `blockStream.buffer.isBufferPersistenceEnabled = true` on all CNs |
| BNs shared a failure domain          | Enforce Tier-1 BN placement across distinct AZs or providers             |
| No available standby BN              | Pre-stage at least one warm standby BN                                   |
| State snapshot was stale             | Increase BN snapshot frequency; define RPO                               |

---

*Playbook version: 1.0 DRAFT - verify all `[VERIFY]` defaults against your deployed version*
*Source defaults: CN v0.70 / BN v0.15 (May 2026)*
*Closes: [#3786](https://github.com/hiero-ledger/hiero-block-node/issues/3786)*
