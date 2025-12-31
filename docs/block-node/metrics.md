# Hiero Block Node Metrics

## Table of Contents

1. [Summary](#summary)
2. [Purpose](#purpose)
3. [Configuration](#configuration)
4. [How to Access Metrics](#how-to-access-metrics)
5. [Metrics](#metrics-by-plugin)
   1. [app](#app)
   2. [Block Access](#block-access)
   3. [Block Messaging](#block-messaging)
   4. [Publisher](#publisher)
   5. [Subscriber](#subscriber)
   6. [Verification](#verification)
   7. [files.recent](#filesrecent)
   8. [files.historic](#fileshistoric)
   9. [s3-archive](#s3-archive)
   10. [Server Status API](#server-status-api)
   11. [Backfill](#backfill)

## Summary

This document describes the metrics that are available in the system, its purpose, and how to use them.

**App level metric name prefix:** `hiero_block_node`

These metrics expose **operational health, data‑integrity, and storage growth** for every stage of a Hiero Block Node (BN).
They are scraped by Prometheus with the **standard pull model**:

## Purpose

The purpose of metrics is to provide a way to measure the performance of the system. Metrics are used to monitor the system and to detect any issues that may arise. Metrics can be used to identify bottlenecks, track the performance of the system over time, and to help diagnose problems.

## Configuration

### Prometheus

Prefix: prometheus, ie. `prometheus.configKey`

| ConfigKey                 | Description                                                                           | Default |
|:--------------------------|:--------------------------------------------------------------------------------------|--------:|
| enableEndpoint            | either `true` or `false`. Enables or disables the endpoint for metrics                |    true |
| endpointPortNumber        | Port of the Prometheus endpoint                                                       |   16007 |
| endpointMaxBacklogAllowed | The maximum number of incoming TCP connections which the system will queue internally |       1 |

## How to Access Metrics

```
http://<host>:16007/metrics
```

* Default port `16007`.
* Output is plain‑text in Prometheus exposition format (`# HELP`, `# TYPE`, `<metric> <value>`).

Example `scrape_configs` snippet:

        scrape_configs:
          - job_name: hiero-block-node
            static_configs:
              - targets: ['bn‑01.example.com:16007']   # change port if customised

---

## Metrics by Plugin

### app

**Plugin:** `app`
Node‑level state and current block numbers.

| Type  |             Name              |              Description               |
|-------|-------------------------------|----------------------------------------|
| Gauge | `app_historical_oldest_block` | Oldest block the BN currently stores   |
| Gauge | `app_historical_newest_block` | Newest block the BN currently stores   |
| Gauge | `app_state_status`            | 0=Starting, 1=Running, 2=Shutting Down |

---

### Block Access

**Plugin:** `block-access [block-access-service]`
Observes the block access service that serves requests for blocks.

|  Type   |                Name                |                 Description                 |
|---------|------------------------------------|---------------------------------------------|
| Counter | `get_block_requests`               | Number of get block requests                |
| Counter | `get_block_requests_success`       | Successful single block requests            |
| Counter | `get_block_requests_not_available` | Requests for blocks that were not available |
| Counter | `get_block_requests_not_found`     | Requests for blocks that were not found     |

---

### Block Messaging

**Plugin:** `messaging [facility-messaging]`
Observes the messaging system that connects the publisher with subscribers and the rest of the system.

|  Type   |                          Name                           |                 Description                 |
|---------|---------------------------------------------------------|---------------------------------------------|
| Counter | `messaging_block_items_received`                        | Incoming block items seen by the mediator   |
| Counter | `messaging_block_verification_notifications`            | Notifications issued after verification     |
| Counter | `messaging_block_persisted_notifications`               | Notifications issued after persistence      |
| Gauge   | `messaging_no_of_item_listeners`                        | Active item listeners                       |
| Gauge   | `messaging_no_of_notification_listeners`                | Active notification listeners               |
| Gauge   | `messaging_item_queue_percent_used`                     | Percent of item queue utilised              |
| Gauge   | `messaging_notification_queue_percent_used`             | Percent of notification queue utilised      |
| Counter | `messaging_block_backfilled_notifications`              | Notifications issues for backfilling blocks |
| Counter | `messaging_newest_block_known_to_network_notifications` | Notifications issued for newest block known |

---

### Publisher

**Plugin:** `publisher [block-node-publisher]`
Observes inbound streams from publishers.

|  Type   |                   Name                   |                     Description                     |
|---------|------------------------------------------|-----------------------------------------------------|
| Counter | `publisher_block_items_received`         | Live block items received (sum over all publishers) |
| Gauge   | `publisher_lowest_block_number_inbound`  | Lowest incoming block number                        |
| Gauge   | `publisher_highest_block_number_inbound` | Highest incoming block number                       |
| Gauge   | `publisher_open_connections`             | Connected publishers                                |
| Counter | `publisher_blocks_ack_sent`              | Block‑ack messages sent                             |
| Gauge   | `publisher_latest_block_number_ack_sent` | Latest Block Number Ack Sent from Publisher         |
| Counter | `publisher_stream_errors`                | Publisher connection streams that end in an error   |
| Counter | `publisher_blocks_skips_sent`            | Block‑ack skips sent                                |
| Counter | `publisher_blocks_resend_sent`           | Block Resend messages sent                          |
| Counter | `publisher_block_endofstream_sent`       | Block End-of-Stream messages sent                   |
| Counter | `publisher_block_endstream_received`     | Block End-Stream messages received                  |
| Counter | `publisher_receive_latency_ns`           | Time taken to receive a block (ns=nanoseconds)      |

---

### Subscriber

**Plugin:** `stream-subscriber` [block-node-stream-subscriber]`
Observes outbound streams served to subscribers.

|  Type   |             Name              |              Description              |
|---------|-------------------------------|---------------------------------------|
| Gauge   | `subscriber_open_connections` | Connected subscribers                 |
| Counter | `subscriber_errors`           | Errors while streaming to subscribers |

---

### Verification

**Plugin:** `verification [block-node-verification]`
Measures block‑verification throughput and success rate.

|  Type   |              Name              |                 Description                  |
|---------|--------------------------------|----------------------------------------------|
| Counter | `verification_blocks_received` | Blocks received for verification             |
| Counter | `verification_blocks_verified` | Blocks that passed verification              |
| Counter | `verification_blocks_failed`   | Blocks that failed verification              |
| Counter | `verification_blocks_error`    | Internal errors during verification          |
| Counter | `verification_block_time`      | Verification time per block (ns=nanoseconds) |
| Counter | `hashing_block_time`           | Hashing time per block (ns=nanoseconds)      |

---

### files.recent

**Plugin:** `block-providers/files.recent [block-node-blocks-file-recent]`
Activity and utilization of the recent on‑disk tier.

|  Type   |                    Name                    |                  Description                   |
|---------|--------------------------------------------|------------------------------------------------|
| Counter | `files_recent_blocks_written`              | Blocks written to recent tier                  |
| Counter | `files_recent_blocks_read`                 | Blocks read from recent tier                   |
| Counter | `files_recent_blocks_deleted`              | Blocks deleted from recent tier                |
| Counter | `files_recent_blocks_deleted_failed`       | Blocks failed deletion from recent tier        |
| Gauge   | `files_recent_blocks_stored`               | Blocks stored in recent tier                   |
| Gauge   | `files_recent_total_bytes_stored`          | Bytes stored in recent tier                    |
| Counter | `files_recent_persistence_time_latency_ns` | Time taken to persist a block (ns=nanoseconds) |

---

### files.historic

**Plugin:** `block-providers/files.historic [block-node-blocks-file-historic]`
Activity and utilization of the historic on‑disk tier.

|  Type   |                 Name                 |              Description              |
|---------|--------------------------------------|---------------------------------------|
| Counter | `files_historic_blocks_written`      | Blocks written to historic tier       |
| Counter | `files_historic_blocks_read`         | Blocks read from historic tier        |
| Gauge   | `files_historic_blocks_stored`       | Blocks stored in historic tier        |
| Gauge   | `files_historic_total_bytes_stored`  | Bytes stored in historic tier         |
| Counter | `files_historic_zips_deleted_failed` | Zips failed deletion in historic tier |

---

### s3‑archive

**Plugin:** `s3-archive [hiero-block-node.s3-archive]`
Tracks long‑term archival jobs that push blocks to S3.

|  Type   |              Name               |               Description               |
|---------|---------------------------------|-----------------------------------------|
| Counter | `s3_archive_blocks_written`     | Blocks archived to S3                   |
| Gauge   | `s3_archive_latest_block`       | Latest block number archived            |
| Counter | `s3_archive_tasks_failed_total` | Failed archival tasks                   |
| Counter | `s3_archive_tasks_sucess_total` | Successful archival tasks               |
| Gauge   | `s3_archive_total_bytes_stored` | Total bytes stored in S3 (cost metric)  |
| Counter | `s3_archive_chunks_uploaded`    | Chunks uploaded to S3                   |
| Gauge   | `s3_archive_chunks_opened`      | Open chucks currently                   |
| Counter | `s3_archive_files_closed`       | Total number of files closed, finished. |

---

### Server Status API

**Plugin:** `server-status [block-node-server-status]`
Observes the server status API that provides information about the node.

|  Type   |           Name            |           Description            |
|---------|---------------------------|----------------------------------|
| Counter | `server_status_requests ` | Number of server status requests |

### Backfill

**Plugin:** `backfill [block-node-backfill]`
Provides metrics related to the backfill process, including On-Demand and Historical backfills.

|  Type   |             Name             |                                            Description                                            |
|---------|------------------------------|---------------------------------------------------------------------------------------------------|
| Counter | `backfill_gaps_detected`     | Total number of gaps detected at start-up                                                         |
| Counter | `backfill_blocks_fetched`    | Total number of blocks fetched during backfill                                                    |
| Counter | `backfill_blocks_backfilled` | Total number of blocks successfully backfilled                                                    |
| Counter | `backfill_fetch_errors`      | Total number of errors encountered while fetching blocks                                          |
| Counter | `backfill_retries`           | Total number of retries attempted during backfill                                                 |
| Gauge   | `backfill_status`            | Current status of the backfill process (0=Idle, 1=Running) |
| Gauge   | `backfill_pending_blocks`    | Number of blocks pending to be backfilled                                                         |

## Alerting Recommendations

Alerting rules can be created based on these metrics to notify the operations team of potential issues.
Utilizing Low (L), Medium (M) and High (H) severity levels, some recommended alerting rules to consider include:

Note: High level alerts are intentionally left out during the beta 1 phase to reduce noise.
As the product matures through beta and rc phases, high severity alerts will be added.

**Node Status**: High level alerts for overall node health

| Severity |       Metric       |      Alert Condition      |
|----------|--------------------|---------------------------|
| M        | `app_state_status` | If not equal to `RUNNING` |

**Publisher**: Alerts related to publisher connections and performance

| Severity |             Metric             |                   Alert Condition                   |
|----------|--------------------------------|-----------------------------------------------------|
| L        | `publisher_open_connections`   | If value exceeds 40, otherwise, configure as needed |
| M        | `publisher_receive_latency_ns` | If value exceeds 5s                                 |

**Failures**: Alerts for various failure metrics

| Severity |                 Metric                 |                          Alert Condition                           |
|----------|----------------------------------------|--------------------------------------------------------------------|
| M        | `verification_blocks_error`            | If errors during verification exceed 3 in last 60s                 |
| M        | `publisher_block_send_response_failed` | If value exceeds 3 in the last 60s, otherwise, configure as needed |
| L        | `backfill_fetch_errors`                | If value exceeds 3 in the last 60s, otherwise, configure as needed |
| M        | `publisher_stream_errors`              | If value exceeds 3 in the last 60s, otherwise, configure as needed |

**Messaging**: Alerts for messaging service operations regarding block items and block notification

| Severity |                   Metric                    |                      Alert Condition                      |
|----------|---------------------------------------------|-----------------------------------------------------------|
| L        | `messaging_item_queue_percent_used`         | If percentage exceeds 60%, otherwise, configure as needed |
| L        | `messaging_notification_queue_percent_used` | If percentage exceeds 60%, otherwise, configure as needed |

**Latency**: Alerts for latency metrics in receiving, hashing, verifying, and persisting blocks

| Severity |                   Metric                   |                   Alert Condition                    |
|----------|--------------------------------------------|------------------------------------------------------|
| M        | `publisher_receive_latency_ns`             | If value exceeds 20s, otherwise, configure as needed |
| M        | `hashing_block_time`                       | If value exceeds 2s, otherwise, configure as needed  |
| M        | `verification_block_time`                  | If value exceeds 20s, otherwise, configure as needed |
| M        | `files_recent_persistence_time_latency_ns` | If value exceeds 20s, otherwise, configure as needed |
