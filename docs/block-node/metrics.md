# Hiero Block Node Metrics

**App level metric name prefix:** `hiero_block_node`

These metrics expose **operational health, data‑integrity, and storage growth** for every stage of a Hiero Block Node (BN).
They are scraped by Prometheus with the **standard pull model**:

```
http://<host>:9999/metrics
```

* Default port `9999`.
* Output is plain‑text in Prometheus exposition format (`# HELP`, `# TYPE`, `<metric> <value>`).

Example `scrape_configs` snippet:

        scrape_configs:
          - job_name: hiero-block-node
            static_configs:
              - targets: ['bn‑01.example.com:9999']   # change port if customised

---

## app

**Plugin:** `app`
Node‑level state and current block numbers.

| Type  |             Name              |              Description              |
|-------|-------------------------------|---------------------------------------|
| Gauge | `app_historical_oldest_block` | Oldest block the BN currently stores  |
| Gauge | `app_historical_newest_block` | Newest block the BN currently stores  |
| Gauge | `app_state_status`            | 0=Starting, 1=Running, 2Shutting Down |

---

## Block Access

**Plugin:** `block-access [block-access-service]`
Observes the block access service that serves requests for single blocks.

|  Type   |                 Name                  |                 Description                 |
|---------|---------------------------------------|---------------------------------------------|
| Counter | `single-block-requests`               | Number of single block requests             |
| Counter | `single-block-requests-success`       | Successful single block requests            |
| Counter | `single-block-requests-not-available` | Requests for blocks that were not available |
| Counter | `single-block-requests-not-found`     | Requests for blocks that were not found     |

---

## Block Messaging

**Plugin:** `messaging [facility-messaging]`
Observes the messaging system that connects the publisher with subscribers and the rest of the system.

|  Type   |                     Name                     |                Description                |
|---------|----------------------------------------------|-------------------------------------------|
| Counter | `messaging_block_items_received`             | Incoming block items seen by the mediator |
| Counter | `messaging_block_verification_notifications` | Notifications issued after verification   |
| Counter | `messaging_block_persisted_notifications`    | Notifications issued after persistence    |
| Gauge   | `messaging_no_of_item_listeners`             | Active item listeners                     |
| Gauge   | `messaging_no_of_notification_listeners`     | Active notification listeners             |
| Gauge   | `messaging_item_queue_percent_used`          | Percent of item queue utilised            |
| Gauge   | `messaging_notification_queue_percent_used`  | Percent of notification queue utilised    |

---

## Publisher

**Plugin:** `publisher [block-node-publisher]`
Observes inbound streams from publishers.

|  Type   |                   Name                   |                     Description                     |
|---------|------------------------------------------|-----------------------------------------------------|
| Counter | `publisher_block_items_received`         | Live block items received (sum over all publishers) |
| Counter | `live_block_items_messaged`              | Legacy metric – kept for compatibility              |
| Gauge   | `publisher_lowest_block_number_inbound`  | Oldest inbound block number                         |
| Gauge   | `publisher_current_block_number_inbound` | Current block number from primary publisher         |
| Gauge   | `publisher_highest_block_number_inbound` | Newest inbound block number                         |
| Gauge   | `publisher_open_connections`             | Connected publishers                                |
| Counter | `publisher_blocks_ack_sent`              | Block‑ack messages sent                             |

---

## Subscriber

**Plugin:** `stream-subscriber` [block-node-stream-subscriber]`
Observes outbound streams served to subscribers.

|  Type   |              Name              |                          Description                           |
|---------|--------------------------------|----------------------------------------------------------------|
| Counter | `subscriber_historic_to_live`  | Historic‑to‑live stream transitions                            |
| Counter | `subscriber_live_to_historic`  | Live‑to‑historic stream transitions                            |
| Gauge   | `subscriber_open_connections`  | Connected subscribers                                          |
| Gauge   | `subscriber_newest_block_sent` | Newest block number sent (lag indicator vs. publisher inbound) |
| Counter | `subscriber_errors`            | Errors while streaming to subscribers                          |

---

## Verification

**Plugin:** `verification [block-node-verification]`
Measures block‑verification throughput and success rate.

|  Type   |              Name              |             Description             |
|---------|--------------------------------|-------------------------------------|
| Counter | `verification_blocks_received` | Blocks received for verification    |
| Counter | `verification_blocks_verified` | Blocks that passed verification     |
| Counter | `verification_blocks_failed`   | Blocks that failed verification     |
| Counter | `verification_blocks_error`    | Internal errors during verification |
| Counter | `verification_block_time`      | Verification time per block (ms)    |

---

## files.recent

**Plugin:** `block-providers/files.recent [block-node-blocks-file-recent]`
Activity and utilization of the recent on‑disk tier.

|  Type   |                Name                |           Description           |
|---------|------------------------------------|---------------------------------|
| Counter | `files_recent_blocks_written`      | Blocks written to recent tier   |
| Counter | `files_recent_blocks_read`         | Blocks read from recent tier    |
| Counter | `files_recent_blocks_deleted`      | Blocks deleted from recent tier |
| Gauge   | `files_recent_blocks_stored`       | Blocks stored in recent tier    |
| Gauge   | `files_recent_total_bytes_storerd` | Bytes stored in recent tier     |

---

## files.historic

**Plugin:** `block-providers/files.historic [block-node-blocks-file-historic`
Activity and utilization of the historic on‑disk tier.

|  Type   |                Name                 |            Description            |
|---------|-------------------------------------|-----------------------------------|
| Counter | `files_historic_blocks_written`     | Blocks written to historic tier   |
| Counter | `files_historic_blocks_read`        | Blocks read from historic tier    |
| Counter | `files_historic_blocks_deleted`     | Blocks deleted from historic tier |
| Gauge   | `files_historic_blocks_stored`      | Blocks stored in historic tier    |
| Gauge   | `files_historic_total_bytes_stored` | Bytes stored in historic tier     |

---

## cloud.historic

**Plugin:** `block-providers/cloud.historic [block-node-blocks-cloud-historic]`
Activity and utilization of the cloud on‑disk tier.

|  Type   |                Name                 |          Description           |
|---------|-------------------------------------|--------------------------------|
| Counter | `cloud_historic_blocks_written`     | Blocks written to cloud tier   |
| Counter | `cloud_historic_blocks_read`        | Blocks read from cloud tier    |
| Counter | `cloud_historic_blocks_deleted`     | Blocks deleted from cloud tier |
| Gauge   | `cloud_historic_blocks_stored`      | Blocks stored in cloud tier    |
| Gauge   | `cloud_historic_total_bytes_stored` | Bytes stored in cloud tier     |

---

## s3‑archive

**Plugin:** `s3-archive [hiero-block-node.s3-archive]`
Tracks long‑term archival jobs that push blocks to S3.

|  Type   |              Name               |              Description               |
|---------|---------------------------------|----------------------------------------|
| Counter | `s3_archive_blocks_written`     | Blocks archived to S3                  |
| Gauge   | `s3_archive_latest_block`       | Latest block number archived           |
| Counter | `s3_archive_tasks_failed_total` | Failed archival tasks                  |
| Counter | `s3_archive_tasks_sucess_total` | Successful archival tasks              |
| Gauge   | `s3_archive_total_bytes_stored` | Total bytes stored in S3 (cost metric) |
