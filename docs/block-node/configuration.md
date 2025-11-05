# Hiero Block Node Configuration Document

## Table of Contents

- [Overview](#overview)
- [Core Configuration Options](#core-configuration-options)
- [Server Configuration](#server-configuration)
- [Metrics Endpoint Configuration](#metrics-endpoint-configuration)
- [Configurations By Plugin](#configurations-by-plugin)
  - [Backfill Plugin Configuration](#backfill-plugin-configuration)
  - [Block Access Plugin Configuration](#block-access-plugin-configuration)
  - [Files Historic Plugin Configuration](#files-historic-plugin-configuration)
  - [Files Recent Plugin Configuration](#files-recent-plugin-configuration)
  - [Health Plugin Configuration](#health-plugin-configuration)
  - [Messaging Plugin Configuration](#messaging-plugin-configuration)
  - [Archive Plugin Configuration (S3 Archive)](#archive-plugin-configuration-s3-archive)
  - [Server Status Plugin Configuration](#server-status-plugin-configuration)
  - [Publisher Plugin Configuration](#publisher-plugin-configuration)
  - [Subscriber Plugin Configuration](#subscriber-plugin-configuration)
  - [Verification Plugin Configuration](#verification-plugin-configuration)

## Overview

This document outlines configuration options for the Hiero Block Node. Most settings are controlled via environment variables for flexible deployment.

Each plugin has its own properties, but this focuses on core options and core plugins.

## Core Configuration Options

| ENV Variable                      | Description                                                                                 | Default |
|:----------------------------------|:--------------------------------------------------------------------------------------------|--------:|
| BLOCK_NODE_EARLIEST_MANAGED_BLOCK | Earliest block managed by this node. Older blocks may exist but won’t be fetched or stored. |       0 |

### Server Configuration

| ENV Variable                            | Description                                  |   Default |
|:----------------------------------------|:---------------------------------------------|----------:|
| SERVER_MAX_MESSAGE_SIZE_BYTES           | Max message size (bytes) for HTTP/2.         | 4,194,304 |
| SERVER_SOCKET_SEND_BUFFER_SIZE_BYTES    | Send buffer size (bytes).                    |     32768 |
| SERVER_SOCKET_RECEIVE_BUFFER_SIZE_BYTES | Receive buffer size (bytes).                 |     32768 |
| SERVER_PORT                             | Server listening port.                       |     40840 |
| SERVER_SHUTDOWN_DELAY_MILLIS            | Delay before shutdown (ms).                  |       500 |
| SERVER_MAX_TCP_CONNECTIONS              | Max TCP connections allowed.                 |      1000 |
| SERVER_IDLE_CONNECTION_PERIOD_MINUTES   | Period for idle connections check (minutes). |         5 |
| SERVER_IDLE_CONNECTION_TIMEOUT_MINUTES  | Timeout for idle connections (minutes).      |        30 |

### WebServerHttp2 Configuration

| ENV Variable                          | Description                                                                  |   Default |
|:--------------------------------------|:-----------------------------------------------------------------------------|----------:|
| SERVER_HTTP2_FLOW_CONTROL_TIMEOUT     | Outbound flow control blocking timeout (ms).                                 |      1000 |
| SERVER_HTTP2_INITIAL_WINDOW_SIZE      | Sender's maximum window size (bytes) for stream-level flow control.          | 1,048,576 |
| SERVER_HTTP2_MAX_CONCURRENT_STREAMS   | Max concurrent streams the server will allow.                                |         8 |
| SERVER_HTTP2_MAX_EMPTY_FRAMES         | Max consecutive empty frames allowed on connection.                          |        10 |
| SERVER_HTTP2_MAX_FRAME_SIZE           | Largest frame payload size (bytes) the sender is willing to receive.         |   524,288 |
| SERVER_HTTP2_MAX_HEADER_LIST_SIZE     | Max field section size (bytes) the sender is prepared to accept.             |     8,192 |
| SERVER_HTTP2_MAX_RAPID_RESETS         | Max rapid resets (stream RST sent by client before any data sent by server). |        50 |
| SERVER_HTTP2_RAPID_RESET_CHECK_PERIOD | Period for counting rapid resets (ms).                                       |    10,000 |

### Metrics Endpoint Configuration

| ConfigKey                 | Description                          | Default |
|:--------------------------|:-------------------------------------|--------:|
| enableEndpoint            | Enable/disable Prometheus endpoint.  |    true |
| endpointPortNumber        | Prometheus endpoint port.            |   16007 |
| endpointMaxBacklogAllowed | Max queued incoming TCP connections. |       1 |

## Configurations By Plugin

### Backfill Plugin Configuration

| ENV Variable                          | Description                                   | Default |
|:--------------------------------------|:----------------------------------------------|--------:|
| BACKFILL_START_BLOCK                  | First block this BN deploy wants.             |       0 |
| BACKFILL_END_BLOCK                    | Max block number, -1 means no limit.          |      -1 |
| BACKFILL_BLOCK_NODE_SOURCES_PATH      | File path for BN sources (yaml).              |      "" |
| BACKFILL_SCAN_INTERVAL                | Scan interval for gaps (minutes).             |      60 |
| BACKFILL_MAX_RETRIES                  | Max retries to fetch a block.                 |       3 |
| BACKFILL_INITIAL_RETRY_DELAY          | Initial retry delay (ms), grows linearly.     |    5000 |
| BACKFILL_FETCH_BATCH_SIZE             | Number of blocks per gRPC call.               |      25 |
| BACKFILL_DELAY_BETWEEN_BATCHES        | Delay (ms) between block batches.             |    1000 |
| BACKFILL_INITIAL_DELAY                | Initial delay (s) before starting backfill.   |      15 |
| BACKFILL_PER_BLOCK_PROCESSING_TIMEOUT | Timeout (ms) per block to allow recovery.     |    1000 |
| BACKFILL_GRPC_OVERALL_TIMEOUT         | Overall gRPC timeout (connect, read, poll).   |   30000 |
| BACKFILL_ENABLE_TLS                   | Enable TLS if supported by block-node client. |   false |

### Block Access Plugin Configuration

Currently, no specific options.

### Files Historic Plugin Configuration

| ENV Variable                             | Description                                                   |                             Default |
|:-----------------------------------------|:--------------------------------------------------------------|------------------------------------:|
| FILES_HISTORIC_ROOT_PATH                 | Root path for saving historic blocks.                         | /opt/hiero/block-node/data/historic |
| FILES_HISTORIC_COMPRESSION               | Compression type (e.g., ZSTD).                                |                                     |
| FILES_HISTORIC_POWERS_OF_TEN             | Files per zip in powers of ten (1=10, 2=100, …, 6=1,000,000). |                                   4 |
| FILES_HISTORIC_BLOCK_RETENTION_THRESHOLD | Number of zips to retain. 0 means keep indefinitely.          |                                   0 |

### Files Recent Plugin Configuration

| ENV Variable                           | Description                                         |                         Default |
|:---------------------------------------|:----------------------------------------------------|--------------------------------:|
| FILES_RECENT_LIVE_ROOT_PATH            | Root path for saving live blocks.                   | /opt/hiero/block-node/data/live |
| FILES_RECENT_COMPRESSION               | Compression type (e.g., ZSTD).                      |                            ZSTD |
| FILES_RECENT_MAX_FILES_PER_DIR         | Max files per directory to avoid filesystem issues. |                               3 |
| FILES_RECENT_BLOCK_RETENTION_THRESHOLD | Block retention count. `0` means keep indefinitely. |                          96,000 |

### Health Plugin Configuration

Currently, no specific options.

### Messaging Plugin Configuration

| ENV Variable                            | Description                                                                               | Default |
|:----------------------------------------|:------------------------------------------------------------------------------------------|--------:|
| MESSAGING_BLOCK_ITEM_QUEUE_SIZE         | Max messages in block item queue. Each batch ~100 items. Must be power of 2.              |     512 |
| MESSAGING_BLOCK_NOTIFICATION_QUEUE_SIZE | Max block notifications queued. Each may hold a full block in memory. Must be power of 2. |      16 |

### Archive Plugin Configuration (S3 Archive)

| ENV Variable            | Description                                                                 |            Default |
|:------------------------|:----------------------------------------------------------------------------|-------------------:|
| ARCHIVE_BLOCKS_PER_FILE | Number of blocks per archive file. Must be a positive power of 10.          |             100000 |
| ARCHIVE_ENDPOINT_URL    | Endpoint URL for the archive service (e.g., `https://s3.amazonaws.com/`).   |                 "" |
| ARCHIVE_BUCKET_NAME     | Bucket name where archive files are stored.                                 | block-node-archive |
| ARCHIVE_BASE_PATH       | Base path inside the bucket for archive files.                              |             blocks |
| ARCHIVE_STORAGE_CLASS   | Storage class (e.g., STANDARD, INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE). |           STANDARD |
| ARCHIVE_REGION_NAME     | Region for the archive service (e.g., `us-east-1`).                         |          us-east-1 |
| ARCHIVE_ACCESS_KEY      | Access key for the archive service.                                         |                 "" |
| ARCHIVE_SECRET_KEY      | Secret key for the archive service.                                         |                 "" |

### Server Status Plugin Configuration

Currently, no specific options.

### Publisher Plugin Configuration

| ENV Variable                 | Description                                                    |                   Default |
|:-----------------------------|:---------------------------------------------------------------|--------------------------:|
| PRODUCER_BATCH_FORWARD_LIMIT | Max number of blocks to forward in a batch. Must be ≥ 100,000. | 9,223,372,036,854,775,807 |

### Subscriber Plugin Configuration

| ENV Variable                           | Description                                                                                            | Default |
|:---------------------------------------|:-------------------------------------------------------------------------------------------------------|--------:|
| SUBSCRIBER_LIVE_QUEUE_SIZE             | Queue size (in batches) for transferring live data between messaging and client threads. Must be ≥100. |    4000 |
| SUBSCRIBER_MAXIMUM_FUTURE_REQUEST      | Max blocks ahead of latest "live" block a request can start from. Must be ≥10.                         |    4000 |
| SUBSCRIBER_MINIMUM_LIVE_QUEUE_CAPACITY | Minimum free capacity in the live queue before dropping oldest blocks. Typically ~10% of queue size.   |     400 |

### Verification Plugin Configuration

Currently, no specific options.
