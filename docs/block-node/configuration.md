# Hiero Block Node Configuration Document

## Table of Contents

- [Overview](#overview)
- [Core Configuration Options](#core-configuration-options)
- [Server Configuration](#server-configuration)
- [Application State Configuration](#application-state-configuration)
- [Metrics Endpoint Configuration](#metrics-endpoint-configuration)
- [Configurations By Plugin](#configurations-by-plugin)
  - [Cloud Storage Archive Plugin Configuration](#cloud-storage-archive-plugin-configuration)
  - [Backfill Plugin Configuration](#backfill-plugin-configuration)
  - [Block Access Plugin Configuration](#block-access-plugin-configuration)
  - [Files Historic Plugin Configuration](#files-historic-plugin-configuration)
  - [Files Recent Plugin Configuration](#files-recent-plugin-configuration)
  - [Health Plugin Configuration](#health-plugin-configuration)
  - [Messaging Plugin Configuration](#messaging-plugin-configuration)
  - [Archive Plugin Configuration (S3 Archive)](#archive-plugin-configuration-s3-archive)
  - [Server Status Plugin Configuration](#server-status-plugin-configuration)
  - [Publisher Plugin Configuration](#publisher-plugin-configuration)
  - [RSA Bootstrap Plugin Configuration](#rsa-bootstrap-plugin-configuration)
  - [Subscriber Plugin Configuration](#subscriber-plugin-configuration)
  - [TSS Bootstrap Plugin Configuration](#tss-bootstrap-plugin-configuration)
  - [Verification Plugin Configuration](#verification-plugin-configuration)
  - [Cloud Storage Expanded Plugin Configuration](#cloud-storage-expanded-plugin-configuration)

## Overview

This document outlines configuration options for the Hiero Block Node. Most settings are controlled via environment variables for flexible deployment.

Each plugin has its own properties, but this focuses on core options and core plugins.

## Core Configuration Options

| ENV Variable                      | Description                                                                                 | Default |
|:----------------------------------|:--------------------------------------------------------------------------------------------|--------:|
| BLOCK_NODE_EARLIEST_MANAGED_BLOCK | Earliest block managed by this node. Older blocks may exist but won’t be fetched or stored. |       0 |

### Server Configuration

| ENV Variable                            | Description                                  |    Default |
|:----------------------------------------|:---------------------------------------------|-----------:|
| SERVER_MAX_MESSAGE_SIZE_BYTES           | Max message size (bytes) for HTTP/2.         | 37,748,736 |
| SERVER_SOCKET_SEND_BUFFER_SIZE_BYTES    | Send buffer size (bytes).                    |      32768 |
| SERVER_SOCKET_RECEIVE_BUFFER_SIZE_BYTES | Receive buffer size (bytes).                 |  8,388,608 |
| SERVER_PORT                             | Server listening port.                       |      40840 |
| SERVER_SHUTDOWN_DELAY_MILLIS            | Delay before shutdown (ms).                  |        500 |
| SERVER_MAX_TCP_CONNECTIONS              | Max TCP connections allowed.                 |       1000 |
| SERVER_IDLE_CONNECTION_PERIOD_MINUTES   | Period for idle connections check (minutes). |          5 |
| SERVER_IDLE_CONNECTION_TIMEOUT_MINUTES  | Timeout for idle connections (minutes).      |         30 |

### WebServerHttp2 Configuration

| ENV Variable                          | Description                                                                  |   Default |
|:--------------------------------------|:-----------------------------------------------------------------------------|----------:|
| SERVER_HTTP2_FLOW_CONTROL_TIMEOUT     | Outbound flow control blocking timeout (ms).                                 |       500 |
| SERVER_HTTP2_INITIAL_WINDOW_SIZE      | Sender's maximum window size (bytes) for stream-level flow control.          | 1,048,576 |
| SERVER_HTTP2_MAX_CONCURRENT_STREAMS   | Max concurrent streams the server will allow.                                |         8 |
| SERVER_HTTP2_MAX_EMPTY_FRAMES         | Max consecutive empty frames allowed on connection.                          |        10 |
| SERVER_HTTP2_MAX_FRAME_SIZE           | Largest frame payload size (bytes) the sender is willing to receive.         |   524,288 |
| SERVER_HTTP2_MAX_HEADER_LIST_SIZE     | Max field section size (bytes) the sender is prepared to accept.             |     8,192 |
| SERVER_HTTP2_MAX_RAPID_RESETS         | Max rapid resets (stream RST sent by client before any data sent by server). |        50 |
| SERVER_HTTP2_RAPID_RESET_CHECK_PERIOD | Period for counting rapid resets (ms).                                       |    10,000 |

### Application State Configuration

| ENV Variable                         | Description                                                                                     |                       Default                        |
|:-------------------------------------|:------------------------------------------------------------------------------------------------|------------------------------------------------------|
| APP_STATE_TSS_DATA_FILE_PATH         | Path where TSS data (ledger ID, address book, WRAPS VK) is persisted across restarts.           | /opt/hiero/block-node/node/app-state-data.bin        |
| APP_STATE_STORED_BLOCKS_FILE_PATH    | Path where the set of stored block ranges is persisted. Written every 1,000 blocks received.    | /opt/hiero/block-node/node/stored-blocks-data.bin    |
| APP_STATE_AVAILABLE_BLOCKS_FILE_PATH | Path where the set of available block ranges is persisted. Written every 1,000 blocks received. | /opt/hiero/block-node/node/available-blocks-data.bin |
| APP_STATE_UPDATE_SCAN_INTERVAL       | How often (ms) the application state facility checks for pending TSS data updates. Minimum 100. | 500                                                  |
| APP_STATE_UPDATE_INITIAL_DELAY       | Delay (ms) before the application state facility begins its first scan. Minimum 100.            | 100                                                  |

Stored blocks are all blocks reported as persisted by any plugin. Available blocks are the subset
also reported as retrievable (i.e. by a `BlockProviderPlugin`). Both range sets are loaded at
startup and persisted to disk automatically.

### Metrics Endpoint Configuration

The metrics HTTP server is provided by the `hiero-metrics` library and exposes
Prometheus-format metrics. Its properties are prefixed with
`metrics.exporter.openmetrics.http.` and can be set via:

- `app.properties` (classpath, lowest priority)
- JVM system properties (`-D` flags via `JAVA_TOOL_OPTIONS`, highest priority)
- Helm chart `blockNode.metrics.*` values (which inject `-D` flags automatically)

| Chart Value                  | JVM Property                                 | Description                         |  Default |
|:-----------------------------|:---------------------------------------------|:------------------------------------|---------:|
| `blockNode.metrics.hostname` | `metrics.exporter.openmetrics.http.hostname` | Bind address for the metrics server |  0.0.0.0 |
| `blockNode.metrics.port`     | `metrics.exporter.openmetrics.http.port`     | Prometheus endpoint port            |    16007 |
| `blockNode.metrics.path`     | `metrics.exporter.openmetrics.http.path`     | HTTP path for metrics endpoint      | /metrics |

> **Note:** These properties come from the `hiero-metrics` library, not from a Block
> Node `@ConfigData` record. They cannot be set via environment variable
> mapping (`AutomaticEnvironmentVariableConfigSource`). The chart injects them as
> JVM system properties through `JAVA_TOOL_OPTIONS`.

## Configurations By Plugin

### Cloud Storage Archive Plugin Configuration

| ENV Variable                         | Description                                                                                                                                                                                              |    Default |
|:-------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------:|
| CLOUD_STORAGE_ARCHIVE_GROUPING_LEVEL | Files per archive in powers of ten (1=10, 2=100, …, 6=1,000,000).                                                                                                                                        |          5 |
| CLOUD_STORAGE_ARCHIVE_PART_SIZE_MB   | The size of each multi-part upload part in megabytes. Minimum value is 5, maximum value is 2047                                                                                                          |         10 |
| CLOUD_STORAGE_ARCHIVE_ENDPOINT_URL   | Endpoint URL for the cloud archive service (e.g., `https://s3.amazonaws.com/`).                                                                                                                          |         "" |
| CLOUD_STORAGE_ARCHIVE_BUCKET_NAME    | Bucket name where cloud archive files are stored.                                                                                                                                                        |         "" |
| CLOUD_STORAGE_ARCHIVE_STORAGE_CLASS  | Storage class (e.g., STANDARD, INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE). Values available at [AWS S3 storage classes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html) | "STANDARD" |
| CLOUD_STORAGE_ARCHIVE_REGION_NAME    | Region for the cloud archive service (e.g., `us-east-1`).                                                                                                                                                |         "" |
| CLOUD_STORAGE_ARCHIVE_ACCESS_KEY     | Access key for the archive service.                                                                                                                                                                      |         "" |
| CLOUD_STORAGE_ARCHIVE_SECRET_KEY     | Secret key for the archive service.                                                                                                                                                                      |         "" |

### Backfill Plugin Configuration

| ENV Variable                          | Description                                                     |   Default |
|:--------------------------------------|:----------------------------------------------------------------|----------:|
| BACKFILL_START_BLOCK                  | First block this BN deploy wants.                               |         0 |
| BACKFILL_END_BLOCK                    | Max block number, -1 means no limit.                            |        -1 |
| BACKFILL_BLOCK_NODE_SOURCES_PATH      | File path for BN sources (PBJ JSON `block-nodes.json`).         |        "" |
| BACKFILL_SCAN_INTERVAL                | Scan interval for gap detection (ms).                           |     60000 |
| BACKFILL_MAX_RETRIES                  | Max retries to fetch a block.                                   |         3 |
| BACKFILL_INITIAL_RETRY_DELAY          | Initial retry delay (ms), grows linearly.                       |      5000 |
| BACKFILL_FETCH_BATCH_SIZE             | Number of blocks per gRPC call.                                 |        10 |
| BACKFILL_DELAY_BETWEEN_BATCHES        | Delay (ms) between block batches.                               |      1000 |
| BACKFILL_INITIAL_DELAY                | Initial delay (ms) before starting backfill.                    |     15000 |
| BACKFILL_PER_BLOCK_PROCESSING_TIMEOUT | Timeout (ms) to wait for a block batch.                         |      1000 |
| BACKFILL_GRPC_OVERALL_TIMEOUT         | Overall gRPC timeout (connect, read, poll) in ms.               |     10000 |
| BACKFILL_MAX_INCOMING_BUFFER_SIZE     | Max gRPC incoming buffer size in bytes (min 10 MB, max 300 MB). | 104857600 |
| BACKFILL_ENABLE_TLS                   | Enable TLS if supported by block-node client.                   |     false |

**Note:** The following can be configured in the JSON file at `BACKFILL_BLOCK_NODE_SOURCES_PATH`:
- Per-node gRPC timeout overrides: `grpc_connect_timeout`, `grpc_read_timeout`, `grpc_poll_wait_time`
- Advanced HTTP/2 tuning: `grpc_webclient_tuning`

See [Backfill Plugin Design](../design/backfill-plugin.md#blocknode-sources-configuration-file-structure)
for the JSON schema.

### Block Access Plugin Configuration

Currently, no specific options.

### Files Historic Plugin Configuration

| ENV Variable                             | Description                                                   |                             Default |
|:-----------------------------------------|:--------------------------------------------------------------|------------------------------------:|
| FILES_HISTORIC_ROOT_PATH                 | Root path for saving historic blocks.                         | /opt/hiero/block-node/data/historic |
| FILES_HISTORIC_COMPRESSION               | Compression type (e.g., ZSTD).                                |                                     |
| FILES_HISTORIC_POWERS_OF_TEN             | Files per zip in powers of ten (1=10, 2=100, …, 6=1,000,000). |                                   4 |
| FILES_HISTORIC_BLOCK_RETENTION_THRESHOLD | Number of zips to retain. 0 means keep indefinitely.          |                                   0 |
| FILES_HISTORIC_MAX_FILES_PER_DIR         | Max files per directory to avoid filesystem issues.           |                                   3 |

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

| ENV Variable                              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                    |                   Default |
|:------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------:|
| PRODUCER_BATCH_FORWARD_LIMIT              | Max number of blocks to forward in a batch. Must be ≥ 100,000.                                                                                                                                                                                                                                                                                                                                                                                                 | 9,223,372,036,854,775,807 |
| PRODUCER_PUBLISHER_UNAVAILABILITY_TIMEOUT | The time in seconds to wait when we have no active publishers before sending a publisher unavailability timeout status update. Must be ≥ 0.                                                                                                                                                                                                                                                                                                                    |                       300 |
| PRODUCER_STALE_RESEND_PRUNE_BUFFER        | Number of blocks behind `lastPersistedBlockNumber` that a `blocksToResend` entry may sit before `handlePersisted` prunes it. Entries within the buffer are still considered fillable by a publisher; entries older than the buffer are dropped (the gap is owned by backfill at that point). Set to ~CN block-history depth. Must be 0 ≤ value ≤ 200.                                                                                                          |                       100 |
| PRODUCER_FLOW_CONTROL_PAUSE_DELAY_NANOS   | Duration in nanoseconds that `onNext` parks the request-processing thread when a request arrives while the handler is paused. The handler is paused as part of the staged shutdown sequence, which schedules the actual shutdown to run after a fixed delay; parking the next incoming request prevents it from racing that pending shutdown and lets any final outbound messages flush before the connection closes. Must be 100,000 ≤ value ≤ 5,000,000,000. |               100,000,000 |
| PRODUCER_DUPLICATE_BLOCK_SKIP_WINDOW      | Number of blocks behind `lastPersistedBlockNumber` for which a duplicate block header is answered with `SkipBlock` instead of `EndOfStream(DUPLICATE_BLOCK)`. A publisher only slightly behind can fast-forward without reconnecting; duplicates further behind than the window still close the stream so the publisher reconnects. Must be 1 ≤ value ≤ 10.                                                                                                    |                         5 |

### RSA Bootstrap Plugin Configuration

| ENV Variable                                             | Description                                                                                  | Default |
|:---------------------------------------------------------|:---------------------------------------------------------------------------------------------|--------:|
| ROSTER_BOOTSTRAP_RSA_MIRROR_NODE_BASE_URL                | The URL of the mirror node from which to request the node address book.                      |      "" |
| ROSTER_BOOTSTRAP_RSA_INITIAL_QUERY_INTERVAL_MILLIS       | The initial period between queries to the mirror node until a node address book is found.    |    5000 |
| ROSTER_BOOTSTRAP_RSA_SUBSEQUENT_QUERY_INTERVAL_MILLIS    | The subsequent period between queries to the mirror node after a node address book is found. |   60000 |
| ROSTER_BOOTSTRAP_RSA_MIRROR_NODE_CONNECT_TIMEOUT_SECONDS | TCP connect timeout when calling the Mirror Node.                                            |       5 |
| ROSTER_BOOTSTRAP_RSA_MIRROR_NODE_READ_TIMEOUT_SECONDS    | Per-request read timeout when calling the Mirror Node.                                       |      10 |
| ROSTER_BOOTSTRAP_RSA_MIRROR_NODE_PAGE_SIZE               | Number of nodes requested per paginated Mirror Node call.                                    |     100 |

### Subscriber Plugin Configuration

| ENV Variable                           | Description                                                                                            | Default |
|:---------------------------------------|:-------------------------------------------------------------------------------------------------------|--------:|
| SUBSCRIBER_LIVE_QUEUE_SIZE             | Queue size (in batches) for transferring live data between messaging and client threads. Must be ≥100. |    4000 |
| SUBSCRIBER_MAXIMUM_FUTURE_REQUEST      | Max blocks ahead of latest "live" block a request can start from. Must be ≥10.                         |    4000 |
| SUBSCRIBER_MINIMUM_LIVE_QUEUE_CAPACITY | Minimum free capacity in the live queue before dropping oldest blocks. Typically ~10% of queue size.   |     400 |

### TSS Bootstrap Plugin Configuration

| ENV Variable                                  | Description                                                                               |   Default |
|:----------------------------------------------|:------------------------------------------------------------------------------------------|----------:|
| ROSTER_BOOTSTRAP_TSS_BLOCK_NODE_SOURCES_PATH  | File path to the JSON file containing a list of block node servers to query for TSS data. |        "" |
| ROSTER_BOOTSTRAP_TSS_QUERY_PEER_INTERVAL      | The amount of time in milliseconds between queries to the Peer Block Nodes for TSS data.  |     60000 |
| ROSTER_GRPC_OVERALL_TIMEOUT                   | Overall gRPC timeout (connect, read, poll) in ms.                                         |     10000 |
| ROSTER_BOOTSTRAP_TSS_MAX_INCOMING_BUFFER_SIZE | Maximum block size used for the BlockNode Client                                          | 104857600 |
| ROSTER_BOOTSTRAP_TSS_ENABLE_TLS               | Flag indicating whether TLS should be enabled for the BlockNode client.                   |     false |

### Verification Plugin Configuration

| ENV Variable                                        | Description                                                                    |                                                            Default |
|:----------------------------------------------------|:-------------------------------------------------------------------------------|-------------------------------------------------------------------:|
| VERIFICATION_ALL_BLOCKS_HASHER_ENABLED              | Enable the all-blocks hasher to compute and verify a rolling root hash.        |                                                              false |
| VERIFICATION_ALL_BLOCKS_HASHER_FILE_PATH            | Path to the persisted root hash file for all previous blocks.                  | /opt/hiero/block-node/verification/rootHashOfAllPreviousBlocks.bin |
| VERIFICATION_ALL_BLOCKS_HASHER_PERSISTENCE_INTERVAL | How often (in blocks) the hasher persists its state to disk.                   |                                                                 10 |
| VERIFICATION_TSS_PARAMETERS_FILE_PATH               | Path to the persisted TSS parameters file (ledger ID, address book, WRAPS VK). |              /opt/hiero/block-node/verification/tss-parameters.bin |

> **Note:** `VERIFICATION_ALL_BLOCKS_HASHER_ENABLED` must remain `false` (the default).
> The all-blocks hasher requires a strictly sequential block stream; out-of-order or
> forward-arriving blocks will cause it to fall out of sync and produce incorrect root hashes.

### Cloud Storage Expanded Plugin Configuration

Uploads each verified block as a single ZSTD-compressed `.blk.zstd` object to any
S3-compatible store (AWS S3, GCS S3-interop, MinIO, etc.). The plugin is **disabled by
default** — setting `CLOUD_EXPANDED_ENDPOINT_URL` to a non-empty value activates it.

| ENV Variable                                  | Description                                                                                     |           Default |
|:----------------------------------------------|:------------------------------------------------------------------------------------------------|------------------:|
| CLOUD_STORAGE_EXPANDED_ENDPOINT_URL           | S3-compatible endpoint URL. **Blank disables the plugin.**                                      |                "" |
| CLOUD_STORAGE_EXPANDED_BUCKET_NAME            | Name of the S3 bucket where blocks are stored.                                                  | block-node-blocks |
| CLOUD_STORAGE_EXPANDED_OBJECT_KEY_PREFIX      | Prefix prepended to every object key (e.g. `blocks`).                                           |            blocks |
| CLOUD_STORAGE_EXPANDED_STORAGE_CLASS          | S3 storage class for uploaded objects. Must be `STANDARD` for the current bucky-client version. |          STANDARD |
| CLOUD_STORAGE_EXPANDED_REGION_NAME            | AWS / S3-compatible region name.                                                                |         us-east-1 |
| CLOUD_STORAGE_EXPANDED_ACCESS_KEY             | S3 access key (not logged).                                                                     |                "" |
| CLOUD_STORAGE_EXPANDED_SECRET_KEY             | S3 secret key (not logged).                                                                     |                "" |
| CLOUD_STORAGE_EXPANDED_UPLOAD_TIMEOUT_SECONDS | Max seconds per block upload before treating the upload as failed.                              |                60 |

Object keys follow the format `{prefix}/AAAA/BBBB/CCCC/DDDD/EEE.blk.zstd`, where the
19-digit zero-padded block number is split into a 4/4/4/4/3 folder hierarchy:

| Block number | Object key                                |
|:-------------|:------------------------------------------|
| 1            | `blocks/0000/0000/0000/0000/001.blk.zstd` |
| 1 234 567    | `blocks/0000/0000/0000/1234/567.blk.zstd` |
| 108 273 182  | `blocks/0000/0000/0010/8273/182.blk.zstd` |
