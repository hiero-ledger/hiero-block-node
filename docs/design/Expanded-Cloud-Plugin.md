# Expanded Cloud Storage Plugin

## Table of Contents

1. [Purpose](#purpose)
2. [Goals](#goals)
3. [Terms](#terms)
4. [Entities](#entities)
5. [Design](#design)
6. [Diagram](#diagram)
7. [Configuration](#configuration)
8. [Metrics](#metrics)
9. [Exceptions](#exceptions)
10. [Acceptance Tests](#acceptance-tests)

## Purpose

The Expanded Cloud Storage Plugin (ECSP) is a data storage plugin for the block
node that stores individual block files in cloud storage systems.

## Goals

* The ECSP must store every block, as received, after verification.
* The ECSP must store each block as a single ZStandard-compressed file.
* The ECSP must adhere to a file pattern as defined below.
* The ECSP must store all blocks as files or objects in a cloud storage system.
* The ECSP must not report success until data is stored such that it can be
  recovered if the local system fails unexpectedly, including a failure that
  results in complete and unrecoverable loss of all local storage.

## Terms

<dl>
  <dt>Cloud Storage</dt>
  <dd>Any storage API that stores data remotely with very high
      availability and reliability. Multiple such APIs may be supported
      by the plugin and controlled by configuration.<br/>
      An example of a common cloud storage API is S3 storage.</dd>
</dl>

## Entities

TBD.

## Design

1. The `ExpandedCloudPlugin.handleVerificationNotification()` receives a full
   block recently verified.
2. A new `SingleBlockStoreTask` is created, provided with the verified block,
   and added to a Completion Service.
3. Each `SingleBlockStoreTask` calculates the correct file pattern, opens a
   connection or session to the cloud storage service, stores the block
   as a ZStandard compressed object or file, then closes the connection
   or session.
   1. We may chose to implement some form of connection or session pooling
      to avoid rate limits, reduce costs, reduce latency, and/or
      improve throughput.
4. The `ExpandedCloudPlugin` will periodically query the Completion Service to
   gather completed storage results, handled retries, and report failures.
   1. The plugin will _also_ check for completion immediately _prior_ to
      handling each validation notification, and try to clear completed tasks
      before adding new tasks. This check might also handle ensuring any
      connection or session pool is managed effectively.
   2. On failure a `PerisistenceNotification` will be published with
      `success=false`.
   3. On success a `PerisistenceNotification` will be published with
      `success=true`.

File/object pattern

```text
19 digit block number with a suffix of '.blk.zstd'.
Block number split into groups of 4 digit folders, starting from the left.

examples:
Block         "1" = 0000/0000/0000/0000/001.blk.zstd
Block "108273182" = 0000/0000/0010/8273/182.blk.zstd
```

## Diagram

TBD.

## Configuration

TBD.

## Metrics

TBD.

## Exceptions

TBD.

## Acceptance Tests

TBD.
