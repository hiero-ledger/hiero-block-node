# E2E Test Plan - `Files Historic Persistence`

## Overview

Persistence is a key component of the Block-Node. We need to ensure that we have
good and reliable persistence plugins that can be used by the Block-Node. We
support local file system archive of received and verified blocks
which we call `Files Historic Persistence`. Also, complementary to the persistence
capability itself, we have the ability to retrieve (read) blocks that have been
archived locally. `Block Provision` is another key component of the Block-Node.
The `Files Historic Persistence` is however an optional component, meaning that
the system is able to operate without it present and loaded.

This E2E test plan is designed to highlight the key scenarios that need to be
tested for end results in order to ensure the correctness and proper working of
the `Files Historic Persistence` as well as the complementary `Block Provision`
logic. Essentially, this test plan describes the intended behavior of the
`Files Historic Persistence` and the expected results it produces.

## Key Considerations for `Files Historic Persistence`

- **Batching of Blocks to Archive**: `Files Historic Persistence` will archive
  blocks in batches. This means that it will collect a number of blocks
  (configurable amount) and then archive them together in a single zip.
- **Batch Size**: The configurable batch size is a power of 10. This done to
  complement a trie data structure that is used for path resolution. Also,
  this means that a batch will always start with a number that is a product of
  the configured batch size and will end with `startNumber + batchSize - 1`,
  e.g. if batch size is 1000, we will see 0-999, 1000-1999, 2000-2999 etc.
- **No Gaps Allowed**: The `Files Historic Persistence` will not allow gaps in
  the zips. This means that we have a predictable start and end number for each
  batch and also, we are certain that if a zip is created successfully, it
  contains all the blocks from the batch.
- **Path Resolution Logic**: `Files Historic Persistence` relies on path
  resolution logic which determines the location of a given zip for each
  blockNumber.
- **Trie Data Structure for Path Resolution**: `Files Historic Persistence`
  relies on a trie data structure for path resolution. The trie structure
  is designed to efficiently resolve paths for blocks based on their
  blockNumber. Each block is stored in a zip file, and the path to that zip
  is determined by the blockNumber, based on configured batch size. For example,
  if we have a batch size of 1000, the path for the zip file where block with
  blockNumber `0000000000000001234` resides would be same as the path for block
  with blockNumber `0000000000000001235`, as they both belong to the same batch
  (1000-1999). An example path for that zip file could be:
  `/blocks/000/000/000/000/000/1000s.zip` if the batch size is 1000.
- **Files Historic Root Path**: This is the root path (configurable) where all
  blocks will be archived. Generally, the stored blocks are long-lived as this
  type of persistence is used for long-term storage of blocks.
- **Scope**: The `Files Historic Persistence` will handle Persisted
  Notifications that are arriving via the node's messaging system. It will then
  proceed to determine if it can archive the next batch or not. If it can, it
  will proceed to archive the batch in a zip file, using the node's
  `Block Provision` facility to supply the blocks that need to be archived.
  Finally, it will publish the result of the archive operation to the node's
  messaging system as a Persisted Notification, only if the archive was
  successful. An important clarification is that the
  `Files Historic Persistence` will only batch and archive blocks that have
  been persisted by other plugins with higher priority!
- **Persistence Results**: `Files Historic Persistence` publishes persistence
  results to the node's messaging system when a successful archive happens.
  These results will be pushed to subscribed handlers.

## Test Scenarios

> _**NOTE**: for the purpose of the below test definitions, we will have the
> following root definition (this could be configured to be different, but
> whatever is configured is of no relevance to the outcome of the tests, simply
> we need a reference point so an example could be shown):_
> `Files Historic Root`: `/blocks`
>
> _**NOTE**: the trie structure for `Files Recent Root` is resolving
> all the digits of a `long` (max. 19 digits). We have three digits per node
> or directory. Based on archive size, the max. depth of directories vary. After
> we have reached our max. depth, we will then have an arbitrary number of zip
> files, each named with the batch's start number suffixed with `s.zip`.
> As an example, if we have a batch size of 1000, we will have the following
> resolutions:
> </br>
> `/blocks/000/000/000/000/000/0000s.zip` (first thousand 0-999)
> </br>
> `/blocks/000/000/000/000/000/1000s.zip` (second thousand 1000-1999)
> </br>
> etc._
>
> _**NOTE**: assume that for each test the configured batch size is `10` in
> order to have a more manageable environment. An example resolution for a
> zip would be:
> </b>
> `/blocks/000/000/000/000/000/00/00s.zip` (first ten 0-9)
> `/blocks/000/000/000/000/000/00/10s.zip` (second ten 10-19)
> </b>
> etc._
>
> _**NOTE**: assume that the Block-Node under test is configured to have the
> compression algorithm set to `ZStandard` so the file extension for an archived
> block will be `.blk.zstd`_

|                      Test Case ID | Test Name                            | Implemented (Y/N) |
|----------------------------------:|:-------------------------------------|:-----------------:|
| [E2ETC_FHP_0001](#E2ETC_FHP_0001) | `Verify Archived Batch Zip Location` |         N         |
| [E2ETC_FHP_0002](#E2ETC_FHP_0002) | `Verify Archived Batch Zip Content`  |         N         |
| [E2ETC_FHP_0003](#E2ETC_FHP_0003) | `Verify No Gaps in Zip`              |         N         |
| [E2ETC_FHP_0004](#E2ETC_FHP_0004) | `Verify Cleanup on IO Failure`       |         N         |
| [E2ETC_FHP_0005](#E2ETC_FHP_0005) | `Verify Immutable Zips`              |         N         |

---

### E2ETC_FHP_0001

#### Test Name

`Verify Archived Batch Zip Location`

#### Scenario Description

Blocks are persisted in range `0-9` (the only ones currently in existence).
A Persisted Notification is published to the node's messaging system for blocks
persisted in range `0-9` with priority higher than the
`Files Historic Persistence`'s . The `Files Historic Persistence` will then
proceed to archive the batch of blocks in a zip file.

#### Requirements

It is expected that the `Files Historic Persistence` will archive the batch
successfully creating a zip file at the resolved location.

#### Preconditions

A running plugin in the Block-Node under test that is able to publish a
Persisted Notification for blocks persisted in range `0-9` with priority
higher than the `Files Historic Persistence`'s.

#### Input

Persisted Notification for blocks persisted in range `0-9` with priority
higher than the `Files Historic Persistence`'s.

#### Output

Regular file: `/blocks/000/000/000/000/000/00/00s.zip` exists.

#### Other

N/A

---

### E2ETC_FHP_0002

#### Test Name

`Verify Archived Batch Zip Content`

#### Scenario Description

Blocks are persisted in range `0-9` (the only ones currently in existence).
A Persisted Notification is published to the node's messaging system for blocks
persisted in range `0-9` with priority higher than the
`Files Historic Persistence`'s . The `Files Historic Persistence` will then
proceed to archive the batch of blocks in a zip file.

#### Requirements

It is expected that the `Files Historic Persistence` will archive the batch
successfully creating a zip file at the resolved location.

#### Preconditions

A running plugin in the Block-Node under test that is able to publish a
Persisted Notification for blocks persisted in range `0-9` with priority
higher than the `Files Historic Persistence`'s.

#### Input

Persisted Notification for blocks persisted in range `0-9` with priority
higher than the `Files Historic Persistence`'s.

#### Output

Regular file: `/blocks/000/000/000/000/000/00/00s.zip` contains 10 entries
for each block `0000000000000000000.blk.zstd` to `0000000000000000009.blk.zstd`.
The binary content of each block is the same as the original block that was
persisted.

#### Other

N/A

---

### E2ETC_FHP_0003

#### Test Name

`Verify No Gaps in Zip`

#### Scenario Description

Blocks are persisted in range `1-9` (the only ones currently in existence).
A Persisted Notification is published to the node's messaging system for blocks
persisted in range `1-9` with priority higher than the
`Files Historic Persistence`'s . The `Files Historic Persistence` will then
proceed to not archive the batch of blocks in a zip file.

#### Requirements

It is expected that the `Files Historic Persistence` will not archive the batch
as there is a gap in the batch that is next to be archived. No files or data
are expected to be persisted anywhere on the file system.

#### Preconditions

A running plugin in the Block-Node under test that is able to publish a
Persisted Notification for blocks persisted in range `1-9` with priority
higher than the `Files Historic Persistence`'s.

#### Input

Persisted Notification for blocks persisted in range `1-9` with priority
higher than the `Files Historic Persistence`'s.

#### Output

Regular file: `/blocks/000/000/000/000/000/00/00s.zip` does not exist.

#### Other

N/A

---

### E2ETC_FHP_0004

#### Test Name

`Verify Cleanup on IO Failure`

#### Scenario Description

Blocks are persisted in range `0-9` (the only ones currently in existence).
A Persisted Notification is published to the node's messaging system for blocks
persisted in range `0-9` with priority higher than the
`Files Historic Persistence`'s . The `Files Historic Persistence` will then
proceed to archive the batch of blocks in a zip file.

#### Requirements

It is expected that the `Files Historic Persistence` will clean up any potential
data and side effects produced during an attempt to archive a batch, but an IO
issue has occurred during the archive attempt.

#### Preconditions

A running plugin in the Block-Node under test that is able to publish a
Persisted Notification for blocks persisted in range `0-9` with priority
higher than the `Files Historic Persistence`'s. A way to simulate an IO issue
during the archive attempt.

#### Input

Persisted Notification for blocks persisted in range `0-9` with priority
higher than the `Files Historic Persistence`'s.

#### Output

Regular file: `/blocks/000/000/000/000/000/00/00s.zip` does not exist.

#### Other

N/A

---

### E2ETC_FHP_0005

#### Test Name

`Verify Immutable Zips`

#### Scenario Description

Blocks are persisted in range `0-9` (the only ones currently in existence).
A Persisted Notification is published to the node's messaging system for blocks
persisted in range `0-9` with priority higher than the
`Files Historic Persistence`'s . The `Files Historic Persistence` will then
proceed to archive the batch of blocks in a zip file. The zip file is immutable
and cannot be modified after it has been created.

#### Requirements

It is expected that the `Files Historic Persistence` will archive the batch
successfully creating a zip file at the resolved location, and that the zip
file is immutable, meaning that it cannot be modified after it has been created.

#### Preconditions

A running plugin in the Block-Node under test that is able to publish a
Persisted Notification for blocks persisted in range `0-9` with priority
higher than the `Files Historic Persistence`'s. A subsequent publish of the same
notification is made after the first zip is successfully created.

#### Input

Persisted Notification for blocks persisted in range `0-9` with priority
higher than the `Files Historic Persistence`'s. A subsequent publish of the same
notification is made after the first zip is successfully created.

#### Output

Regular file: `/blocks/000/000/000/000/000/00/00s.zip` exists and is immutable.

#### Other

N/A

---
