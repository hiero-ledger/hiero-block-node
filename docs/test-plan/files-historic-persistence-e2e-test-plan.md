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
  `/historic/000/000/000/000/000/1000s.zip` if the batch size is 1000.
- **Files Historic Root Path**: This is the root path (configurable) where all
  blocks will be archived. Generally, the stored blocks are long-lived as this
  type of persistence is used for long-term storage of blocks.
- **Scope**: The `Files Historic Persistence` will handle Persisted Notifications
  that are arriving via the node's messaging system. It will then proceed to
  determine if it can archive the next batch or not. If it can, it will
  proceed to archive the batch in a zip file, using the node's `Block Provision`
  facility to supply the blocks that need to be archived. Finally, it will
  publish the result of the archive operation to the node's messaging system
  as a Persisted Notification, only if the archive was successful.
- **Persistence Results**: `Files Historic Persistence` publishes persistence
  results to the node's messaging system when a successful archive happens.
  These results will be pushed to subscribed handlers.

## Test Scenarios

TBD

|                      Test Case ID | Test Name | Implemented (Y/N) |
|----------------------------------:|:----------|:-----------------:|
| [E2ETC_FHP_0001](#E2ETC_FHP_0001) | `TBD`     |         N         |
| [E2ETC_FHP_0002](#E2ETC_FHP_0002) | `TBD`     |         N         |

---

### E2ETC_FHP_0001

#### Test Name

TBD

#### Scenario Description

TBD

#### Requirements

TBD

#### Preconditions

TBD

#### Input

TBD

#### Output

TBD

#### Other

N/A

---

### E2ETC_FHP_0002

#### Test Name

TBD

#### Scenario Description

TBD

#### Requirements

TBD

#### Preconditions

TBD

#### Input

TBD

#### Output

TBD

#### Other

N/A

---
