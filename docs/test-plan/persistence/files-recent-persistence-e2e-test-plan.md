# E2E Test Plan - `Files Recent Persistence`

## Overview

Persistence is a key component of the Block-Node. Although not each and every
Block-Node will feature persistence, we need to ensure that we have good and
reliable persistence plugins that can be used by the Block-Node. We support
local file system persistence for recently received and verified blocks which
we call `Files Recent Persistence`. Also, complementary to the persistence
capability itself, we also have the ability to retrieve (read) blocks that have
been persisted. `Block Provision` is also a key component of the Block-Node.
The `Files Recent Persistence` is however an optional component, meaning that
the system is able to operate without it present and loaded.

This E2E test plan is designed highlighting the key scenarios that need to be
tested for end results in order to ensure the correctness and proper working of
the `Files Recent Persistence` as well as the complementary `Block Provision`
logic.

## Key Considerations for `Files Recent Persistence`

- **Path Resolution Logic**: `Files Recent Persistence` relies on path
  resolution logic which determines the location of a given block.
- **Trie Data Structure for Path Resolution**: `Files Recent Persistence`
  utilizes a trie data structure to resolve paths for blocks. This is done to
- **Files Recent Root Path**: This is the root path (configurable) where all
  blocks that have passed verification are stored. Generally, the stored blocks
  are relatively short-lived because this storage type is intended for recently
  received and verified blocks.
- **Scope**: The `Files Recent Persistence` will accept verified blocks through
  the node's messaging system and will store them under its root.
  The `Files Recent Persistence` will keep a limited number of blocks in its
  root path and will then proceed to delete the oldest blocks(rolling history).
  That is the full scope of the `Files Recent Persistence`. While the blocks are
  stored and available, they can be queried by other components of the system.
- **Persistence Results**: `Files Recent Persistence` publishes persistence
  results to the node's messaging system. These results will be pushed to
  subscribed handlers.

## Test Scenarios

> _**NOTE**: for the purpose of the below test definitions, we will have the
> following root definition (this could be configured to be different, but
> whatever is configured is of no relevance to the outcome of the tests, simply
> we need a reference point so an example could be shown):_
> - `Files Recent Root`: `/blocks`
>
> _**NOTE**: assume that before each test no block files are present,
> all distinct roots are empty unless specified otherwise._
>
> _**NOTE**: the trie structure for `Files Recent Root` is resolving
> all the digits of a `long` (max. 19 digits). We have three digits per node
> or directory up to a max. depth of 16, and then we have the full
> blockNumber (`long`) as the file name of any persisted block with all leading
> zeros. So for example, if the block number is 1234567890123456789 and the root
> is `/blocks` then the path will be
> `/blocks/123/456/789/012/345/6/1234567890123456789.blk.zstd`._

|   Test Case ID | Test Name | Scenario Description | Requirement | Input | Output | Implemented (Y/N) |
|---------------:|:----------|:---------------------|:------------|:------|:-------|:-----------------:|
| E2ETC_FRP_0001 | `TBD`     | TBD                  | TBD         | TBD   | TBD    |         N         |
