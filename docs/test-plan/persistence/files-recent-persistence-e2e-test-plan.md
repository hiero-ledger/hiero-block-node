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
logic. Essentially, this test plan describes the intended behavior of the
`Files Recent Persistence` and the expected results it produces. The tests
implemented following this plan must assert all these assumptions.

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
> _**NOTE**: the trie structure for `Files Recent Root` is resolving
> all the digits of a `long` (max. 19 digits). We have three digits per node
> or directory up to a max. depth of 16, and then we have the full
> blockNumber (`long`) as the file name of any persisted block with all leading
> zeros. So for example, if the blockNumber is 1234567890123456789 and the root
> is `/blocks` then the path will be
> `/blocks/123/456/789/012/345/6/1234567890123456789.blk.zstd`._
>
> _**NOTE**: assume that before each test the Block-Node under test is expecting
> the next block to be received to be with number 0000000000001234567 which
> resolves to `/blocks/000/000/000/000/123/4/0000000000001234567.blk.zstd`. This
> is enough to illustrate the path resolution logic as well. Also, assume that
> the block is valid, has content and will successfully pass verification.
> Finally, assume that the above-mentioned block is NOT persisted before the
> start of each test._
>
> _**NOTE**: assume that we have a working verification because the
> `Files Recent Persistence` relies on blocks to be verified in order to execute
> any logic._
>
> _**NOTE**: assume that the Block-Node under test is configured to have the
> compression algorithm set to `ZStandard` so the file extension for a persisted
> block will be `.blk.zstd`_

|   Test Case ID | Test Name                                                               | Scenario Description                                                                                                                                                                                                                                                                             | Requirement                                                                                                                                                                                                                           | Input                                                                                                                                                               | Output                                                                                                                                                                                                                                                                        | Implemented (Y/N) |
|---------------:|:------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------:|
| E2ETC_FRP_0001 | `Verify Acknowledgement Returned After Successful Persistence`          | `Files Recent Persistence` will persist a block after it has been received and verified. It will then publish a persistence notification to the internal messaging system. It is expected that the Block-Node will respond with an acknowledgement to the publisher's request in this situation. | A publisher that is able to stream to the Block-Node under test.                                                                                                                                                                      | Valid block `0000000000001234567` is streamed as items.                                                                                                             | An acknowledgement is returned to the publisher.                                                                                                                                                                                                                              |         N         |
| E2ETC_FRP_0002 | `Verify Persisted Block File Location`                                  | `Files Recent Persistence` will persist a block after it has been received and verified. It is expected that a regular file is written at the properly resolved location.                                                                                                                        | A publisher that is able to stream to the Block-Node under test.                                                                                                                                                                      | Valid block `0000000000001234567` is streamed as items.                                                                                                             | Regular Readable File: `/blocks/000/000/000/000/123/4/0000000000001234567.blk.zstd` exists.                                                                                                                                                                                   |         N         |
| E2ETC_FRP_0003 | `Verify Persisted Block File Content`                                   | `Files Recent Persistence` will persist a block after it has been received and verified. It is expected that a regular file is written at the properly resolved location and the content of the file is the same as the original block streamed.                                                 | A publisher that is able to stream to the Block-Node under test.                                                                                                                                                                      | Valid block `0000000000001234567` is streamed as items.                                                                                                             | Regular Readable File: `/blocks/000/000/000/000/123/4/0000000000001234567.blk.zstd` has the same binary content as the binary data received as block items for the specified block.                                                                                           |         N         |
| E2ETC_FRP_0004 | `Verify EndOfStream returned on IO Failure`                             | `Files Recent Persistence` will persist a block after it has been received and verified. It is expected that the Block-Node will return and EndOfStream with PERSISTENCE_FAILED if an IO failure occurs during write.                                                                            | A publisher that is able to stream to the Block-Node under test. A way to simulate an IO issue for the resolved block.                                                                                                                | Valid block `0000000000001234567` is streamed as items.                                                                                                             | An EndOfStream with PERSISTENCE_FAILED is returned to the publisher.                                                                                                                                                                                                          |         N         |
| E2ETC_FRP_0005 | `Verify Persisted Block File Cleanup on IO Failure`                     | `Files Recent Persistence` will persist a block after it has been received and verified. It is expected that all data potentially written to the filesystem is cleaned if an IO failure occurs during write. No files or data related to the current block must be present after such failure.   | A publisher that is able to stream to the Block-Node under test. A way to simulate an IO issue for the resolved block.                                                                                                                | Valid block `0000000000001234567` is streamed as items.                                                                                                             | Regular Readable File: `/blocks/000/000/000/000/123/4/0000000000001234567.blk.zstd` does not exist. No data of the received block is present on the filesystem.                                                                                                               |         N         |
| E2ETC_FRP_0006 | `Verify Persisted Block File Discoverable After Successful Persistence` | `Files Recent Persistence` will persist a block after it has been received and verified. It is expected that after the persistence of the given block is successful, the block will be accessible via the node's public API.                                                                     | A publisher that is able to stream to the Block-Node under test. A client that can call the public APIs to read the block (i.e. `getBlock`). Client receives not found if initially attempts to read the block under test (expected). | Valid block `0000000000001234567` is streamed as items.                                                                                                             | Client is able to use the node's public API to read the persisted block (i.e. `getBlock`).                                                                                                                                                                                    |         N         |
| E2ETC_FRP_0007 | `Verify Persisted Block File can be Overwritten`                        | `Files Recent Persistence` will persist a block after it has been received and verified. It is expected that a regular file is written at the properly resolved location. It is expected that this file can be overwritten.                                                                      | A publisher that is able to stream to the Block-Node under test.                                                                                                                                                                      | Valid block `0000000000001234567` is streamed as items, waiting for Acknowledgement (ensure it is persisted). Then it is streamed again with different binary data. | Regular Readable File: `/blocks/000/000/000/000/123/4/0000000000001234567.blk.zstd` exists and it's binary content is the same as the second time it was streamed.                                                                                                            |         N         |
| E2ETC_FRP_0008 | `Verify Persisted Block in Rapid Succession`                            | `Files Recent Persistence` will persist a block after it has been received and verified. It is expected that a regular file is written at the properly resolved location. It is expected that an Acknowledgement is returned to the publisher.                                                   | A publisher that is able to stream to the Block-Node under test.                                                                                                                                                                      | Valid blocks `0000000000001234000` to `0000000000001235000` are rapidly streamed as items.                                                                          | Regular Readable Files: `/blocks/000/000/000/000/123/4/0000000000001234000.blk.zstd` to `/blocks/000/000/000/000/123/5/0000000000001235000.blk.zstd` exist, have the same binary content as the original sent blocks, are discoverable and readable through public API calls. |         N         |
