# E2E Test Plan - Real Blocks Streaming

## Overview

The Real Blocks suite streams block sets captured from real Consensus Node
deployments into a Block Node with verification on. The blocks come from OCI
load archives that `fetchTestBlocks` downloads before the run.

The purpose is a load test with real blocks: real block sizes, transaction mix
and rate, instead of generated ones. The suite supports several sources of
real blocks. This E2E test plan lists the sources and what the Block Node must
do with the blocks of each: keep verifying, persisting and acknowledging them.

## Key Considerations

- **Sources**: each source is a CN deployment or test that produces the
  blocks: SDPT/SDLT, SDCT / longevity perf1 and BNCE 10K so far. Adding a
  source means publishing a new load archive (or a new set folder in one); the
  test needs no change.
- **Self-describing Set**: each set folder in a load archive holds the blocks
  (`.blk.gz`), a `manifest.json` naming the source, and the roster files the
  Block Node needs to verify them, if the blocks do not provide that data
  themselves: `tss-bootstrap-roster.json` for TSS sets,
  `rsa-bootstrap-roster.json` for WRB/RSA sets. First and last block come from
  the file names, and the test mounts whichever roster files the set has.
- **Shape Of A Set**: where a set starts (block 0 or mid-chain) and its proof
  type (TSS or WRB/RSA) depend on how the source was captured, not on the test.
  The test handles each case from the set's own files:
  - First block: the Block Node runs with `BLOCK_NODE_EARLIEST_MANAGED_BLOCK`
    and `VERIFICATION_FIRST_ORDERED_BLOCK` set to the first block of the set
    (the defaults for a set that starts at block 0). Chain hashes need no local
    history: the Block Node takes the previous block root hash and the
    all-blocks tree root from each block's footer.
  - TSS: a set that starts at block 0 carries the ledger ID in the
    `LedgerIdPublication` of block 0. A mid-chain set must ship
    `tss-bootstrap-roster.json`, otherwise every block fails with
    `MISSING_VERIFICATION_DATA`.
  - WRB/RSA: the set must ship `rsa-bootstrap-roster.json`.
- **One Test Per Set**: every set folder runs as its own test against its own
  fresh Block Node, named by the set's path relative to the archive root.
  `-PrealBlocks.set=<glob>` narrows the selection.
- **Roster Files Are Overwritten**: the Block Node writes its TSS data and
  address book history back to the configured paths, so the test mounts a
  writable copy of the roster files, never the originals.
- **Hash Algorithm**: the Block Node hashes with SHA-256 only. A SHA-384 set
  is refused with `UNSUPPORTED_STREAM_FORMAT` and cannot be re-hashed (the TSS
  signature is over the SHA-384 root hash). Published archives hold SHA-256
  captures only.
- **Crypto Version**: the TSS library version of the Block Node must match the
  Consensus Node that produced the blocks, otherwise every block fails with
  `BAD_BLOCK_PROOF`.
- **Large Blocks**: real blocks can be hundreds of MB. The Block Node runs with
  `SERVER_MAX_MESSAGE_SIZE_BYTES=300000000`, and the simulator's
  `blockStream.maxBlockItemsToStream` is raised so it does not stop mid-set.
- **Resend Loop**: a refused block is resent by the simulator forever. The
  test fails early when `verification_blocks_verified` stops advancing for a
  stall window, instead of waiting for the full timeout.
- **Pace**: the simulator sends one block every
  `blockStream.millisecondsPerBlock` ms, a parameter of the run. With 0 it
  sends as fast as the Block Node accepts.
- **Report**: each set writes `build/real-blocks/<set>.txt`, uploaded as a CI
  artifact so runs can be compared. It holds:
  - blocks/s: blocks in the set divided by the time from the first block sent
    to the last block acknowledged. With pace 0 this is the highest rate the
    Block Node sustains for that set; with a fixed pace it shows whether the
    Block Node keeps up.
  - the average verification time and receive latency per block:
    `verification_block_time` and `publisher_receive_latency_ns` divided by
    the block count. Both are running sums in nanoseconds, so averages are all
    they give, not percentiles.
- **Scope**: a load test with real blocks, not a functional test of Block Node
  behavior; failure paths are covered by the other suites. Each set has one
  expected outcome: every block verified, persisted and acknowledged.
  Throughput and latency are reported per set (see Report), with no
  thresholds until nightly runs give a baseline. The suite runs with
  `./gradlew :suites:runRealBlockSuites`, nightly and on demand, not on pull
  requests.

## Test Scenarios

|            Test Case ID | Test Name                                                     | Implemented ( :white_check_mark: / :x: ) |
|------------------------:|:--------------------------------------------------------------|:----------------------------------------:|
|                         | _**<br/>[Real Blocks Tests](#real-blocks-tests)<br/>&nbsp;**_ |                                          |
| [TC-RB-001](#tc-rb-001) | Verify SDPT/SDLT Blocks                                       |                   :x:                    |
| [TC-RB-002](#tc-rb-002) | Verify SDCT / Longevity perf1 Blocks                          |                   :x:                    |
| [TC-RB-003](#tc-rb-003) | Verify BNCE 10K Blocks                                        |                   :x:                    |

### Real Blocks Tests

#### TC-RB-001

##### Test Name

`Verify SDPT/SDLT Blocks`

##### Scenario Description

A SHA-256 block set captured from an SDPT/SDLT run is streamed by the
simulator into an empty Block Node, starting at the first block of the set.

##### Requirements

The Block Node MUST accept the first block of the set as its first block.<br/>
The Block Node MUST verify every block using only the data in the blocks and
the roster files of the set.<br/>
The Block Node MUST persist and acknowledge every block.

##### Expected Behavior

- Every block of the set is verified, persisted and acknowledged.
- No block fails verification.

##### Preconditions

- A load archive with an SDPT/SDLT set, fetched with
  `-PtestBlocks=<archive>`.
- A Block Node container configured to start at the first block of the set,
  with writable copies of the set's roster files mounted.

##### Input

- Blocks `<first>` to `<last>` of the set, streamed by the simulator in `DIR`
  mode.

##### Output

- `verification_blocks_verified` and `files_recent_blocks_written` equal the
  number of blocks in the set.
- `verification_blocks_failed` is 0.
- The simulator's last known publisher status acknowledges `<last>`.
- The report `build/real-blocks/<set>.txt` gives blocks/s and the average
  verification time and receive latency per block.

##### Other

N/A

---

#### TC-RB-002

##### Test Name

`Verify SDCT / Longevity perf1 Blocks`

##### Scenario Description

Same as [TC-RB-001](#tc-rb-001), with a set captured from an SDCT / longevity
perf1 run.

##### Requirements

Same as [TC-RB-001](#tc-rb-001).

##### Expected Behaviour

- Every block of the set is verified, persisted and acknowledged.

##### Preconditions

- A load archive with an SDCT / longevity perf1 set.

##### Input

- Blocks `<first>` to `<last>` of the set, streamed by the simulator.

##### Output

- Same as [TC-RB-001](#tc-rb-001).

##### Other

N/A

---

#### TC-RB-003

##### Test Name

`Verify BNCE 10K Blocks`

##### Scenario Description

Same as [TC-RB-001](#tc-rb-001), with a set captured from a BNCE run at 10K
TPS: 5K crypto transfer and 5K HCS.

##### Requirements

Same as [TC-RB-001](#tc-rb-001).

##### Expected Behaviour

- Every block of the set is verified, persisted and acknowledged.

##### Preconditions

- A load archive with a BNCE 10K set.

##### Input

- Blocks `<first>` to `<last>` of the set, streamed by the simulator.

##### Output

- Same as [TC-RB-001](#tc-rb-001).

##### Other

N/A

---
