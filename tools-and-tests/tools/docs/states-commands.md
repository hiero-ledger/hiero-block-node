# States Subcommands

The `states` command contains utilities for working with saved state directories (`SignedState.swh` and related
files). These tools load historical Hedera consensus node state snapshots and can convert them to JSON block stream
representations or validate block zero state.

### Available Subcommands

|    Command    |                                     Description                                      |
|---------------|--------------------------------------------------------------------------------------|
| `state-to-json` | Convert saved state directories to JSON block stream representation               |
| `block-zero`    | Load and validate the state at the start of Hedera Mainnet Block Zero             |

---

### The `state-to-json` Subcommand

Reads one or more saved state directories and prints a JSON representation of the state as a block stream `Block`
containing state change `BlockItem` entries. The output is written to stdout.

#### Usage

```
states state-to-json [<savedStateDirectories>...]
```

#### Options

|         Option          |                          Description                           |
|-------------------------|----------------------------------------------------------------|
| `<savedStateDirectories>...` | One or more saved state directories to process. Each directory should contain `SignedState.swh` and related files (CSV/gzipped CSV). |

#### Example

```bash
# Convert a saved state directory to JSON
states state-to-json /path/to/saved-state-33485415

# Convert multiple saved state directories
states state-to-json /path/to/state1 /path/to/state2
```

#### Notes

- The command loads each saved state using `SavedStateConverter`, which reads `SignedState.swh`, account maps,
  storage maps, and binary object CSV files (supports both plain and `.gz` compressed files).
- The output is a JSON-serialized `Block` protobuf containing `BlockItem` entries for each state change (accounts,
  files, contract bytecodes, contract storage).
- Output is printed to stdout, so redirect to a file if needed:
  ```bash
  states state-to-json /path/to/state > state.json
  ```

---

### The `block-zero` Subcommand

Constructs and validates the state at the beginning of Hedera Mainnet Block Zero (stream start on
13th September 2019). This command uses the closest available saved state snapshot (round 33485415, taken at the end
of block zero) and reverses the single transaction that occurred during block zero to reconstruct the initial state.

#### Usage

```
states block-zero
```

#### What It Does

1. **Loads the saved state** at round 33485415 (end of block zero)
2. **Reverses transaction 1** (a crypto transfer by account 0.0.11337) to compute the state at the *start* of
   block zero
3. **Prints a summary** of the start-of-block-zero state (accounts, balances, files, contracts, storage)
4. **Re-applies transaction 1** and compares balances with the original saved state to verify correctness
5. **Applies transactions 2 and 3** to compute the state at the end of block two
6. **Compares with CSV balances** from 2019-09-13T22:00:00.000081Z to validate the computed end-of-block-two state

#### Output

The command prints:
- State summaries with account counts, total balances, file counts, contract counts, and storage entries
- A validation report for the loaded saved state (hash verification, signature checks)
- Account comparison reports showing matching accounts, differences, and accounts only in one set

#### Notes

- This command requires no arguments; all data is bundled as resources within the tools JAR.
- The bundled resources include the saved state at round 33485415 and the CSV balances file from
  2019-09-13T22:00:00.000081Z.
- The command validates that reversing and re-applying transactions produces consistent results, serving as a
  correctness check for the state loading and conversion logic.

#### Background

Block zero is the first block of the Hedera Mainnet blockchain. At stream start, the network was not empty — accounts,
files, and smart contract KV pairs already existed. The saved state at round 33485415 is the closest historical snapshot
to block zero. By reversing the known transaction that occurred during block zero, the initial state can be
reconstructed.

```
Event                              Consensus Timestamp      Consensus Time UTC
════════════════════════════════════════════════════════════════════════════════
Block 0                            1568411631.396440000     2019-09-13T21:53:51.396440Z
└─ Txn 0.0.11337                   1568411631.396440000     (crypto transfer +83,417 tinybar)
Saved State 33485415               1568411631.916679000     2019-09-13T21:53:51.916679Z

Block 1                            1568411670.872035001     2019-09-13T21:54:30.872035Z
└─ Txn 0.0.11337                   1568411670.872035001     (crypto transfer +83,417 tinybar)

Block 2                            1568411762.486929000     2019-09-13T21:56:02.486929Z
└─ Txn 0.0.11337                   1568411762.486929000

2019-09-13T22:00:00 Balances.csv   1568412000.81000         2019-09-13T22:00:00.000081Z
```
