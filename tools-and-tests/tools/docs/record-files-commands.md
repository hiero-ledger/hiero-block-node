## Records Subcommands

The `records` command contains subcommands for working with raw record stream files (.rcd / .rcd.gz).

### Available Subcommands

| Command |                                     Description                                     |
|---------|-------------------------------------------------------------------------------------|
| `ls`    | List record file info contained in provided `.rcd` / `.rcd.gz` files or directories |

> **Note:** The legacy `record2block` command has been replaced by [`blocks wrap`](blocks-commands.md#the-wrap-subcommand), which converts record file blocks in day archives to wrapped block stream blocks.

---

### The `ls` Subcommand

Lists parsed metadata for each record file, including record format version, HAPI version, and short hashes.

#### Usage

```
records ls <files-or-dirs>...
```

#### Options

|        Option        |                                                   Description                                                   |
|----------------------|-----------------------------------------------------------------------------------------------------------------|
| `<files-or-dirs>...` | Files or directories to process. Directories are walked and files ending with `.rcd` or `.rcd.gz` are included. |

#### Output Columns

|  Column   |                  Description                  |
|-----------|-----------------------------------------------|
| File Name | Name of the record file                       |
| Ver       | Record format version (e.g., 5, 6)            |
| HAPI      | HAPI protocol version (e.g., 0.11.0)          |
| PrevHash  | First 8 hex characters of previous block hash |
| BlockHash | First 8 hex characters of this block's hash   |
| Size      | Human-readable file size                      |

#### Example Output

```
File Name                                                              Ver   HAPI      PrevHash   BlockHash  Size
------------------------------------------------------------------------------------------------------------------------
recordstreams_record0.0.20_2022-02-01T17_30_00.008743294Z.rcd          5     0.11.0    f032213b   b9279d9b   58.9 KB
recordstreams_record0.0.4_2022-02-01T17_30_02.050973000Z.rcd           5     0.11.0    b9279d9b   d4cd9a9b   56.6 KB
recordstreams_record0.0.4_2022-02-01T17_30_04.006373067Z.rcd           5     0.11.0    d4cd9a9b   9d7591cc   71.7 KB
recordstreams_record0.0.10_2022-02-01T17_30_06.019622000Z.rcd          5     0.11.0    9d7591cc   b1ce579a   55.1 KB
```

#### Example

```bash
# List info for record files in a directory
records ls /path/to/recordfiles/

# List info for specific files
records ls file1.rcd file2.rcd.gz
```

---

## Related Commands

For converting record files to block stream format, see:

- [`blocks wrap`](blocks-commands.md#the-wrap-subcommand) - Convert record file blocks in day archives to wrapped block stream blocks
- [`days`](days-commands.md) - Commands for working with compressed daily record file archives (.tar.zstd)
