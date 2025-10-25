# Command Line Tools for Block Nodes & Streams

## Table of Contents

1. [Overview](#overview)
    1. [The `blocks` Subcommand](docs/blocks-commands.md)
    2. [The `records` Subcommand](docs/record-files-commands.md)
    3. [The `days` Subcommand](docs/days-commands.md)
    4. [The `mirror` Subcommand](docs/mirror-node-commands.md)
2. [Running from command line](#running-from-command-line)
3. [Help and discovery](#help-and-discovery)
4. [Other Documentation](#other-documentation)

## Overview

This subproject provides command line tools for working with Block Stream files, Record Stream files and related
operations such as downloading Mirror Node data and working with compressed day archives. It uses [picocli](https://picocli.info)
to provide a command line interface which makes it easy to extend and add new subcommands or options.

The main entry point class is `org.hiero.block.tools.BlockStreamTool`. The following top level commands are available:
- [blocks](docs/blocks-commands.md) - Works with block stream files
- [records](docs/record-files-commands.md) - Tools for working with raw record stream files (.rcd / .rcd.gz)
- [days](docs/days-commands.md) - Works with compressed daily record file archives (.tar.zstd)
- [mirror](docs/mirror-node-commands.md) - Works with mirror nodes to fetch data and derive auxiliary files (CSV, block times, etc.)

A lot of these tools have been built for the process of conversion from record files to wrapped record block files. See
[Record to Block Conversion Overview](docs/record-to-block-conversion.md) for more details on that process.

## Running from command line

Refer to the [Quickstart](../../docs/tools/quickstart.md) for a quick guide on how to build and run the tools CLI.

Typical invocation (after building the `tools` shadow jar):

```bash
java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar <top-level-command> <subcommand> [options]
```

For example:

```bash
java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar blocks json -t path/to/dir
```

## Help and discovery

For full, authoritative usage and all options for any command run the tool with `--help`. Example:

```bash
java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar record2block --help
```

Or for a nested subcommand:

```bash
java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar days download-days-v2 --help
```

If you want the README to include example invocations for any specific workflow (e.g. full record→block conversion, or downloading a year's worth of days), tell me which workflow and I'll add a short step-by-step example.


## Other Documentation
Additional documentation for specific techical topics can be found in the `docs/` directory:
- [Address Book Updating](docs/address-book-updating.md)
- [Record Files Format Spec](docs/record-file-format.md)
