# Command Line Tools for Block Nodes & Streams

## Table of Contents

1. [Overview](#overview)
2. [Other Documentation](#other-documentation)
3. [Command Reference](#command-reference)
   1. [The `blocks` Subcommand](docs/blocks-commands.md)
   2. [The `records` Subcommand](docs/record-files-commands.md)
   3. [The `days` Subcommand](docs/days-commands.md)
   4. [The `mirror` Subcommand](docs/mirror-node-commands.md)
   5. [The `metadata` Subcommand](docs/metadata-commands.md)
   6. [The `networkCapacity` Subcommand](docs/network-capacity-commands.md)
   7. [The `states` Subcommand](docs/states-commands.md)
4. [Running from Command Line](#running-from-command-line)
5. [Help and Discovery](#help-and-discovery)
6. [Requirements](#requirements)
7. [Install & Build](#install--build)
8. [Docker / Compose](#docker--compose)
9. [Troubleshooting](#troubleshooting)
10. [Development](#development)

## Overview

This subproject provides command line tools for working with Block Stream files, Record Stream files and related
operations such as downloading Mirror Node data and working with compressed day archives. It uses [picocli](https://picocli.info)
to provide a command line interface which makes it easy to extend and add new subcommands or options.

The main entry point class is `org.hiero.block.tools.BlockStreamTool`. The following top-level commands are available:

|                       Command                        |                            Description                             |
|------------------------------------------------------|--------------------------------------------------------------------|
| [blocks](docs/blocks-commands.md)                    | Works with Block Stream files (.blk, .blk.gz, .blk.zstd)           |
| [records](docs/record-files-commands.md)             | Tools for working with raw Record Stream files (.rcd / .rcd.gz)    |
| [days](docs/days-commands.md)                        | Works with compressed daily record file archives (.tar.zstd)       |
| [mirror](docs/mirror-node-commands.md)               | Works with Mirror Nodes to fetch data and derive auxiliary files   |
| [metadata](docs/metadata-commands.md)                | Works with metadata files (block_times.bin, day_blocks.json, etc.) |
| [networkCapacity](docs/network-capacity-commands.md) | Network capacity testing tool (gRPC/HTTP2 throughput)              |
| [states](docs/states-commands.md)                    | Tools for working with saved state directories (SignedState.swh)   |

A lot of these tools have been built for the process of conversion from record files to wrapped record block files. See
[Record to Block Conversion Overview](docs/record-to-block-conversion.md) for more details on that process.

## Other Documentation

Additional documentation for specific technical topics:

- [Address Book Updating](docs/address-book-updating.md)
- [Address Book Analysis](docs/address-book-analysis.md)
- [Record Files Format Spec](docs/record-file-format.md)
- [Record to Block Conversion](docs/record-to-block-conversion.md)
  -

## Command Reference

### Quick Command Tree

```
subcommands
├── blocks                    # Works with Block Stream files
│   ├── json                  # Convert binary Block Stream to JSON
│   ├── ls                    # List/inspect block files
│   ├── validate              # Validate block hash chain and signatures
│   └── wrap                  # Convert record files to wrapped blocks
│
├── records                   # Tools for Record Stream files
│   └── ls                    # List record file info
│
├── days                      # Works with compressed daily archives
│   ├── ls                    # List record file sets in day archives
│   ├── validate              # Validate blockchain in day files
│   ├── validate-with-stats   # Validate with signature statistics
│   ├── validate-sig-counts   # Validate signature counts per node
│   ├── compress              # Compress day directories to .tar.zstd
│   ├── download-day          # Download record files for one day
│   ├── download-days         # Download record files for date range
│   ├── download-days-v2      # Download days (v2 implementation)
│   ├── download-days-v3      # Download days (v3 with Guava preload fix)
│   ├── download-live         # Live download following Mirror Node
│   ├── download-live2        # Live download with inline validation
│   ├── print-listing         # Print listing for one day
│   ├── ls-day-listing        # Print all files in day listing
│   ├── split-files-listing   # Split files.json into day listing files
│   ├── clean                 # Clean bad record sets from day files
│   ├── updateDayListings     # Update day listings from GCS bucket
│   ├── fix-missing-signatures # Fix missing signatures in downloaded days
│   └── fix-sig-names         # Fix signature file naming (.rcs.sig → .rcd.sig)
│
├── mirror                    # Works with Mirror Nodes
│   ├── blocktime             # Display block time for block numbers
│   ├── extractBlockTimes     # Extract block times from CSV
│   ├── validateBlockTimes    # Validate block times file
│   ├── addNewerBlockTimes    # Add newer block times from GCP
│   ├── fetchRecordsCsv       # Download Mirror Node CSV dump
│   ├── extractDayBlock       # Extract per-day block info to JSON
│   ├── update                # Update block data from Mirror Node
│   ├── generateAddressBook   # Generate address book from CSV
│   └── compareAddressBooks   # Compare two address book files
│
├── metadata                  # Works with metadata files
│   ├── ls                    # Display metadata file summary
│   └── update                # Update all metadata files
│
├── networkCapacity           # Network capacity testing (gRPC/HTTP2)
│
└── states                   # Tools for saved state directories
    ├── state-to-json        # Convert saved state to JSON Block Stream
    └── block-zero           # Load and validate Mainnet Block Zero state
```

## Running from Command Line

### Building the Tools JAR

```bash
# Build the shadow JAR (all-in-one executable)
./gradlew :tools:shadowJar
```

### Running Commands

Typical invocation after building:

```bash
java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar <command> <subcommand> [options]
```

For example:

```bash
# Convert block files to JSON
java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar blocks json path/to/blocks/

# List block file info
java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar blocks ls path/to/blocks/

# Validate day archives
java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar days validate /path/to/compressedDays
```

### Running via Gradle (Development)

```bash
./gradlew :tools:run --args="<command> <subcommand> [options]"

# Example
./gradlew :tools:run --args="blocks ls path/to/blocks/"
```

## Help and Discovery

For full, authoritative usage and all options for any command, run the tool with `--help`:

```bash
# Top-level help (lists all commands)
java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar --help

# Help for a specific command
java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar blocks --help

# Help for a nested subcommand
java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar days download-days-v2 --help
```

## Docker / Compose

Build and run the tools using Docker:

```bash
# Build Docker image
./gradlew :tools:createDockerImage
# Edit the docker compose example below to point to your config and recording paths
cd build/docker
# then run docker compose
docker-compose -f docker-compose.yml up
# or use the gradle tasks to start server and client containers provided in the example docker-compose.yml
./gradlew :tools:startDockerContainerNetworkCapacityServer
./gradlew :tools:startDockerContainerNetworkCapacityClient
```

### Example: Server via Compose

```yaml
services:
  # Example of the Container of block tool Blockstream for gRPC network capacity testing server mode
  block-tool-server:
    build: .
    image: block-tools:${VERSION}
    container_name: block-tools-server
    env_file:
      - .env
    ports:
      - "9090:9090"
    volumes:
      - ../resources/main/serverDefaultConfig.json:/app/conf/serverDefaultConfig.json:ro
    # this command starts the server mode of the networkCapacity tool
    command: ["networkCapacity",
              "-m", "server",
              "-c", "/app/conf/serverDefaultConfig.json",
              "-p", "9090"]
```

### Example: Client via Compose

```yaml
services:
  # Example of the Container of block tool Blockstream for gRPC network capacity testing client mode
  block-tool-client:
    build: .
    image: block-tools:${VERSION}
    container_name: block-tools-client
    env_file:
      - .env
    depends_on:
      - block-tool-server
    volumes:
      - ../resources/main/clientDefaultConfig.json:/app/conf/clientDefaultConfig.json:ro
      # make sure to have a Block Stream recording folder to use the client with
      - /path/to/your/recordings:/app/recording:ro
    # this command starts the client mode of the networkCapacity tool
    command: ["networkCapacity",
              "-m", "client",
              "-c", "/app/conf/clientDefaultConfig.json",
              "-s", "block-tool-server",
              "-p", "9090",
              "-f", "/app/recording"]
```

**Compose tips**

* When bind‑mounting files, use valid `SRC:DEST[:MODE]` strings; avoid empty segments.
* Paths are resolved relative to the Compose file. Use absolute paths for host directories outside the project folder.

## Requirements

* **Java JDK 25** or later
* For GCP-related commands (downloading from buckets):
  * Google Cloud SDK with authentication configured
  * A GCP project with billing enabled for requester-pays access

```bash
# Authenticate with GCP
gcloud auth application-default login
# or
gcloud auth login
```

## Install & Build

### From Source

```bash
# Clone and build
./gradlew clean build

# Build just the tools shadow JAR
./gradlew :tools:shadowJar
```

### Running Without Packaging (Development)

```bash
./gradlew :tools:run --args="<command> [options]"
```

## Troubleshooting

|              Problem              |                                     Solution                                      |
|-----------------------------------|-----------------------------------------------------------------------------------|
| "Missing required subcommand"     | You invoked the binary without a subcommand. Run with `--help` to see options.    |
| `NoSuchFileException` for configs | The path you passed does not exist. Check the working directory or Docker mounts. |
| "Recording folder does not exist" | Ensure `-f` points to a real directory that exists.                               |
| GCP authentication errors         | Run `gcloud auth application-default login` and ensure billing is enabled.        |
| "Requester pays" access denied    | Set your GCP project: `gcloud config set project YOUR_PROJECT_ID`                 |

## Development

* Code is organized under the `org.hiero.block.tools` package
* The top-level entrypoint is `BlockStreamTool` which registers all subcommands
* Each subcommand is a Picocli command class
* Recommended Java: 25+
