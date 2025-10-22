# Command Line Tools for Block Nodes & Streams

## Table of Contents

1. [Overview](#overview)
2. [Running from command line](#running-from-command-line)
3. [Subcommands](#subcommands)
   1. [The `json` Subcommand](#the-json-subcommand)
   2. [The `info` Subcommand](#the-info-subcommand)

## Overview

This subproject provides command line tools for working with Block Stream files, Record Stream files and maybe other
things in the future. It uses [picocli](https://picocli.info) to provide a command line interface which makes it easy to extend and add
new subcommands or options.

## Running from command line

Refer to the [Quickstart](../../docs/tools/quickstart.md) for a quick guide on how to run the tools CLI.

## Subcommands

The following top level subcommands are available:
- `blocks` - Tools for working with block stream files
- `records` - Tools for working with raw record stream files
- `days` - Tools for working with days of record files compressed as tar.zstd files
- `mirror` - Tools for working with mirror nodes to fetch data

## Blocks Subcommands
The following subcommands are available under the `blocks` top level command:
- `json` - Converts a binary Block Stream to JSON
- `info` - Prints info for block files

## Records Subcommands
The following subcommands are available under the `blocks` top level command:
- `ls` - Lists the contents of record files
- `record2block` - Converts a historical Record Stream files into blocks
- `fetchRecordsCsv` - Download Mirror Node record table CSV dump from GCP bucket

## Days Subcommands
The following subcommands are available under the `blocks` top level command:
- `ls` - Lists the contents of record files
- `record2block` - Converts a historical Record Stream files into blocks

## Mirror Node Subcommands
The following subcommands are available under the `blocks` top level command:
- `extractBlockTimes` - Extract block times from Mirror Node records csv file
- `validateBlockTimes` - Validates a block times file as produced by `extractBlockTimes`
- `addNewerBlockTimes` - Extends the block times file with newer block times
- `fetchRecordsCsv` - Download Mirror Node record table CSV dump from GCP bucket

---

### The `json` Subcommand

Converts a binary Block Stream to JSON

`Usage: json [-t] [-ms=<minSizeMb>] [<files>...]`

**Options:**

- `-ms <minSizeMb>` or `--min-size=<minSizeMb>`
  - Filter to only files bigger than this minimum file size in megabytes
- `-t` or `--transactions`
  - expand transactions, this is no longer pure json conversion but is very useful making the
    transactions human-readable.
- `<files>...`
  - The block files or directories of block files to convert to JSON
### 3) Convert mirror **record** streams → **block** streams

### The `info` Subcommand
```bash
./gradlew :tools:run --args="record2block -i /data/mirror-records -o /data/blocks"
```

### 4) Mirror‑node CSV → block times
Prints info for block files

```bash
# Download CSV dumps from GCP first (example flags; use -h for exact ones)
./gradlew :tools:run --args="fetchRecordsCsv --bucket my-bucket --prefix mirror/records -o /tmp/records-csv"
`Usage: info [-c] [-ms=<minSizeMb>] [-o=<outputFile>] [<files>...]`

**Options:**

- `-c` or `--csv`
  - Enable CSV output mode (default: false)
- `-ms <minSizeMb>` or `--min-size=<minSizeMb>`
  - Filter to only files bigger than this minimum file size in megabytes
- `-o <outputFile>` or `--output-file=<outputFile>`
  - Output to file rather than stdout
- `<files>...`
  - The block files or directories of block files to print info for

### The `record2block` Subcommand

Converts a historical Record Stream files into blocks. This depends on the `block_times.bin` file being present. It can
be created by running the other commands `fetchRecordsCsv`, `extractBlockTimes` and `addNewerBlockTimes` in that order.
It can also be validated by running the `validateBlockTimes` command.

This command depends on reading data from public requester pays Google Cloud buckets. To do that it needs you to be
authenticated with the Google Cloud SDK. You can authenticate with `gcloud auth application-default login` or
`gcloud auth login` see [Google Documentation](https://cloud.google.com/storage/docs/reference/libraries#authentication)
for more info.

`Usage: record2block [-s 0] [-e 100] [-j] [-c] [--min-node-account-id=3] [--max-node-account-id=34] [-d <dataDir>] [--block-times=<blockTimesFile>]`

**Options:**

- `-s <blockNumber>` or `--start-block=<blockNumber>`
  - The first block number to process
  - Default: 0
- `-e <blockNumber>` or `--end-block=<blockNumber>`
  - The last block number to process
  - Default: 3001
- `-j` or `--json`
  - also output blocks as json, useful for debugging and testing
  - Default: false
- `-c` or `--cache-enabled`
  - Use local cache for downloaded content, saves cloud costs and bandwidth when testing
  - Default: false
- `--min-node-account-id=<minNodeAccountId>`
  - the account id of the first node in the network
  - Default: 3
- `--max-node-account-id=<maxNodeAccountId>`
  - the account id of the last node in the network
  - Default: 34
- `--data-dir=<dataDir>`
  - the data directory for output and temporary files
  - Default: "data"
- `--block-times=<blockTimesFile>`
  - Path to the block times ".bin" file.
  - Default: "data/block_times.bin"

### The `fetchRecordsCsv` Subcommand

Download Mirror Node record table CSV dump from GCP bucket. The records table on Mirror Node has a row for every block
Mirror Node knows about. The CSV file is huge 11GB+ in November 2024. This data is important for records to blocks
conversion as we have to make sure the block number assigned for a record file matches what Mirror Node says as the
source of truth.

This command depends on reading data from public requester pays Google Cloud buckets. To do that it needs you to be
authenticated with the Google Cloud SDK. You can authenticate with `gcloud auth application-default login` or
`gcloud auth login` see [Google Documentation](https://cloud.google.com/storage/docs/reference/libraries#authentication)
for more info.

`Usage: fetchRecordsCsv [--record-csv=<recordFilePath>]`

**Options:**

- `--record-csv=<recordFilePath>`
  - Path to the record CSV file.
  - Default: "data/record.csv"

### The `extractBlockTimes` Subcommand

Extract block times from Mirror Node records csv file. Reads `recordFilePath` and produces `blockTimesFile`. We need to
convert the Mirror Node records CSV because it is huge 11GB+ compressed and too large to fit into RAM, and we can not
random access easily. The only part of the data needed for the records to blocks conversion is the block times. The
block time being the record file time for a given block. The record file consensus time is used as the file name of the
record file in the bucket.

The block times file is a binary file of longs, each long is the number of nanoseconds for that block after first block
time. So first block = 0, second about 5 seconds later etc. The index is the block number, so block 0 is first long,
block 1 is second block and so on. This file can then be memory mapped and used as fast lookup for block
number(array offset) into block time, i.e. record file name.

`Usage: extractBlockTimes [--record-csv=<recordFilePath>] [--block-times=<blockTimesFile>]`

**Options:**

- `--record-csv=<recordFilePath>`
  - Path to the record CSV file.
  - Default: "data/record.csv"
- `--block-times=<blockTimesFile>`
  - Path to the block times ".bin" file.
  - Default: "data/block_times.bin"

### The `addNewerBlockTimes` Subcommand

Extends the block times file with newer block times. This is done by listing the record files in the bucket and
counting them for block numbers. It processes day by day, listing one day then appending block times to the block times
file. Then at the end of each day it checks the block number it has computed still matches Mirror Node by using the
Mirror Node REST API. This whole process can take a long time if the Mirror Node CSV dump is old.

This command depends on reading data from public requester pays Google Cloud buckets. To do that it needs you to be
authenticated with the Google Cloud SDK. You can authenticate with `gcloud auth application-default login` or
`gcloud auth login` see [Google Documentation](https://cloud.google.com/storage/docs/reference/libraries#authentication)
for more info.

`Usage: addNewerBlockTimes [-c]  [--min-node-account-id=3] [--max-node-account-id=34] [-d <dataDir>] [--block-times=<blockTimesFile>]`

**Options:**

- `-c` or `--cache-enabled`
  - Use local cache for downloaded content, saves cloud costs and bandwidth when testing
  - Default: true
- `--min-node-account-id=<minNodeAccountId>`
  - the account id of the first node in the network
  - Default: 3
- `--max-node-account-id=<maxNodeAccountId>`
  - the account id of the last node in the network
  - Default: 34
- `--data-dir=<dataDir>`
  - the data directory for output and temporary files
  - Default: "data"
- `--block-times=<blockTimesFile>`
  - Path to the block times ".bin" file.
  - Default: "data/block_times.bin"



---

## Docker / Compose

Run the servers using Docker or Docker Compose. Build the image first:

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

### Example: server via Compose

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

### Example: client via Compose

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
      # make sure to have a blockstream recording folder to use the client with
      - /path/to/your/recordings:/app/recording:ro
    # this command starts the client mode of the networkCapacity tool
    command: ["networkCapacity",
              "-m", "client",
              "-c", "/app/conf/clientDefaultConfig.json",
              "-s", "block-tool-server",
              "-p", "9090",
              "-f", "/app/recording"]
```

---

## Contents

* [Overview](#overview)
* [Requirements](#requirements)
* [Install & Build](#install--build)
* [How to Run](#how-to-run)
    * [Global help & version](#global-help--version)
    * [Subcommands](#subcommands)
* [Common Workflows & Examples](#common-workflows--examples)
    * [1) Convert blocks → JSON](#1-convert-blocks--json)
    * [2) Inspect block files](#2-inspect-block-files)
    * [3) Convert mirror **record** streams → **block** streams](#3-convert-mirror-record-streams--block-streams)
    * [4) Mirror‑node CSV → block times](#4-mirror-node-csv--block-times)
    * [5) Validate block times](#5-validate-block-times)
    * [6) Add newer block times from GCP](#6-add-newer-block-times-from-gcp)
    * [7) Network capacity tests (gRPC, HTTP/2)](#7-network-capacity-tests-grpc-http2)
* [Docker / Compose](#docker--compose)
* [Troubleshooting](#troubleshooting)
* [Development](#development)
* [License](#license)

---

## Overview

`BlockStreamTool` is a Picocli‑based CLI that bundles several utilities under a single binary. The entrypoint command exposes multiple subcommands for transforming, inspecting, and validating Hedera block streams, as well as a network capacity tester.

**Available subcommands** (as printed by `--help`):

* `json` – Convert a binary block stream to JSON.
* `info` – Print metadata/info for block files.
* `record2block` – Convert mirror **record** streams into **block** streams.
* `fetchRecordsCsv` – Download mirror‑node **record** table CSV dump from a GCP bucket.
* `extractBlockTimes` – Extract block times from mirror‑node CSV.
* `validateBlockTimes` – Validate an existing block‑times file.
* `addNewerBlockTimes` – Append/augment block times with newer data (from GCP).
* `networkCapacity` – Run a client/server tool to measure streaming throughput and behavior over gRPC/HTTP2.

> Tip: Each subcommand has its own `-h/--help` with all supported flags and options.

---

## Requirements

* Java JDK 21
* For some subcommands you may need access to
    * local block/record files,
    * a Google Cloud bucket (for `fetchRecordsCsv`/`addNewerBlockTimes`),
    * or config JSON files (for `networkCapacity`).

---

## Install & Build

### From source

```bash
# Clone and build
./gradlew clean build
```

### Run without packaging (dev)

Use Gradle’s `run` and pass args to the tool:

```bash
./gradlew :tools:run --args="<subcommand> [options]"
```

### From JAR

If your build produces a runnable JAR:

```bash
java -jar block-tools.jar <subcommand> [options]
```

---

## How to Run

### Global help & version

```bash
# Show top-level help (lists subcommands)
subcommands -h

# Show version
subcommands -V

# Help for a specific subcommand\ nsubcommands <subcommand> -h
```

### Subcommands

Below are quick synopses. Use `-h` on each for authoritative flags.

* **json** – Convert binary block stream → JSON
    * *Synopsis:* `json -i <input> -o <output>`
* **info** – Print block file info/metadata
    * *Synopsis:* `info -i <block-file-or-dir>`
* **record2block** – Convert mirror record streams → block streams
    * *Synopsis:* `record2block -i <records-dir> -o <blocks-dir> [options]`
* **fetchRecordsCsv** – Download mirror‑node records CSV from GCP
    * *Synopsis:* `fetchRecordsCsv --bucket <name> --prefix <path> -o <csv-dir> [options]`
* **extractBlockTimes** – Extract block times from mirror CSV
    * *Synopsis:* `extractBlockTimes -i <csv-file-or-dir> -o <block-times.csv>`
* **validateBlockTimes** – Validate a block‑times file
    * *Synopsis:* `validateBlockTimes -i <block-times.csv>`
* **addNewerBlockTimes** – Augment block‑times with newer data (GCP)
    * *Synopsis:* `addNewerBlockTimes --bucket <name> --prefix <path> -i <existing.csv> -o <updated.csv>`
* **networkCapacity** – Throughput testing (server/client)
    * *Synopsis:* `networkCapacity -m <server|client> -c <config.json> [other options]`

---

## Common Workflows & Examples

### 1) Convert blocks → JSON

```bash
# from source (Gradle)
./gradlew :tools:run --args="json -i blocks/00000001.blk -o blocks/00000001.json"

# from jar
java -jar block-tools.jar json -i blocks/ -o blocks-json/
```

### 2) Inspect block files

```bash
./gradlew :tools:run --args="info -i blocks/"
```

### 3) Convert mirror **record** streams → **block** streams

```bash
./gradlew :tools:run --args="record2block -i /data/mirror-records -o /data/blocks"
```

### 4) Mirror‑node CSV → block times

```bash
# Download CSV dumps from GCP first (example flags; use -h for exact ones)
./gradlew :tools:run --args="fetchRecordsCsv --bucket my-bucket --prefix mirror/records -o /tmp/records-csv"

# Extract block times
./gradlew :tools:run --args="extractBlockTimes -i /tmp/records-csv -o /tmp/block-times.csv"
```

### 5) Validate block times

```bash
./gradlew :tools:run --args="validateBlockTimes -i /tmp/block-times.csv"
```

### 6) Add newer block times from GCP

```bash
./gradlew :tools:run --args="addNewerBlockTimes --bucket my-bucket --prefix mirror/records -i /tmp/block-times.csv -o /tmp/block-times.updated.csv"
```

### 7) Network capacity tests (gRPC, HTTP/2)

The tool can run as a **server** (sink) or **client** (source). Both typically take a JSON config and optional port/host flags.

#### Server mode

```bash
./gradlew :tools:run --args="networkCapacity -m server -c conf/serverDefaultConfig.json -p 8090"
```

#### Client mode (replay a local recording folder)

```bash
./gradlew :tools:run --args="networkCapacity -m client -c conf/clientDefaultConfig.json -s 127.0.0.1 -p 8090 -f /path/to/RecordingBlockStream10"
```

**Notes**

* Ensure the recording folder exists and is a directory (`-f`).
* Use `-h` for all available tuning flags (HTTP/2 frame sizes, flow control, message size limits, etc.).

---

## Docker / Compose

Run the servers using Docker or Docker Compose. Build the image first:

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

### Example: server via Compose

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

### Example: client via Compose

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
      # make sure to have a blockstream recording folder to use the client with
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

---

## Troubleshooting

* **“Missing required subcommand”**
    * You invoked the binary without a subcommand. Run `subcommands -h` to see options, or include one (e.g., `json`, `info`, `networkCapacity`).
* **`NoSuchFileException` for JSON configs or inputs**
    * The path you passed to `-c`/`-i` does not exist inside the **process/container**. Double‑check the working directory or your Docker volume mounts.
* **Recording folder does not exist**
    * Ensure `-f` points to a real directory, and the mount path inside the container matches your host path.
* **Compose volume error: “empty section between colons”**
    * The `SRC:DEST[:MODE]` string has an empty part (e.g., `a::b`). Fix the mapping.
* **Passing args via Gradle**
    * Use `--args="..."` and quote the entire argument string.
* **Flow control / HTTP2 resets** (for `networkCapacity`)
    * Start with sane defaults, then adjust max message sizes, window sizes, and timeouts using the tool’s flags or config file. Use `-h` to discover the exact options.

---

## Development

* Code is organized under the `org.hiero.block.tools` package. The top‑level entrypoint is `BlockStreamTool` which registers all subcommands.
* Each subcommand is a Picocli command class; run `subcommands <cmd> -h` to discover its flags.
* Recommended Java: 21.
