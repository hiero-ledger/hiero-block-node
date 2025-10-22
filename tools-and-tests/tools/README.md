# BlockStreamTool

Command‑line toolkit for working with **Hedera block stream** files and related data pipelines.

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
