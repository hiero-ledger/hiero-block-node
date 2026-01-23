## Network Capacity Command

The `networkCapacity` command runs a client/server tool to measure streaming throughput and behavior over gRPC/HTTP2.

### Usage

```bash
networkCapacity -m <mode> -c <config.json> [options]
```

### Options

|         Option          |                    Description                    |
|-------------------------|---------------------------------------------------|
| `-m`, `--mode`          | Mode: `server` or `client` (required)             |
| `-c`, `--config`        | Path to JSON configuration file (required)        |
| `-p`, `--port`          | Port to listen on (server) or connect to (client) |
| `-f`, `--folder`        | Recording folder (required for client mode)       |
| `-s`, `--serverAddress` | Server address (required for client mode)         |

### Server Mode

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
