# Backfill Plugin

The Backfill Plugin detects gaps in historical blocks and fetches missing blocks from configured block nodes using gRPC.

## Configuration

### Basic Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `backfill.startBlock` | 0 | The first block that this BN deploy wants to have available |
| `backfill.endBlock` | -1 | Maximum number of blocks (-1 means no limit) |
| `backfill.blockNodeSourcesPath` | "" | File path for a JSON configuration of the BN sources |
| `backfill.scanInterval` | 60000 | Interval in milliseconds to scan for missing gaps |
| `backfill.maxRetries` | 3 | Maximum number of retries to fetch a missing block |
| `backfill.initialRetryDelay` | 5000 | Initial cooldown time between retries in milliseconds |
| `backfill.fetchBatchSize` | 10 | Number of blocks to fetch in a single gRPC call |
| `backfill.delayBetweenBatches` | 1000 | Cool downtime in milliseconds between batches |
| `backfill.initialDelay` | 15000 | Initial delay before starting the backfill process |
| `backfill.perBlockProcessingTimeout` | 1000 | Timeout in milliseconds for processing each block |
| `backfill.enableTLS` | false | Enable TLS for secure connections |

### gRPC Client Configuration Overrides

The backfill plugin supports granular configuration of gRPC client parameters. This allows operators to optimize backfill gRPC requests without needing a new software release.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `backfill.grpcOverallTimeout` | 60000 | Default timeout in milliseconds for all gRPC operations |
| `backfill.grpcConnectTimeout` | -1 | Connection timeout in milliseconds. If -1, uses `grpcOverallTimeout` |
| `backfill.grpcReadTimeout` | -1 | Read timeout in milliseconds. If -1, uses `grpcOverallTimeout` |
| `backfill.grpcPollWaitTime` | -1 | Poll wait time in milliseconds. If -1, uses `grpcOverallTimeout` |

#### Configuration Examples

**Using default overall timeout:**
```properties
backfill.grpcOverallTimeout=60000
```

**Overriding specific timeouts:**
```properties
backfill.grpcOverallTimeout=60000
backfill.grpcConnectTimeout=30000
backfill.grpcReadTimeout=90000
backfill.grpcPollWaitTime=45000
```

**Partial overrides (others fall back to overall timeout):**
```properties
backfill.grpcOverallTimeout=60000
backfill.grpcConnectTimeout=20000
# grpcReadTimeout and grpcPollWaitTime will use grpcOverallTimeout (60000ms)
```

**YAML format (for Kubernetes/Helm):**
```yaml
blockNode:
  config:
    BACKFILL_GRPC_OVERALL_TIMEOUT: "60000"
    BACKFILL_GRPC_CONNECT_TIMEOUT: "30000"
    BACKFILL_GRPC_READ_TIMEOUT: "90000"
    BACKFILL_GRPC_POLL_WAIT_TIME: "45000"
```

### Block Node Sources Configuration

The `blockNodeSourcesPath` should point to a JSON file with the following structure:

```json
{
  "nodes": [
    {
      "address": "localhost",
      "port": 40800,
      "priority": 1
    },
    {
      "address": "node2.example.com",
      "port": 40902,
      "priority": 2
    }
  ]
}
```

## Usage

### Autonomous Backfill

The plugin automatically detects gaps in the block range and fetches missing blocks from configured sources at regular intervals.

### On-Demand Backfill

The plugin can also be triggered on-demand when the latest block known to the network is received via `NewestBlockKnownToNetwork` notification.

## Metrics

The plugin exposes the following metrics:

- `backfill_gaps_detected` - Number of gaps detected during the backfill process
- `backfill_blocks_fetched` - Number of blocks fetched during the backfill process
- `backfill_blocks_backfilled` - Number of blocks successfully backfilled
- `backfill_fetch_errors` - Number of errors encountered during the backfill process
- `backfill_retries` - Number of retries during the backfill process
- `backfill_status` - Current status of the backfill process (0=idle, 1=running, 2=error, 3=on-demand-error)
- `backfill_pending_blocks` - Current amount of blocks pending to be backfilled

## Design Documentation

For detailed design information, see [backfill-plugin.md](../../docs/design/backfill-plugin.md).
