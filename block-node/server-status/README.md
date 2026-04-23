# server-status

Exposes the `serverStatus` and `serverStatusDetail` gRPC endpoints that allow operators and tooling to query the Block Node's current state: what block range it holds, what software version it is running, and whether it is in "latest state only" or full-history mode.

---

## Key Files

|               File               |                                                                                       Purpose                                                                                        |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ServerStatusServicePlugin.java` | The entire plugin. Implements `BlockNodePlugin` and `BlockNodeServiceInterface`. Handles both RPC methods: `serverStatus` (summary) and `serverStatusDetail` (full range breakdown). |

---

## Notable Logic

### `serverStatusDetail()` — `blockRangeBuilder` reuse bug

A single `BlockRange.Builder` is created before the loop that iterates over `availableBlocks().streamRanges()` and is reused for every iteration without calling `clear()`. All entries in the resulting `blockRanges` list end up equal to the **last** range (P0 concern). Each loop iteration must create a fresh builder instance:

```java
blockRanges.add(BlockRange.newBuilder()
    .rangeStart(longRange.start())
    .rangeEnd(longRange.end())
    .build());
```

### `onlyLatestState` — hardcoded to `false`

The `onlyLatestState` field in the response is currently hardcoded to `false` with a `// TODO(#579)` comment. Any operator or tool that relies on this flag to determine whether a node retains full block history or only the latest state will always see `false`, even on retention-limited nodes. Fix tracked as P2.
