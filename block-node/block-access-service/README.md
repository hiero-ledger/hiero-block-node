# block-access-service

Exposes the `getBlock` gRPC RPC for on-demand block retrieval. Clients (e.g., mirror nodes, tooling) call this endpoint to fetch a specific block by number and receive it in their preferred encoding (protobuf, JSON, or ZSTD-compressed protobuf).

This plugin is a thin gRPC adapter over the `HistoricalBlockFacility`. It deliberately avoids full proto parsing of block items to prevent version-skew issues when the client and server are running different HAPI versions.

---

## Key Files

|              File               |                                                                                                                   Purpose                                                                                                                    |
|---------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BlockAccessServicePlugin.java` | The entire plugin. Implements `BlockNodePlugin` and `BlockAccessServiceInterface`. Handles incoming `BlockRequest` messages, delegates to `HistoricalBlockFacility.block(blockNumber)`, and streams the result back in the requested format. |

---

## Notable Logic

### Unparsed response path

`BlockAccessServicePlugin` returns block data via a `BlockAccessor` without deserialising the individual `BlockItem` fields. This means the server can serve blocks that were encoded with a newer or older HAPI proto version than the server knows about — the raw bytes are forwarded as-is. Format conversion (e.g., protobuf → JSON) is handled by `BlockAccessor.blockBytes(Format)`, which is implemented per storage backend in `BlockFileBlockAccessor` (recent) and `ZipBlockAccessor` (historic).

### Single-class plugin

All logic lives in `BlockAccessServicePlugin`. If you need to extend the `getBlock` behaviour (e.g., add range requests, add streaming), this is the only file to modify.
