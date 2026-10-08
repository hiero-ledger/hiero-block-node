# Application State Facility

**Module:** `org.hiero.block.node.app.state`
**Plugin class:** `org.hiero.block.node.app.state.ApplicationStateFacilityPlugin`
**Gradle project:** `:facility-app-state`
**Design doc:** [`docs/design/architecture/new-plugin-architecture.md`](../../docs/design/architecture/new-plugin-architecture.md)

## Purpose

Owns the mutable state shared by the block node's plugins, persists it to disk, and tells the other
plugins when it changes:

|           State            |                            Accessor                            |           Persisted to           |
|----------------------------|----------------------------------------------------------------|----------------------------------|
| TSS data                   | `tssData()`                                                    | `app.state.tssBootstrapFilePath` |
| RSA address book history   | `rangedAddressBookHistory()`, `getAddressBookForBlock`         | `app.state.rsaBootstrapFilePath` |
| Stored block ranges        | `storedBlocks()`                                               | `app.state.blockRangesFilePath`  |
| Available block ranges     | derived from `HistoricalBlockFacility`                         | not persisted                    |
| Known publishers, partners | `knownPublishers()`, `inboundPartners()`, `outboundPartners()` | read from `app.state.*FilePath`  |
| Backfill sources           | `backfillSources()`, `updateBackfillSources(...)`              | not persisted                    |
| Next expected block        | `nextExpectedBlock()`, `updateExpectedBlock(...)`              | not persisted                    |

## Public API

The API is the `org.hiero.block.node.spi.ApplicationStateFacility` interface in `spi-plugins`. It
extends `BlockNodePlugin`.

- **Updates:** `updateTssData`, `updateAddressBookHistory`, `addStoredBlockRange`,
  `updateAvailableBlocks`. An update is installed in memory immediately (compare-and-set, so older
  data never replaces newer data), then persisted and dispatched from a single dispatcher thread, so
  a notification never carries older data than one already sent.
- **Change notifications:** each accepted change is sent through the `BlockMessagingFacility` as a
  `TssDataNotification`, `AddressBookHistoryNotification`, `StoredBlocksNotification` or
  `AvailableBlocksNotification`. A plugin implements `ApplicationStateNotificationHandler` and
  registers it with `BlockMessagingFacility.registerApplicationStateNotificationHandler(...)`,
  usually from `init()`.
- **Startup state:** the state loaded from disk is dispatched when this plugin starts. A plugin
  that starts later reads the current value from the facility in its own `start()`.

## Lifecycle

`BlockNodeApp` loads the facility with `ServiceLoader`, passes it to every plugin as
`BlockNodeContext.applicationStateFacility()`, and runs it in a fixed order relative to the other
plugins:

1. `init()` runs right after the messaging facility and before every block provider, because
   providers may report blocks from their own `init()`. It registers the oldest and newest block
   gauges, `app_historical_oldest_block` and `app_historical_newest_block`.
2. `start()` runs after the messaging facility has started, before all other plugins. It loads the
   TSS data, address book history and block ranges, dispatches them, then starts the dispatcher
   thread. A corrupt RSA bootstrap file aborts startup with `IllegalStateException`.
3. `stop()` runs after the web servers close and after every other plugin has stopped, but before
   the messaging facility stops. It finishes queued updates and persists the block ranges, so the
   persisted ranges include anything plugins reported while stopping.

Block ranges are also persisted every `1000` stored blocks, checked every `app.state.updateScanInterval`
milliseconds. Files are replaced atomically (write to a temporary sibling, then move).

## Wiring

```java
// module-info.java of facility-app-state
provides com.swirlds.config.api.ConfigurationExtension with ApplicationStateConfigExtension;
provides org.hiero.block.node.spi.ApplicationStateFacility with ApplicationStateFacilityPlugin;

// module-info.java of spi-plugins and app
uses org.hiero.block.node.spi.ApplicationStateFacility;
```

The plugin is provided only as an `ApplicationStateFacility` (not additionally as a
`BlockNodePlugin`), so a `ServiceLoader` lookup yields a single instance, which is the one the
application initializes, starts and stops. The module must be present in the plugin list of every
deployment; `BlockNodeApp` fails to start without it.

## Configuration

`ApplicationStateConfig` (prefix `app.state`) is registered by `ApplicationStateConfigExtension`.
See the record's Javadoc for each property and its default.
