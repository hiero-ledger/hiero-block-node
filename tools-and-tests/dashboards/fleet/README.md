# Fleet dashboard (multi-instance, env-rendered)

The **"Block-Node: Full-History Metrics"** overview is maintained here and rendered into one output
per deployment *variant*, because the variants differ only in how they scope a fleet of nodes — not
in their panels.

## Schemas: why there are two sources

The variants target different Grafana lineages:

- **cloud** is on a Grafana that requires the **new v2 dashboard schema** (`elements` / `layout` /
  `variables[]`, `annotations` as an array). Its source is **`full-history-metrics.v2.json`**.
- **latitude / local** still use the **classic schema** (`panels` / `templating` / `annotations.list`).
  Their source is **`full-history-metrics.json`**.

Both sources are the *same dashboard* in two schemas. Keep them in sync by hand for now; the v2 file
was produced by importing the classic dashboard into Grafana (which migrates classic→v2) and
exporting. When latitude/local move to v2, collapse to the single v2 source and delete the classic one.

## Layout

- `full-history-metrics.v2.json` — v2 source (**edit for cloud**).
- `full-history-metrics.json` — classic source (**edit for latitude/local**).
- Both use a `DS_PROMETHEUS` datasource variable (datasource self-resolves per Grafana) and a
  `__FLEET__` selector token in every query, e.g. `...{__FLEET__}`.
- `env-profiles.json` — the variants. Each names its `schema` (`v2` | `classic`) and `source`, the
  `fleetSelector` that replaces `__FLEET__`, and the fleet template `variables` appended after
  `DS_PROMETHEUS` (into `variables[]` for v2, `templating.list` for classic).
- `dist/<variant>/full-history-metrics.json` — generated outputs (committed, sync-checked).

## Variants

|  variant   | schema  |                      selector                       |    fleet variables    |                                    serves                                     |
|------------|---------|-----------------------------------------------------|-----------------------|-------------------------------------------------------------------------------|
| `cloud`    | v2      | `environment="$environment", instance=~"$instance"` | environment, instance | previewnet / testnet / mainnet (Grafana Cloud) via the `environment` dropdown |
| `latitude` | classic | `cluster=~"$cluster"`                               | cluster               | bnce / perfnet (Latitude Mimir) via the datasource dropdown                   |
| `local`    | classic | *(empty)* → `{}`                                    | —                     | docker-compose / k8s single node                                              |

## Editing / regenerating

```sh
node tools-and-tests/dashboards/render.js
```

Edit the source that matches the variant's schema (and `env-profiles.json` for variant wiring),
regenerate, and commit the source(s) **and** the `dist/` outputs together — CI's `render.js --check`
fails on drift, and `render.js` prunes any orphaned `dist/` output.

## Conventions baked into the sources

- **Per-instance shaping:** every query is `... by (instance) (...)` with `legendFormat "{{instance}}"`
  so each node is a distinct, readable series (the managed Latitude copy lacked this — hence its
  unreadable labels).
- **Counter `_total`:** counters carry the OpenMetrics `_total` suffix (`blocknode_..._total`);
  gauges do **not** (e.g. `blocknode_files_recent_total_bytes_stored` is a gauge — the `total` is part
  of its name, not a counter suffix). This holds uniformly across cloud/Latitude/local.

## Dashboard layout (full-history-metrics)

Fleet-first — scan the fleet, then drill by concern:

1. **Fleet Health** — a table, one row per node (status, newest block, blocks behind leader, queue
   %, connections, storage), sorted by "behind leader" and colour-thresholded to surface the outlier.
2. **Throughput & Freshness** — block processing rate; blocks-behind-leader over time.
3. **Verify & Persist** — verification success %, avg verify time/block, block items per block.
4. **Ingest & Backpressure** — publishers, live items/s, flow-control pauses.
5. **Serve / Access** — getBlock rate, request outcomes, subscribers.
6. **Backfill** — pending/in-flight, backfilled/s, backfill problems/s.
7. **Storage** — recent/historic bytes, persistence rate.
8. **Errors & Consistency** — error rates, queue saturation, roster/tss problems.

## Verified / to verify live

- **cloud (v2): validated** — imported into the cloud Grafana and renders correctly.
- **latitude (classic): label scheme not yet confirmed** against live Latitude Mimir
  (`cluster` fleet / `instance` identity are inferred from the managed copy). Confirm via Explore;
  a mismatch is a one-line fix in `env-profiles.json` + re-render.
- **Optional-subsystem panels** (publisher flow-control, notification queue, roster/tss) show
  *"No data"* where that subsystem is inactive — expected, not a bug.
