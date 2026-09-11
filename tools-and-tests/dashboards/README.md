# Grafana dashboards (single source)

The Grafana dashboards shared by the Block Node's in-repo deployments are maintained **once** here
and rendered into each deployment target, so the copies can't drift.

## Layout

- `src/` — the canonical dashboards (**edit these**), in the **Grafana v2 schema**
  (`dashboard.grafana.app`: `elements` / `layout` / `variables`). 16 JSON files:
  `block-node-server.json` (overview), `performance-metrics.json`, and one per plugin under
  `Hiero-Block-Node-Plugins/` (app-core, backfill, block-access-service, block-provider-files-recent,
  block-provider-files-historic, cloud-storage-archive, health, messaging-facility,
  roster-bootstrap-rsa, roster-bootstrap-tss, server-status, stream-publisher, stream-subscriber,
  verification).
- `fleet/` — the multi-instance **"Full-History Metrics"** overview: one templated source rendered
  per deployment variant (cloud / Latitude / local). See `fleet/README.md`.
- `render.js` — renders both: copies `src/` into the docker + chart targets, and renders `fleet/`
  into its per-variant `dist/` outputs (Node, no dependencies).

## Targets

`render.js` writes the 16 dashboards into:

- `block-node/app/docker/metrics/dashboards/` — local docker-compose metrics stack, bind-mounted
  into Grafana's provisioning directory.
- `charts/block-node-server/dashboards/` — the Helm chart. `grafana-dashboard-configmap.yaml`
  publishes **every** `*.json` in that tree via `.Files.Glob`, so adding a dashboard needs no chart
  edit and the chart cannot drift from what was rendered.

Both targets read the committed files directly — there is no render step at deploy time, which is
why the rendered copies are committed rather than gitignored. They are marked
`linguist-generated` in `.gitattributes` so they collapse in GitHub diffs.

Each target also carries **hand-maintained extras that are not shared** and are left untouched:
docker has `block-node-server-logs.json` + `cAdvisor.json`; the chart has `node-exporter-full.json`
+ `kubernetes-views-pods.json`.

## Editing dashboards

1. Edit (or add) a dashboard under `src/`. Add new shared files to the `SHARED` list in `render.js`.
2. Regenerate the copies:

   ```sh
   node tools-and-tests/dashboards/render.js
   ```
3. Commit `src/` **and** the regenerated target copies together.

CI runs `node tools-and-tests/dashboards/render.js --check` and fails if any target copy is out of
sync with `src/` — i.e. if a generated copy was edited directly, or `src/` was changed without
re-rendering.

## One universal file per dashboard (works docker / k8s / cloud)

Each dashboard carries three template variables — `DS_PROMETHEUS` (datasource), `environment`, and
`instance` (both `label_values(...)` query variables with **include-all** defaults) — and every query
scopes with `{environment=~"$environment", instance=~"$instance"}`. Because a regex match with the
include-all default (`=~".*"`) also matches series that *lack* those labels, the **same file works
everywhere**:

- **docker / k8s single node** — no `environment` fleet label; the dropdowns resolve to "All" and
  match the one node.
- **cloud / multi-tenant** — pick an environment + instance from the dropdowns to drill into a node.

So there are no per-environment variants for these dashboards: the committed `src/` file is what
docker + chart provision (file/sidecar provisioning accepts the v2 schema on Grafana 12+) and what
you import/sync to cloud. The datasource self-resolves per Grafana via `DS_PROMETHEUS`.

> The multi-node **fleet** overview ("Full-History Metrics") is different — it aggregates across a
> fleet and lives under `fleet/` with its own per-variant rendering. See `fleet/README.md`.
